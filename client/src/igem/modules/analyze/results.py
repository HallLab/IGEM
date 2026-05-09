"""Result container for regression-style analyses."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from igem.modules.analyze._corrections import apply_correction


_DEFAULT_ANNOTATION_KEEP = [
    "gene_symbol",
    "hgnc_id",
    "ensembl_id",
    "entrez_id",
    "chromosome",
    "start_position",
    "end_position",
    "gene_locus_group",
    "gene_locus_type",
]


# Canonical schema for results emitted by ``association_study`` /
# ``interaction_study``. Legacy ``ewas`` / ``gwas`` results continue
# to use the older subset (``variable``, ``n``, ``beta``, ``se``,
# ``ci_low``, ``ci_high``, ``p_value``) for retrocompatibility — the
# ``with_correction`` helper accepts either ``p_value`` or
# ``beta_pvalue`` as the source column.
ASSOCIATION_RESULT_COLUMNS = [
    "outcome",
    "variable",
    "variable_type",      # continuous / binary / categorical / genotype
    "n",
    "beta",
    "se",
    "ci_low",
    "ci_high",
    "beta_pvalue",        # Wald p-value (canonical replacement of "p_value")
    "lrt_pvalue",         # LRT p-value (relevant for categorical / genotype)
    "diff_aic",           # AIC(full) - AIC(null)
    "converged",          # bool — statsmodels convergence flag
    "error",              # str | None — error message if fit failed
]
INTERACTION_RESULT_COLUMNS = [
    "outcome",
    "term1",
    "term2",
    "n",
    "lrt_chi2",
    "lrt_df",
    "lrt_pvalue",
    "diff_aic",
    "converged",
    "error",
]


def _resolve_pvalue_column(df: pd.DataFrame) -> str:
    """
    Return the canonical p-value column name in a result frame.

    Prefers ``beta_pvalue`` (new schema) and falls back to ``p_value``
    (legacy schema). Raises ``ValueError`` if neither is present.
    """
    if "beta_pvalue" in df.columns:
        return "beta_pvalue"
    if "p_value" in df.columns:
        return "p_value"
    raise ValueError(
        "RegressionResults.df has no p-value column "
        "(expected 'beta_pvalue' or 'p_value')"
    )


@dataclass
class RegressionResults:
    """
    Wrapper around the per-test results of an ``ewas`` / ``gwas`` /
    ``interactions`` run.

    Encapsulates the model spec used (``family``, ``formula_template``,
    ``outcome``, ``covariates``) so downstream code can reproduce the
    fit; tracks per-variable failures separately in ``errors`` so a
    single bad regression doesn't poison the whole result.

    The class is immutable in spirit: ``with_correction``, ``passing``
    and ``top`` always return new ``RegressionResults`` objects.
    """

    df: pd.DataFrame
    family: str
    outcome: str
    covariates: list[str]
    formula_template: str
    errors: pd.DataFrame
    metadata: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------
    @property
    def n_tests(self) -> int:
        return int(len(self.df))

    @property
    def n_errors(self) -> int:
        return int(len(self.errors))

    @property
    def correction_method(self) -> Optional[str]:
        return self.metadata.get("correction_method")

    def to_dataframe(self) -> pd.DataFrame:
        return self.df.copy()

    def to_csv(self, path: str | Path, index: bool = False) -> Path:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(out, index=index)
        return out

    def summary(self) -> dict[str, Any]:
        info: dict[str, Any] = {
            "n_tests": self.n_tests,
            "n_errors": self.n_errors,
            "family": self.family,
            "outcome": self.outcome,
            "covariates": list(self.covariates),
            "formula_template": self.formula_template,
        }
        if self.correction_method:
            info["correction_method"] = self.correction_method
            info["n_passing_corrected_005"] = int(
                (self.df["p_corrected"] < 0.05).sum()
            ) if "p_corrected" in self.df.columns else 0
        return info

    def __repr__(self) -> str:
        parts = [
            f"family={self.family}",
            f"outcome={self.outcome!r}",
            f"n_tests={self.n_tests}",
        ]
        if self.n_errors:
            parts.append(f"n_errors={self.n_errors}")
        if self.correction_method:
            parts.append(f"correction={self.correction_method}")
        return f"<RegressionResults {' '.join(parts)}>"

    # ------------------------------------------------------------------
    # Transformations (return new RegressionResults)
    # ------------------------------------------------------------------
    def with_correction(
        self,
        method: str = "bonferroni",
        *,
        groupby: Optional[str] = None,
    ) -> "RegressionResults":
        """
        Add a ``p_corrected`` column with adjusted p-values.

        When ``groupby`` is ``None`` (default), the correction is
        applied across all rows. When ``groupby`` is the name of a
        column in ``df`` (typically ``"outcome"`` for PheWAS
        multi-outcome runs), the correction is applied **within each
        group** — preventing the false inflation of test count that
        global correction would induce when several outcomes are
        scanned in one call.

        The source p-value column is auto-detected: prefers
        ``beta_pvalue`` (new ``association_study`` schema), falls
        back to ``p_value`` (legacy ``ewas`` / ``gwas`` schema).
        Overwrites any prior correction applied to the same Result.
        """
        pval_col = _resolve_pvalue_column(self.df)

        if groupby is not None and groupby not in self.df.columns:
            raise ValueError(
                f"groupby={groupby!r} not in result columns: "
                f"{list(self.df.columns)}"
            )

        new_df = self.df.copy()
        if groupby is None:
            new_df["p_corrected"] = apply_correction(
                new_df[pval_col].to_numpy(), method=method,
            )
        else:
            corrected = np.full(len(new_df), np.nan, dtype=float)
            for _, group_idx in new_df.groupby(groupby).groups.items():
                idx_pos = new_df.index.get_indexer(group_idx)
                pvals = new_df.loc[group_idx, pval_col].to_numpy()
                corrected[idx_pos] = apply_correction(pvals, method=method)
            new_df["p_corrected"] = corrected

        new_meta = {
            **self.metadata,
            "correction_method": method,
            "correction_groupby": groupby,
        }
        return replace(self, df=new_df, metadata=new_meta)

    def passing(
        self,
        *,
        p: Optional[float] = None,
        p_corrected: Optional[float] = None,
    ) -> "RegressionResults":
        """
        Filter to rows that pass thresholds. At least one of ``p`` or
        ``p_corrected`` must be provided.

        Auto-detects the source p-value column: ``beta_pvalue`` (new
        schema) or ``p_value`` (legacy schema).
        """
        if p is None and p_corrected is None:
            raise ValueError(
                "passing() requires at least one of p= or p_corrected="
            )
        mask = pd.Series(True, index=self.df.index)
        if p is not None:
            pval_col = _resolve_pvalue_column(self.df)
            mask &= self.df[pval_col] < p
        if p_corrected is not None:
            if "p_corrected" not in self.df.columns:
                raise ValueError(
                    "p_corrected= requires the result to be passed "
                    "through with_correction(...) first"
                )
            mask &= self.df["p_corrected"] < p_corrected
        return replace(self, df=self.df.loc[mask].reset_index(drop=True))

    def annotate(
        self,
        client_or_report: Any,
        *,
        input_col: str = "variable",
        keep_columns: Optional[list[str]] = None,
        assembly: str = "GRCh38.p14",
    ) -> "RegressionResults":
        """
        Merge gene annotations from the IGEM server into the result.

        For every unique value in ``input_col``, asks the server for
        a ``gene_annotations`` row and joins the columns listed in
        ``keep_columns`` into ``df``. Pass either an :class:`IGEM`
        instance or its ``.report`` component as ``client_or_report``.

        Rows whose ``input_col`` value did not match a gene get NaN in
        the annotation columns. The mapping is left-join on
        ``input_col == input_value`` (the column name returned by the
        gene_annotations report).
        """
        if input_col not in self.df.columns:
            raise ValueError(
                f"input_col {input_col!r} not in result; "
                f"available: {list(self.df.columns)}"
            )

        report = (
            client_or_report.report
            if hasattr(client_or_report, "report")
            else client_or_report
        )
        if not hasattr(report, "gene_annotations"):
            raise TypeError(
                "annotate(client) requires either an IGEM instance or "
                "a ReportComponent with .gene_annotations"
            )

        values = (
            self.df[input_col]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        if not values:
            return replace(self)

        annotation = report.gene_annotations(
            input_values=values,
            assembly=assembly,
        )
        annot_df = annotation.df

        keep = list(keep_columns) if keep_columns else _DEFAULT_ANNOTATION_KEEP
        keep = [c for c in keep if c in annot_df.columns]
        if "input_value" not in annot_df.columns:
            raise RuntimeError(
                "gene_annotations report did not return 'input_value'; "
                "cannot merge"
            )

        merged = self.df.merge(
            annot_df[["input_value", *keep]].drop_duplicates(
                subset="input_value"
            ),
            how="left",
            left_on=input_col,
            right_on="input_value",
            suffixes=("", "_annot"),
        )
        if "input_value" in merged.columns and input_col != "input_value":
            merged = merged.drop(columns="input_value")

        new_meta = {
            **self.metadata,
            "annotated_with": "gene_annotations",
            "annotation_assembly": assembly,
        }
        return replace(self, df=merged, metadata=new_meta)

    def suggested_plots(self) -> list[str]:
        """
        Return the plot kinds that make sense for this result.

        Thin convenience over :func:`igem.modules.plot.suggest_plots`;
        imports lazily so :mod:`igem.modules.analyze` keeps no
        compile-time dependency on the plot module. The returned
        strings are the ``kind`` values accepted by
        :func:`igem.modules.plot.from_results` /
        :func:`igem.modules.plot.from_interaction`.
        """
        from igem.modules.plot.suggest import suggest_plots
        return suggest_plots(self)

    def top(
        self,
        n: int = 20,
        *,
        by: Optional[str] = None,
        ascending: bool = True,
    ) -> "RegressionResults":
        """
        Sort the result by ``by`` and keep the top ``n`` rows.

        With ``by=None`` (default), auto-detects the canonical p-value
        column: ``beta_pvalue`` (new schema) or ``p_value`` (legacy).
        """
        if n <= 0:
            raise ValueError(f"n must be positive; got {n}")
        if by is None:
            by = _resolve_pvalue_column(self.df)
        elif by not in self.df.columns:
            raise ValueError(
                f"sort column {by!r} not in result; "
                f"available: {list(self.df.columns)}"
            )
        new_df = (
            self.df.sort_values(by, ascending=ascending)
            .head(n)
            .reset_index(drop=True)
        )
        return replace(self, df=new_df)


def make_metadata(
    *,
    n_samples: int,
    n_dropped: int,
    extras: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Standard metadata block stamped onto a fresh RegressionResults."""
    from igem import __version__

    base: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "igem_version": __version__,
        "n_samples_used": int(n_samples),
        "n_samples_dropped_na": int(n_dropped),
    }
    if extras:
        base.update(extras)
    return base
