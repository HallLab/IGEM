"""Result container for regression-style analyses."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

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
        self, method: str = "bonferroni"
    ) -> "RegressionResults":
        """
        Add a ``p_corrected`` column with adjusted p-values. Overwrites
        any prior correction applied to the same Result.
        """
        if "p_value" not in self.df.columns:
            raise ValueError(
                "RegressionResults.df has no 'p_value' column to correct"
            )
        new_df = self.df.copy()
        new_df["p_corrected"] = apply_correction(
            new_df["p_value"].to_numpy(), method=method
        )
        new_meta = {**self.metadata, "correction_method": method}
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
        """
        if p is None and p_corrected is None:
            raise ValueError(
                "passing() requires at least one of p= or p_corrected="
            )
        mask = pd.Series(True, index=self.df.index)
        if p is not None:
            if "p_value" not in self.df.columns:
                raise ValueError(
                    "p= requires 'p_value' column on the result"
                )
            mask &= self.df["p_value"] < p
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
        client_or_reports: Any,
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
        instance or its ``.reports`` component as ``client_or_reports``.

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

        reports = (
            client_or_reports.reports
            if hasattr(client_or_reports, "reports")
            else client_or_reports
        )
        if not hasattr(reports, "gene_annotations"):
            raise TypeError(
                "annotate(client) requires either an IGEM instance or "
                "a ReportsComponent with .gene_annotations"
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

        annotation = reports.gene_annotations(
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

    def top(
        self,
        n: int = 20,
        *,
        by: str = "p_value",
        ascending: bool = True,
    ) -> "RegressionResults":
        if by not in self.df.columns:
            raise ValueError(
                f"sort column {by!r} not in result; "
                f"available: {list(self.df.columns)}"
            )
        if n <= 0:
            raise ValueError(f"n must be positive; got {n}")
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
