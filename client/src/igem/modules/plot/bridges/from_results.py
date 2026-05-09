"""Bridge: RegressionResults -> appropriate plotting primitive."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Optional

from matplotlib.figure import Figure

from igem.modules.analyze.results import RegressionResults, _resolve_pvalue_column
from igem.modules.plot.primitives.dotplot import dotplot
from igem.modules.plot.primitives.manhattan import (
    manhattan,
    manhattan_bonferroni,
    manhattan_fdr,
)
from igem.modules.plot.primitives.qq import qq_plot

PathLike = str | Path
Kind = Literal["auto", "manhattan", "manhattan_fdr", "manhattan_bonferroni", "qq", "top"]

# Threshold above which ``kind="auto"`` defaults to a Manhattan view.
# Below it, the result is small enough that a sorted dotplot of the
# top hits is more informative than a scatter of points.
_AUTO_MANHATTAN_MIN_TESTS = 50


def _resolve_kind(results: RegressionResults, kind: str) -> str:
    if kind != "auto":
        return kind
    if results.n_tests >= _AUTO_MANHATTAN_MIN_TESTS:
        return "manhattan"
    return "top"


def _detect_chrom_pos(df) -> tuple[Optional[str], Optional[str]]:
    """
    Pick a chromosome / position column pair if the result was
    ``annotate``d with gene metadata. Returns (None, None) for plain
    EWAS-style results.
    """
    chrom_col = next(
        (c for c in ("chromosome", "chrom", "CHR", "chr") if c in df.columns),
        None,
    )
    pos_col = next(
        (c for c in ("start_position", "position", "POS", "pos") if c in df.columns),
        None,
    )
    return chrom_col, pos_col


def from_results(
    results: RegressionResults,
    *,
    kind: Kind = "auto",
    n_top: int = 20,
    cutoff: Optional[float] = None,
    output_path: Optional[PathLike] = None,
    title: Optional[str] = None,
    **primitive_kwargs: Any,
) -> Figure:
    """
    Plot a :class:`RegressionResults` using the most appropriate primitive.

    ``kind="auto"`` picks ``manhattan`` when there are at least
    ``_AUTO_MANHATTAN_MIN_TESTS`` rows, otherwise ``top`` (a sorted
    dotplot). Other valid values: ``"manhattan"``, ``"manhattan_fdr"``,
    ``"manhattan_bonferroni"``, ``"qq"``, ``"top"``.

    The bridge auto-detects:

    * the canonical p-value column (``beta_pvalue`` for the new schema,
      ``p_value`` for legacy ``ewas`` / ``gwas`` results);
    * chromosome / position columns when the result has been passed
      through :meth:`RegressionResults.annotate`, enabling proper
      genomic-axis Manhattan layout.

    Extra keyword arguments are forwarded to the underlying primitive.
    """
    df = results.df
    pvalue_col = _resolve_pvalue_column(df)
    chrom_col, pos_col = _detect_chrom_pos(df)
    resolved = _resolve_kind(results, kind)
    # ``manhattan_fdr`` and ``manhattan_bonferroni`` build informative
    # default titles from their threshold parameters; only pre-fill an
    # outcome-based title for the kinds that have no such default.
    outcome_title = title or results.outcome

    if resolved == "manhattan":
        return manhattan(
            df,
            pvalue_col=pvalue_col,
            chrom_col=chrom_col,
            pos_col=pos_col,
            cutoffs=[(cutoff, "#C0392B", "--")] if cutoff is not None else None,
            title=outcome_title,
            output_path=output_path,
            **primitive_kwargs,
        )
    if resolved == "manhattan_fdr":
        if "p_corrected" not in df.columns:
            raise ValueError(
                "manhattan_fdr requires the result to be passed through "
                "RegressionResults.with_correction(method='fdr_bh') first"
            )
        return manhattan_fdr(
            df,
            pvalue_col="p_corrected",
            q_threshold=cutoff if cutoff is not None else 0.05,
            chrom_col=chrom_col,
            pos_col=pos_col,
            title=title,
            output_path=output_path,
            **primitive_kwargs,
        )
    if resolved == "manhattan_bonferroni":
        return manhattan_bonferroni(
            df,
            pvalue_col=pvalue_col,
            alpha=cutoff if cutoff is not None else 0.05,
            chrom_col=chrom_col,
            pos_col=pos_col,
            title=title,
            output_path=output_path,
            **primitive_kwargs,
        )
    if resolved == "qq":
        return qq_plot(
            df[pvalue_col].to_numpy(),
            title=outcome_title,
            output_path=output_path,
            **primitive_kwargs,
        )
    if resolved == "top":
        return dotplot(
            df,
            pvalue_col=pvalue_col,
            n_top=n_top,
            cutoff=cutoff if cutoff is not None else 0.05,
            title=outcome_title,
            output_path=output_path,
            **primitive_kwargs,
        )
    raise ValueError(
        f"unknown kind={kind!r}; expected one of: auto, manhattan, "
        f"manhattan_fdr, manhattan_bonferroni, qq, top"
    )
