"""Bridge: interaction-study RegressionResults -> heatmap or top-pairs dotplot."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

from igem.modules.analyze.results import RegressionResults
from igem.modules.plot.primitives.heatmap import heatmap

PathLike = str | Path
Kind = Literal["auto", "heatmap", "top_pairs"]

# Threshold above which ``kind="auto"`` falls back to a top-pairs
# dotplot — heatmaps with too many distinct terms become unreadable.
_AUTO_HEATMAP_MAX_TERMS = 30
_INTERACTION_REQUIRED_COLS = ("term1", "term2")


def _is_interaction_result(df: pd.DataFrame) -> bool:
    return all(c in df.columns for c in _INTERACTION_REQUIRED_COLS)


def _resolve_kind(n_terms: int, kind: str) -> str:
    if kind != "auto":
        return kind
    return "heatmap" if n_terms <= _AUTO_HEATMAP_MAX_TERMS else "top_pairs"


def _top_pairs_dotplot(
    df: pd.DataFrame,
    *,
    pvalue_col: str,
    n_top: int,
    cutoff: Optional[float],
    title: Optional[str],
    figsize: Optional[tuple[float, float]],
    dpi: int,
    output_path: Optional[PathLike],
) -> Figure:
    """Sorted dotplot of top interaction pairs."""
    sub = (
        df.dropna(subset=[pvalue_col])
        .sort_values(pvalue_col)
        .head(int(n_top))
        .reset_index(drop=True)
    )
    if len(sub) == 0:
        raise ValueError("no rows left after dropping missing p-values")

    if figsize is None:
        figsize = (10.0, max(4.0, 0.35 * len(sub) + 1.5))
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)

    labels = (sub["term1"].astype(str) + " × " + sub["term2"].astype(str)).tolist()
    y = np.arange(len(sub))[::-1]
    raw_values = sub[pvalue_col].to_numpy(dtype=float)
    # ``value`` is allowed to be non-p-value columns (e.g. diff_aic).
    # Guard the log so non-positive values become NaN rather than warn.
    neglog_p = -np.log10(np.where(raw_values > 0, raw_values, np.nan))
    ax.scatter(neglog_p, y, s=30, c="#53868B", edgecolors="none")
    if cutoff is not None:
        ax.axvline(
            -np.log10(cutoff),
            color="#C0392B", linestyle="--", linewidth=1.0, alpha=0.7,
        )
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel(rf"$-\log_{{10}}$({pvalue_col})")
    if title:
        ax.set_title(title)

    fig.tight_layout()
    if output_path is not None:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(out, bbox_inches="tight")
    return fig


def from_interaction(
    results: RegressionResults,
    *,
    kind: Kind = "auto",
    value: str = "lrt_pvalue",
    n_top: int = 20,
    cutoff: Optional[float] = None,
    cluster: bool = False,
    annotate: bool = False,
    figsize: Optional[tuple[float, float]] = None,
    dpi: int = 150,
    output_path: Optional[PathLike] = None,
    title: Optional[str] = None,
    **primitive_kwargs: Any,
) -> Figure:
    """
    Plot the output of :func:`igem.analyze.interaction_study`.

    The bridge expects ``term1`` / ``term2`` columns in ``results.df``
    (the long-format schema produced by :func:`interaction_study`) and
    rejects regular regression results — those should go through
    :func:`from_results` instead.

    ``kind="auto"`` picks ``"heatmap"`` when the universe of unique
    terms is small enough to show as a matrix
    (``<= _AUTO_HEATMAP_MAX_TERMS = 30``); otherwise falls back to a
    ``"top_pairs"`` dotplot of the top ``n_top`` interactions ranked by
    ``value``. ``value`` defaults to ``"lrt_pvalue"`` but any column on
    ``results.df`` is accepted (e.g., ``"diff_aic"`` for model
    comparison).

    Extra keyword arguments are forwarded to the underlying primitive
    (``heatmap``).
    """
    df = results.df
    if not _is_interaction_result(df):
        raise ValueError(
            "from_interaction expects an interaction-study result with "
            f"{list(_INTERACTION_REQUIRED_COLS)} columns; got: "
            f"{list(df.columns)}. Use plot.from_results for regular "
            "association/EWAS/GWAS results."
        )
    if value not in df.columns:
        raise ValueError(
            f"value={value!r} not in result columns: {list(df.columns)}"
        )

    n_terms = len(set(df["term1"]) | set(df["term2"]))
    resolved = _resolve_kind(n_terms, kind)
    auto_title = title or f"{results.outcome} ({resolved})"

    if resolved == "heatmap":
        fig_size = figsize if figsize is not None else (8.0, 7.0)
        return heatmap(
            df,
            row_col="term1",
            col_col="term2",
            value_col=value,
            transform="neglog10",
            symmetric=True,
            cluster=cluster,
            annotate=annotate,
            cutoff=cutoff,
            figsize=fig_size,
            dpi=dpi,
            title=auto_title,
            output_path=output_path,
            **primitive_kwargs,
        )
    if resolved == "top_pairs":
        return _top_pairs_dotplot(
            df,
            pvalue_col=value,
            n_top=n_top,
            cutoff=cutoff,
            title=auto_title,
            figsize=figsize,
            dpi=dpi,
            output_path=output_path,
        )
    raise ValueError(
        f"unknown kind={kind!r}; expected one of: auto, heatmap, top_pairs"
    )
