"""Manhattan plot primitives."""
from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

PathLike = str | Path

# Two-tone palette mirroring the alternating-chromosome convention used
# in published GWAS Manhattan plots. Used both for chromosomes (when
# ``chrom_col`` is given) and as a fallback hash-based shading.
_DEFAULT_COLORS = ("#53868B", "#4D4D4D")


def _save_fig(fig: Figure, output_path: Optional[PathLike]) -> None:
    if output_path is None:
        return
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches="tight")


def _neglog10(p: pd.Series) -> np.ndarray:
    arr = pd.to_numeric(p, errors="coerce").to_numpy(dtype=float)
    arr = np.where(arr <= 0, np.nan, arr)
    return -np.log10(arr)


def _build_xaxis(
    df: pd.DataFrame,
    chrom_col: Optional[str],
    pos_col: Optional[str],
) -> tuple[np.ndarray, list[tuple[str, float]], np.ndarray]:
    """
    Return (x_positions, chrom_label_centers, color_index_per_row).

    When ``chrom_col`` (and optionally ``pos_col``) are provided, points
    are laid out per-chromosome with cumulative offsets so chromosomes
    occupy disjoint ranges. Otherwise points are laid out by row index
    in the input order.
    """
    if chrom_col is None or chrom_col not in df.columns:
        x = np.arange(len(df), dtype=float)
        return x, [], np.zeros(len(df), dtype=int)

    chroms = df[chrom_col].astype(str).to_numpy()
    if pos_col is not None and pos_col in df.columns:
        pos = pd.to_numeric(df[pos_col], errors="coerce").fillna(0).to_numpy()
    else:
        pos = np.arange(len(df), dtype=float)

    unique_chroms = pd.unique(chroms)
    x = np.zeros(len(df), dtype=float)
    centers: list[tuple[str, float]] = []
    color_idx = np.zeros(len(df), dtype=int)
    offset = 0.0
    gap = max(pos.max() * 0.01 if len(pos) else 1.0, 1.0)
    for i, c in enumerate(unique_chroms):
        mask = chroms == c
        chrom_pos = pos[mask]
        x[mask] = chrom_pos - chrom_pos.min() + offset
        centers.append((str(c), x[mask].mean()))
        color_idx[mask] = i % 2
        offset = x[mask].max() + gap
    return x, centers, color_idx


def manhattan(
    df: pd.DataFrame,
    *,
    pvalue_col: str = "beta_pvalue",
    label_col: str = "variable",
    chrom_col: Optional[str] = None,
    pos_col: Optional[str] = None,
    cutoffs: Optional[Sequence[tuple[float, str, str]]] = None,
    num_labeled: int = 10,
    figsize: tuple[float, float] = (12, 6),
    dpi: int = 150,
    title: Optional[str] = None,
    colors: Sequence[str] = _DEFAULT_COLORS,
    ax: Optional[plt.Axes] = None,
    output_path: Optional[PathLike] = None,
) -> Figure:
    """
    Render a Manhattan plot of ``-log10(p)`` against variable position.

    When ``chrom_col`` is supplied, points are grouped by chromosome with
    alternating colours; otherwise variables are laid out in input order
    along the x-axis. ``cutoffs`` is an iterable of ``(threshold, color,
    linestyle)`` tuples drawn as horizontal reference lines (in raw
    p-value space — the function takes care of the ``-log10`` conversion).
    The top ``num_labeled`` rows by smallest p-value are annotated using
    ``label_col``.

    Returns the :class:`matplotlib.figure.Figure` so the caller can
    further customise it. If ``output_path`` is provided, the figure is
    also written to disk (extension determines the format).
    """
    if pvalue_col not in df.columns:
        raise ValueError(
            f"pvalue_col {pvalue_col!r} not in df columns: {list(df.columns)}"
        )
    if label_col not in df.columns:
        raise ValueError(
            f"label_col {label_col!r} not in df columns: {list(df.columns)}"
        )

    df = df.dropna(subset=[pvalue_col]).reset_index(drop=True)
    if len(df) == 0:
        raise ValueError("df is empty after dropping rows with missing p-values")

    y = _neglog10(df[pvalue_col])
    x, centers, color_idx = _build_xaxis(df, chrom_col, pos_col)

    own_layout = ax is None
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    else:
        fig = ax.figure

    palette = list(colors)
    for ci, color in enumerate(palette):
        mask = color_idx == ci
        ax.scatter(x[mask], y[mask], s=10, c=color, alpha=0.8, edgecolors="none")

    if cutoffs:
        for threshold, color, linestyle in cutoffs:
            ax.axhline(
                -np.log10(threshold),
                color=color,
                linestyle=linestyle,
                linewidth=1.0,
                alpha=0.7,
            )

    if num_labeled > 0:
        top_idx = np.argsort(df[pvalue_col].to_numpy())[: int(num_labeled)]
        for i in top_idx:
            ax.annotate(
                str(df.iloc[i][label_col]),
                (x[i], y[i]),
                xytext=(3, 3),
                textcoords="offset points",
                fontsize=8,
            )

    if centers:
        ax.set_xticks([c for _, c in centers])
        ax.set_xticklabels([name for name, _ in centers])
        ax.set_xlabel("Chromosome")
    else:
        ax.set_xlabel("Variable index")

    ax.set_ylabel(r"$-\log_{10}(p)$")
    if title:
        ax.set_title(title)

    # Skip ``tight_layout`` when ``ax`` was supplied externally — the
    # parent figure (e.g. ``miami_plot``) owns the layout and a nested
    # call would warn about incompatible axes.
    if own_layout:
        fig.tight_layout()
    _save_fig(fig, output_path)
    return fig


def manhattan_bonferroni(
    df: pd.DataFrame,
    *,
    pvalue_col: str = "beta_pvalue",
    alpha: float = 0.05,
    label_col: str = "variable",
    chrom_col: Optional[str] = None,
    pos_col: Optional[str] = None,
    num_labeled: int = 10,
    figsize: tuple[float, float] = (12, 6),
    dpi: int = 150,
    title: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
    output_path: Optional[PathLike] = None,
) -> Figure:
    """
    Manhattan plot with a Bonferroni cutoff line at ``alpha / n_tests``.

    Plots the raw ``pvalue_col`` (no correction is applied to the values
    themselves — only the threshold line is drawn at the Bonferroni
    level). Use :func:`manhattan_fdr` if you have already corrected
    p-values to display.
    """
    n_tests = int(df[pvalue_col].notna().sum())
    if n_tests == 0:
        raise ValueError("no valid p-values in pvalue_col")
    cutoff = alpha / n_tests
    return manhattan(
        df,
        pvalue_col=pvalue_col,
        label_col=label_col,
        chrom_col=chrom_col,
        pos_col=pos_col,
        cutoffs=[(cutoff, "#C0392B", "--")],
        num_labeled=num_labeled,
        figsize=figsize,
        dpi=dpi,
        title=title or f"Bonferroni α={alpha} (n={n_tests})",
        ax=ax,
        output_path=output_path,
    )


def manhattan_fdr(
    df: pd.DataFrame,
    *,
    pvalue_col: str = "p_corrected",
    q_threshold: float = 0.05,
    label_col: str = "variable",
    chrom_col: Optional[str] = None,
    pos_col: Optional[str] = None,
    num_labeled: int = 10,
    figsize: tuple[float, float] = (12, 6),
    dpi: int = 150,
    title: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
    output_path: Optional[PathLike] = None,
) -> Figure:
    """
    Manhattan plot of FDR-corrected p-values with a horizontal cutoff at
    ``q_threshold``.

    Expects ``pvalue_col`` to already hold corrected q-values — typically
    the ``p_corrected`` column produced by
    :meth:`RegressionResults.with_correction`. The threshold line is
    drawn at ``q_threshold`` directly (no further adjustment).
    """
    if pvalue_col not in df.columns:
        raise ValueError(
            f"pvalue_col {pvalue_col!r} not found; pass the corrected "
            f"column name (default: 'p_corrected'). Available: {list(df.columns)}"
        )
    return manhattan(
        df,
        pvalue_col=pvalue_col,
        label_col=label_col,
        chrom_col=chrom_col,
        pos_col=pos_col,
        cutoffs=[(q_threshold, "#C0392B", "--")],
        num_labeled=num_labeled,
        figsize=figsize,
        dpi=dpi,
        title=title or f"FDR q ≤ {q_threshold}",
        ax=ax,
        output_path=output_path,
    )


def miami_plot(
    df_top: pd.DataFrame,
    df_bottom: pd.DataFrame,
    *,
    pvalue_col: str = "beta_pvalue",
    label_col: str = "variable",
    chrom_col: Optional[str] = None,
    pos_col: Optional[str] = None,
    cutoffs: Optional[Sequence[tuple[float, str, str]]] = None,
    num_labeled: int = 5,
    top_label: str = "top",
    bottom_label: str = "bottom",
    figsize: tuple[float, float] = (12, 8),
    dpi: int = 150,
    title: Optional[str] = None,
    colors: Sequence[str] = _DEFAULT_COLORS,
    output_path: Optional[PathLike] = None,
) -> Figure:
    """
    Two stacked Manhattans sharing an x-axis; the lower one is mirrored
    so the eye reads it bottom-up.

    Designed to compare two studies of the same variable universe —
    e.g. a discovery cohort (top) and a replication cohort (bottom),
    or sex-stratified GWAS (males up, females down). Both inputs must
    use the same column names (``pvalue_col``, ``label_col``, etc.).

    The two panels are independent Manhattan renders with shared x.
    Cutoff lines, if provided, are drawn on both panels.
    """
    fig, (ax_top, ax_bottom) = plt.subplots(
        2, 1, figsize=figsize, dpi=dpi, sharex=True,
        gridspec_kw={"hspace": 0.05},
        constrained_layout=True,
    )

    manhattan(
        df_top,
        pvalue_col=pvalue_col,
        label_col=label_col,
        chrom_col=chrom_col,
        pos_col=pos_col,
        cutoffs=cutoffs,
        num_labeled=num_labeled,
        colors=colors,
        ax=ax_top,
        title=None,
    )
    ax_top.set_xlabel("")
    ax_top.text(
        0.01, 0.95, top_label, transform=ax_top.transAxes,
        ha="left", va="top", fontsize=10,
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.7},
    )

    manhattan(
        df_bottom,
        pvalue_col=pvalue_col,
        label_col=label_col,
        chrom_col=chrom_col,
        pos_col=pos_col,
        cutoffs=cutoffs,
        num_labeled=num_labeled,
        colors=colors,
        ax=ax_bottom,
        title=None,
    )
    ax_bottom.invert_yaxis()
    ax_bottom.text(
        0.01, 0.05, bottom_label, transform=ax_bottom.transAxes,
        ha="left", va="bottom", fontsize=10,
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.7},
    )

    if title:
        fig.suptitle(title)
    # ``constrained_layout=True`` (set when the figure was created)
    # handles spacing automatically and is compatible with the inverted
    # bottom axis — no manual ``tight_layout`` call needed.

    _save_fig(fig, output_path)
    return fig
