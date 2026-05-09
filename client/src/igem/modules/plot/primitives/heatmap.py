"""Pairwise heatmap primitive — designed around interaction-study output."""
from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

PathLike = str | Path

Transform = Literal["neglog10", "raw", "signed_neglog10"]
_TRANSFORMS = ("neglog10", "raw", "signed_neglog10")


def _apply_transform(
    matrix: np.ndarray, transform: Transform, sign: Optional[np.ndarray],
) -> np.ndarray:
    if transform == "raw":
        return matrix
    if transform == "neglog10":
        with np.errstate(divide="ignore", invalid="ignore"):
            return -np.log10(np.where(matrix > 0, matrix, np.nan))
    if transform == "signed_neglog10":
        if sign is None:
            raise ValueError(
                "signed_neglog10 needs `sign_col` to provide the sign per cell"
            )
        with np.errstate(divide="ignore", invalid="ignore"):
            mag = -np.log10(np.where(matrix > 0, matrix, np.nan))
        return mag * np.sign(sign)
    raise ValueError(f"transform={transform!r}; expected one of {_TRANSFORMS}")


def _hierarchical_order(matrix: np.ndarray) -> np.ndarray:
    """Return a row/column ordering from a single linkage on |matrix|."""
    from scipy.cluster.hierarchy import leaves_list, linkage
    from scipy.spatial.distance import squareform

    finite = np.where(np.isnan(matrix), 0.0, matrix)
    # Treat the matrix as a similarity → distance via 1 - normalised
    if finite.max() != 0:
        sim = finite / finite.max()
    else:
        sim = finite
    # symmetric distance with zero diagonal
    dist = 1.0 - np.abs(sim)
    np.fill_diagonal(dist, 0.0)
    dist = (dist + dist.T) / 2.0
    condensed = squareform(dist, checks=False)
    order = leaves_list(linkage(condensed, method="average"))
    return order


def heatmap(
    df: pd.DataFrame,
    *,
    row_col: str = "term1",
    col_col: str = "term2",
    value_col: str = "lrt_pvalue",
    sign_col: Optional[str] = None,
    transform: Transform = "neglog10",
    symmetric: bool = True,
    cluster: bool = False,
    annotate: bool = False,
    cmap: str = "viridis",
    cutoff: Optional[float] = None,
    figsize: tuple[float, float] = (8, 7),
    dpi: int = 150,
    title: Optional[str] = None,
    output_path: Optional[PathLike] = None,
) -> Figure:
    """
    Pairwise heatmap of ``value_col`` indexed by ``(row_col, col_col)``.

    Designed around the long-format output of
    :func:`igem.analyze.interaction_study` (one row per
    ``(term1, term2)`` pair) — :func:`pivot_table` rebuilds the matrix.
    When ``symmetric=True`` (default), entries are mirrored across the
    diagonal so the same pair shown as ``(a, b)`` also appears at
    ``(b, a)``.

    ``transform`` controls the colour scale:

    * ``"neglog10"`` (default) — ``-log10(value)``; suitable for
      p-values where smaller is more significant.
    * ``"raw"`` — value as-is.
    * ``"signed_neglog10"`` — magnitude as ``-log10`` multiplied by
      ``sign(sign_col)``; useful when you want to combine effect
      direction (β) with significance into a single colour.

    Optional features:

    * ``cluster=True`` reorders rows/cols using hierarchical clustering
      on the absolute value matrix (single linkage on average distance).
    * ``annotate=True`` writes the (transformed) value in each cell.
    * ``cutoff`` adds a contour-style mark on cells whose raw value
      crosses the threshold (useful to highlight nominally significant
      interactions before correction).
    """
    required = [row_col, col_col, value_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"heatmap requires columns {required}; missing: {missing}. "
            f"Available: {list(df.columns)}"
        )
    if transform not in _TRANSFORMS:
        raise ValueError(f"transform={transform!r}; expected {_TRANSFORMS}")

    work = df[required].dropna(subset=[value_col]).copy()
    if sign_col is not None:
        if sign_col not in df.columns:
            raise ValueError(
                f"sign_col {sign_col!r} not in df columns: {list(df.columns)}"
            )
        work[sign_col] = df[sign_col].to_numpy()

    levels = sorted(set(work[row_col]) | set(work[col_col]), key=str)
    n = len(levels)
    if n == 0:
        raise ValueError("no terms left after dropping missing values")

    raw = pd.DataFrame(np.nan, index=levels, columns=levels)
    sign = (
        pd.DataFrame(np.nan, index=levels, columns=levels)
        if sign_col is not None
        else None
    )
    for _, row in work.iterrows():
        a, b = row[row_col], row[col_col]
        v = row[value_col]
        raw.at[a, b] = v
        if symmetric:
            raw.at[b, a] = v
        if sign is not None:
            s = row[sign_col]
            sign.at[a, b] = s
            if symmetric:
                sign.at[b, a] = s

    matrix = raw.to_numpy(dtype=float)
    sign_arr = sign.to_numpy(dtype=float) if sign is not None else None
    display = _apply_transform(matrix, transform, sign_arr)
    raw_arr = matrix  # for cutoff annotation

    if cluster and n > 1:
        order = _hierarchical_order(np.where(np.isnan(display), 0.0, display))
        display = display[order][:, order]
        raw_arr = raw_arr[order][:, order]
        levels = [levels[i] for i in order]

    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    im = ax.imshow(display, cmap=cmap, aspect="auto")
    fig.colorbar(im, ax=ax, label=_colorbar_label(transform, value_col))

    ax.set_xticks(np.arange(n))
    ax.set_yticks(np.arange(n))
    ax.set_xticklabels(levels, rotation=45, ha="right")
    ax.set_yticklabels(levels)

    if annotate:
        for i in range(n):
            for j in range(n):
                if np.isfinite(display[i, j]):
                    ax.text(
                        j, i, f"{display[i, j]:.2f}",
                        ha="center", va="center",
                        color="white" if display[i, j] > np.nanmean(display) else "black",
                        fontsize=7,
                    )

    if cutoff is not None:
        # Outline cells crossing the cutoff (smaller raw value than
        # cutoff → significant, treated as a "pass").
        for i in range(n):
            for j in range(n):
                if np.isfinite(raw_arr[i, j]) and raw_arr[i, j] < cutoff:
                    ax.add_patch(
                        plt.Rectangle(
                            (j - 0.5, i - 0.5), 1, 1,
                            fill=False, edgecolor="white", linewidth=1.2,
                        )
                    )

    if title:
        ax.set_title(title)

    fig.tight_layout()
    if output_path is not None:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(out, bbox_inches="tight")
    return fig


def _colorbar_label(transform: Transform, value_col: str) -> str:
    if transform == "neglog10":
        return rf"$-\log_{{10}}$({value_col})"
    if transform == "signed_neglog10":
        return rf"sign $\cdot -\log_{{10}}$({value_col})"
    return value_col
