"""Dotplot primitive — top-results style two-panel figure."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

PathLike = str | Path


def dotplot(
    df: pd.DataFrame,
    *,
    label_col: str = "variable",
    pvalue_col: str = "beta_pvalue",
    beta_col: str = "beta",
    ci_low_col: Optional[str] = "ci_low",
    ci_high_col: Optional[str] = "ci_high",
    n_top: int = 20,
    cutoff: Optional[float] = 0.05,
    figsize: Optional[tuple[float, float]] = None,
    dpi: int = 150,
    title: Optional[str] = None,
    output_path: Optional[PathLike] = None,
) -> Figure:
    """
    Two-panel dotplot of the top ``n_top`` rows by ``pvalue_col``.

    The left panel shows ``-log10(p)`` per row (with an optional
    vertical line at ``cutoff``); the right panel shows ``beta`` with
    horizontal CI bars when ``ci_low_col`` / ``ci_high_col`` are
    available. Rows are sorted by p-value (smallest at the top).

    Returns the :class:`matplotlib.figure.Figure`. If ``output_path`` is
    given, the figure is also written to disk.
    """
    required = [label_col, pvalue_col, beta_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"dotplot requires columns {required}; missing: {missing}. "
            f"Available: {list(df.columns)}"
        )

    sub = (
        df.dropna(subset=[pvalue_col])
        .sort_values(pvalue_col)
        .head(int(n_top))
        .reset_index(drop=True)
    )
    if len(sub) == 0:
        raise ValueError("no rows left after dropping missing p-values")

    has_ci = (
        ci_low_col is not None
        and ci_high_col is not None
        and ci_low_col in sub.columns
        and ci_high_col in sub.columns
    )

    if figsize is None:
        figsize = (10.0, max(4.0, 0.35 * len(sub) + 1.5))
    fig, (ax_p, ax_b) = plt.subplots(
        1, 2, figsize=figsize, dpi=dpi, sharey=True
    )

    y_pos = np.arange(len(sub))[::-1]  # largest p at bottom
    neglog_p = -np.log10(sub[pvalue_col].to_numpy(dtype=float))

    ax_p.scatter(neglog_p, y_pos, s=30, c="#53868B", edgecolors="none")
    if cutoff is not None:
        ax_p.axvline(
            -np.log10(cutoff),
            color="#C0392B",
            linestyle="--",
            linewidth=1.0,
            alpha=0.7,
        )
    ax_p.set_xlabel(r"$-\log_{10}(p)$")
    ax_p.set_yticks(y_pos)
    ax_p.set_yticklabels(sub[label_col].astype(str).tolist())
    ax_p.invert_yaxis()

    beta = sub[beta_col].to_numpy(dtype=float)
    if has_ci:
        low = sub[ci_low_col].to_numpy(dtype=float)
        high = sub[ci_high_col].to_numpy(dtype=float)
        ax_b.errorbar(
            beta,
            y_pos,
            xerr=[beta - low, high - beta],
            fmt="o",
            color="#4D4D4D",
            ecolor="#888888",
            elinewidth=1.0,
            capsize=3,
            markersize=5,
        )
    else:
        ax_b.scatter(beta, y_pos, s=30, c="#4D4D4D", edgecolors="none")
    ax_b.axvline(0, color="#888888", linestyle=":", linewidth=1.0)
    ax_b.set_xlabel("β")

    if title:
        fig.suptitle(title)
        fig.tight_layout(rect=(0, 0, 1, 0.96))
    else:
        fig.tight_layout()

    if output_path is not None:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(out, bbox_inches="tight")
    return fig
