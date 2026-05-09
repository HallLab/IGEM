"""QQ plot primitive with genomic inflation lambda."""
from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.figure import Figure
from scipy import stats

PathLike = str | Path


def genomic_inflation(pvalues: Sequence[float] | np.ndarray) -> float:
    """
    Compute genomic inflation factor λ from a sequence of p-values.

    Defined as ``median(χ²_obs) / median(χ²_expected)`` where the
    observed χ² are derived from the p-values via the inverse 1-df
    chi-square CDF, and the expected median is ``chi2.ppf(0.5, 1)``
    (≈ 0.4549). Values near 1.0 indicate no inflation; values
    materially above 1.0 indicate residual confounding (population
    structure, relatedness, batch effects).
    """
    p = np.asarray(pvalues, dtype=float)
    p = p[np.isfinite(p) & (p > 0) & (p <= 1)]
    if len(p) == 0:
        raise ValueError("no finite p-values in (0, 1] available for lambda")
    chi2_obs = stats.chi2.ppf(1.0 - p, df=1)
    return float(np.median(chi2_obs) / stats.chi2.ppf(0.5, df=1))


def _expected_neglog10(n: int) -> np.ndarray:
    ranks = np.arange(1, n + 1, dtype=float)
    return -np.log10((ranks - 0.5) / n)


def qq_plot(
    pvalues: Sequence[float] | np.ndarray,
    *,
    title: Optional[str] = None,
    show_lambda: bool = True,
    ci: bool = True,
    ci_alpha: float = 0.05,
    figsize: tuple[float, float] = (6, 6),
    dpi: int = 150,
    point_color: str = "#53868B",
    ax: Optional[plt.Axes] = None,
    output_path: Optional[PathLike] = None,
) -> Figure:
    """
    Render a QQ plot of observed vs expected ``-log10(p)``.

    The 1:1 diagonal is drawn as a dashed grey line. When ``ci`` is
    True, a 1 - ``ci_alpha`` confidence band is shaded around the
    expected line using the Beta distribution of order statistics. When
    ``show_lambda`` is True, the genomic inflation factor λ is annotated
    in the lower-right corner.

    Returns the :class:`matplotlib.figure.Figure`. If ``output_path`` is
    given, the figure is also written to disk.
    """
    p = np.asarray(pvalues, dtype=float)
    p = p[np.isfinite(p) & (p > 0) & (p <= 1)]
    n = len(p)
    if n == 0:
        raise ValueError("no finite p-values in (0, 1] to plot")

    observed = -np.log10(np.sort(p))
    expected = _expected_neglog10(n)

    if ax is None:
        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    else:
        fig = ax.figure

    if ci:
        ranks = np.arange(1, n + 1, dtype=float)
        upper = -np.log10(stats.beta.ppf(ci_alpha / 2.0, ranks, n - ranks + 1))
        lower = -np.log10(
            stats.beta.ppf(1.0 - ci_alpha / 2.0, ranks, n - ranks + 1)
        )
        ax.fill_between(expected, lower, upper, color="#CCCCCC", alpha=0.5)

    lim = float(max(expected.max(), observed.max()) * 1.05)
    ax.plot([0, lim], [0, lim], color="#888888", linestyle="--", linewidth=1.0)
    ax.scatter(expected, observed, s=10, c=point_color, edgecolors="none")

    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_xlabel(r"Expected $-\log_{10}(p)$")
    ax.set_ylabel(r"Observed $-\log_{10}(p)$")
    if title:
        ax.set_title(title)

    if show_lambda:
        lam = genomic_inflation(p)
        ax.text(
            0.97,
            0.03,
            rf"$\lambda$ = {lam:.3f}",
            transform=ax.transAxes,
            ha="right",
            va="bottom",
            fontsize=10,
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.8},
        )

    fig.tight_layout()
    if output_path is not None:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(out, bbox_inches="tight")
    return fig
