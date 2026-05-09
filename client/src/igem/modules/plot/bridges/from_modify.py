"""Bridge: before/after comparison for modify operations."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

from igem.modules.data import Genotypes, Phenotypes
from igem.modules.plot.primitives.distribution import (
    detect_kind,
    distribution,
)

PathLike = str | Path

_LAYOUTS = ("overlay", "side_by_side")
_GENO_METRICS = ("maf", "call_rate")

_BEFORE_COLOR = "#53868B"
_AFTER_COLOR = "#C0392B"


def _save_fig(fig: Figure, output_path: Optional[PathLike]) -> None:
    if output_path is None:
        return
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches="tight")


def _phen_overlay_continuous(
    ax: plt.Axes, before: pd.Series, after: pd.Series, var: str,
) -> None:
    bins = 30
    all_vals = pd.concat([before.dropna(), after.dropna()])
    edges = np.histogram_bin_edges(all_vals.to_numpy(dtype=float), bins=bins)
    ax.hist(
        before.dropna().to_numpy(dtype=float),
        bins=edges, alpha=0.55, color=_BEFORE_COLOR,
        label=f"before (n={before.notna().sum()})", edgecolor="white", linewidth=0.4,
    )
    ax.hist(
        after.dropna().to_numpy(dtype=float),
        bins=edges, alpha=0.55, color=_AFTER_COLOR,
        label=f"after (n={after.notna().sum()})", edgecolor="white", linewidth=0.4,
    )
    ax.set_xlabel(var)
    ax.set_ylabel("count")
    ax.legend(loc="best", frameon=False)


def _phen_overlay_categorical(
    ax: plt.Axes, before: pd.Series, after: pd.Series, var: str,
) -> None:
    levels = sorted(
        set(before.dropna().unique()) | set(after.dropna().unique()),
        key=lambda v: (v is None, str(v)),
    )
    x = np.arange(len(levels))
    width = 0.4
    b_counts = before.value_counts().reindex(levels, fill_value=0).to_numpy()
    a_counts = after.value_counts().reindex(levels, fill_value=0).to_numpy()
    ax.bar(x - width / 2, b_counts, width, color=_BEFORE_COLOR, label="before")
    ax.bar(x + width / 2, a_counts, width, color=_AFTER_COLOR, label="after")
    ax.set_xticks(x)
    ax.set_xticklabels([str(v) for v in levels], rotation=45, ha="right")
    ax.set_xlabel(var)
    ax.set_ylabel("count")
    ax.legend(loc="best", frameon=False)


def _from_modify_phen(
    before: Phenotypes,
    after: Phenotypes,
    *,
    var: str,
    layout: str,
    continuous_kind: str,
    title: Optional[str],
    figsize: tuple[float, float],
    dpi: int,
    output_path: Optional[PathLike],
) -> Figure:
    if var not in before.df.columns:
        raise ValueError(
            f"var {var!r} not in `before` Phenotypes; available: "
            f"{list(before.df.columns)}"
        )
    if var not in after.df.columns:
        raise ValueError(
            f"var {var!r} not in `after` Phenotypes; available: "
            f"{list(after.df.columns)}"
        )

    before_s = before.df[var]
    after_s = after.df[var]
    kind = detect_kind(pd.concat([before_s.dropna(), after_s.dropna()]))

    if layout == "side_by_side":
        fig, (ax_b, ax_a) = plt.subplots(1, 2, figsize=figsize, dpi=dpi, sharey=True)
        distribution(
            before_s, kind=kind, continuous_kind=continuous_kind,
            color=_BEFORE_COLOR, ax=ax_b, title=f"before (n={before.n_samples})",
        )
        distribution(
            after_s, kind=kind, continuous_kind=continuous_kind,
            color=_AFTER_COLOR, ax=ax_a, title=f"after (n={after.n_samples})",
        )
    else:  # overlay
        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
        if kind == "continuous":
            _phen_overlay_continuous(ax, before_s, after_s, var)
        else:
            _phen_overlay_categorical(ax, before_s, after_s, var)

    if title is None:
        title = f"{var} — before vs after"
    fig.suptitle(title)
    fig.tight_layout(rect=(0, 0, 1, 0.96))

    _save_fig(fig, output_path)
    return fig


def _from_modify_geno(
    before: Genotypes,
    after: Genotypes,
    *,
    metric: str,
    layout: str,
    continuous_kind: str,
    title: Optional[str],
    figsize: tuple[float, float],
    dpi: int,
    output_path: Optional[PathLike],
) -> Figure:
    if metric not in _GENO_METRICS:
        raise ValueError(
            f"metric={metric!r}; expected one of {_GENO_METRICS}"
        )
    # Local import: ``describe.variant_stats`` pulls sgkit which is
    # heavy; defer until we're sure the geno path is taken.
    from igem.modules.describe import variant_stats

    before_df = variant_stats(before)
    after_df = variant_stats(after)
    if metric not in before_df.columns or metric not in after_df.columns:
        raise ValueError(
            f"metric {metric!r} not produced by variant_stats for these "
            f"genotypes; available: {sorted(set(before_df.columns) | set(after_df.columns))}"
        )

    before_s = before_df[metric]
    after_s = after_df[metric]

    if layout == "side_by_side":
        fig, (ax_b, ax_a) = plt.subplots(1, 2, figsize=figsize, dpi=dpi, sharey=True)
        distribution(
            before_s, kind="continuous", continuous_kind=continuous_kind,
            color=_BEFORE_COLOR, ax=ax_b,
            title=f"before (n_variants={before.n_variants})",
        )
        distribution(
            after_s, kind="continuous", continuous_kind=continuous_kind,
            color=_AFTER_COLOR, ax=ax_a,
            title=f"after (n_variants={after.n_variants})",
        )
    else:  # overlay
        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
        _phen_overlay_continuous(ax, before_s, after_s, metric)

    if title is None:
        title = f"{metric} — before vs after"
    fig.suptitle(title)
    fig.tight_layout(rect=(0, 0, 1, 0.96))

    _save_fig(fig, output_path)
    return fig


def from_modify(
    before,
    after,
    *,
    var: Optional[str] = None,
    metric: str = "maf",
    layout: str = "overlay",
    continuous_kind: str = "hist",
    title: Optional[str] = None,
    figsize: tuple[float, float] = (8, 4),
    dpi: int = 150,
    output_path: Optional[PathLike] = None,
) -> Figure:
    """
    Compare the distribution of one variable / metric before and after a
    modify operation.

    The function dispatches on the input type:

    * Both inputs :class:`Phenotypes` — ``var`` (column name) is required;
      kind is auto-detected from the combined values.
    * Both inputs :class:`Genotypes` — ``var`` is ignored; ``metric``
      selects which per-variant column to compare (``"maf"`` by
      default; ``"call_rate"`` also supported). The bridge calls
      :func:`igem.modules.describe.variant_stats` on each input — be
      mindful with biobank-scale data.

    ``layout="overlay"`` (default) draws both distributions on the same
    axes (semi-transparent histograms for continuous, side-by-side bars
    per level for categorical). ``layout="side_by_side"`` puts the two
    distributions on adjacent axes with a shared y-axis.
    """
    if layout not in _LAYOUTS:
        raise ValueError(f"layout={layout!r}; expected one of {_LAYOUTS}")
    if type(before) is not type(after):
        raise TypeError(
            f"before and after must be the same type; got "
            f"{type(before).__name__} vs {type(after).__name__}"
        )

    if isinstance(before, Phenotypes):
        if var is None:
            raise ValueError("var=... is required when comparing Phenotypes")
        return _from_modify_phen(
            before, after, var=var, layout=layout,
            continuous_kind=continuous_kind, title=title,
            figsize=figsize, dpi=dpi, output_path=output_path,
        )
    if isinstance(before, Genotypes):
        return _from_modify_geno(
            before, after, metric=metric, layout=layout,
            continuous_kind=continuous_kind, title=title,
            figsize=figsize, dpi=dpi, output_path=output_path,
        )
    raise TypeError(
        f"from_modify expects Phenotypes or Genotypes; got "
        f"{type(before).__name__}"
    )
