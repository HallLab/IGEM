"""Distribution primitive — one column, one panel."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from scipy import stats

PathLike = str | Path

_CONTINUOUS_KINDS = ("hist", "box", "violin", "qq")
_BINARY_NUNIQUE = 2


def detect_kind(series: pd.Series) -> str:
    """
    Classify a series as ``"binary"`` / ``"categorical"`` / ``"continuous"``.

    Mirrors the rule used by :func:`igem.modules.describe.summarize`:
    binary takes precedence (any column with exactly two distinct
    non-NA values), then numeric → continuous, else → categorical.
    """
    n_unique = int(series.nunique(dropna=True))
    is_numeric = (
        pd.api.types.is_numeric_dtype(series)
        and not pd.api.types.is_bool_dtype(series)
    )
    if n_unique == _BINARY_NUNIQUE:
        return "binary"
    if is_numeric:
        return "continuous"
    return "categorical"


def _save_fig(fig: Figure, output_path: Optional[PathLike]) -> None:
    if output_path is None:
        return
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches="tight")


def _plot_categorical(ax: plt.Axes, s: pd.Series, color: str) -> None:
    counts = s.dropna().value_counts().sort_index()
    if len(counts) == 0:
        ax.text(0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes)
        return
    ax.bar(
        np.arange(len(counts)),
        counts.values,
        color=color,
        edgecolor="none",
    )
    ax.set_xticks(np.arange(len(counts)))
    ax.set_xticklabels([str(v) for v in counts.index], rotation=45, ha="right")
    ax.set_ylabel("count")


def _plot_continuous(
    ax: plt.Axes, s: pd.Series, kind: str, color: str,
) -> None:
    values = pd.to_numeric(s, errors="coerce").dropna().to_numpy()
    if len(values) == 0:
        ax.text(0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes)
        return
    if kind == "hist":
        ax.hist(values, bins=30, color=color, edgecolor="white", linewidth=0.5)
        ax.set_ylabel("count")
    elif kind == "box":
        ax.boxplot(values, vert=False, patch_artist=True,
                   boxprops={"facecolor": color, "edgecolor": "#333333"})
        ax.set_yticks([])
    elif kind == "violin":
        parts = ax.violinplot(values, vert=False, showmeans=False, showmedians=True)
        for body in parts["bodies"]:
            body.set_facecolor(color)
            body.set_edgecolor("#333333")
        ax.set_yticks([])
    elif kind == "qq":
        stats.probplot(values, dist="norm", plot=ax)
        # probplot does its own labelling; recolour the points
        ax.get_lines()[0].set_markerfacecolor(color)
        ax.get_lines()[0].set_markeredgecolor("none")
        ax.set_title("")  # probplot sets its own title; clear it
    else:  # pragma: no cover — guarded earlier
        raise ValueError(f"unknown continuous_kind={kind!r}")


def distribution(
    series: pd.Series,
    *,
    kind: Optional[str] = None,
    continuous_kind: str = "hist",
    title: Optional[str] = None,
    figsize: tuple[float, float] = (6, 4),
    dpi: int = 150,
    color: str = "#53868B",
    ax: Optional[plt.Axes] = None,
    output_path: Optional[PathLike] = None,
) -> Figure:
    """
    Render the distribution of a single column.

    ``kind`` is auto-detected from the series when ``None`` (binary if
    ``nunique == 2``, continuous if numeric, categorical otherwise) —
    the same rule used by :func:`igem.modules.describe.summarize`. For
    continuous columns, ``continuous_kind`` selects the visualisation:
    ``"hist"`` (default), ``"box"``, ``"violin"``, or ``"qq"``.

    Returns the :class:`matplotlib.figure.Figure`. If ``output_path`` is
    given, the figure is also written to disk.
    """
    if continuous_kind not in _CONTINUOUS_KINDS:
        raise ValueError(
            f"continuous_kind={continuous_kind!r}; expected one of "
            f"{_CONTINUOUS_KINDS}"
        )
    resolved = kind if kind is not None else detect_kind(series)
    if resolved not in ("binary", "categorical", "continuous"):
        raise ValueError(
            f"kind={kind!r}; expected 'binary', 'categorical', "
            f"'continuous', or None (auto)"
        )

    if ax is None:
        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    else:
        fig = ax.figure

    if resolved == "continuous":
        _plot_continuous(ax, series, continuous_kind, color)
    else:
        _plot_categorical(ax, series, color)

    label = str(series.name) if series.name is not None else "value"
    ax.set_xlabel(label)
    if title:
        ax.set_title(title)

    fig.tight_layout()
    _save_fig(fig, output_path)
    return fig
