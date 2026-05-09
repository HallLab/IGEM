"""Bridge: Phenotypes -> grid of distribution plots."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure

from igem.modules.data import Phenotypes
from igem.modules.describe import summarize
from igem.modules.plot.primitives.distribution import distribution

PathLike = str | Path

_KIND_ORDER = {"binary": 0, "categorical": 1, "continuous": 2}


def from_describe(
    phen: Phenotypes,
    *,
    cols: Optional[Iterable[str]] = None,
    continuous_kind: str = "hist",
    grid: tuple[int, int] = (3, 4),
    figsize_per_panel: tuple[float, float] = (3.5, 2.5),
    dpi: int = 150,
    sort_by_kind: bool = True,
    output_path: Optional[PathLike] = None,
) -> list[Figure]:
    """
    Render distribution plots for the columns of a :class:`Phenotypes`.

    Each column is classified through :func:`igem.modules.describe.summarize`
    (so the ``kind`` shown matches what the user sees in summary tables)
    and plotted with the corresponding :func:`distribution` primitive.
    Panels are laid out in a ``grid = (rows, cols)`` grid; once a page
    is full, a new :class:`Figure` is started. The function returns the
    full list of figures so callers (or :func:`dashboard_pdf` in Phase 4)
    can compose them further.

    When ``output_path`` is given it must end in ``.pdf`` — distributions
    are inherently multi-page and PDF is the only natural single-file
    container. Per-figure image saves stay the caller's responsibility
    (iterate the returned list and call ``fig.savefig`` per figure).
    """
    if output_path is not None:
        suffix = Path(output_path).suffix.lower()
        if suffix != ".pdf":
            raise ValueError(
                f"from_describe only supports multi-page .pdf output, "
                f"got {suffix!r}. Iterate the returned list to save "
                f"individual figures."
            )

    summary = summarize(phen, cols=cols)
    if sort_by_kind:
        summary = summary.assign(
            _order=summary["kind"].map(_KIND_ORDER).fillna(99),
        ).sort_values(["_order", "column"]).drop(columns="_order")

    targets = list(summary["column"])
    if not targets:
        raise ValueError("no columns to plot (Phenotypes has no usable columns)")

    rows, cols_per_page = grid
    panels_per_page = rows * cols_per_page
    figsize = (
        figsize_per_panel[0] * cols_per_page,
        figsize_per_panel[1] * rows,
    )

    figures: list[Figure] = []
    for page_start in range(0, len(targets), panels_per_page):
        page_cols = targets[page_start : page_start + panels_per_page]
        fig, axes = plt.subplots(rows, cols_per_page, figsize=figsize, dpi=dpi)
        flat_axes = axes.flatten() if hasattr(axes, "flatten") else [axes]

        for ax, col in zip(flat_axes, page_cols):
            kind = summary.loc[summary["column"] == col, "kind"].iloc[0]
            distribution(
                phen.df[col],
                kind=kind,
                continuous_kind=continuous_kind,
                color="#53868B",
                ax=ax,
                title=col,
            )

        # Hide unused panels on the last page
        for ax in flat_axes[len(page_cols):]:
            ax.set_visible(False)

        fig.tight_layout()
        figures.append(fig)

    if output_path is not None:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        with PdfPages(out) as pdf:
            for fig in figures:
                pdf.savefig(fig, bbox_inches="tight")
    return figures
