from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import pandas as pd
from matplotlib.figure import Figure

from igem.core.components.base_component import BaseComponent
from igem.modules import plot as _plot
from igem.modules.analyze.results import RegressionResults
from igem.modules.data import Genotypes, Phenotypes

PathLike = str | Path


class PlotComponent(BaseComponent):
    """
    Visual layer over IGEM analysis results.

    Each method is a thin wrapper over a free function in
    :mod:`igem.modules.plot`; both surfaces stay in sync via the
    ``TestFacadeCoverage`` guard.
    """

    # ------------------------------------------------------------------
    # Bridges (typed objects in)
    # ------------------------------------------------------------------
    def from_results(
        self,
        results: RegressionResults,
        *,
        kind: str = "auto",
        n_top: int = 20,
        cutoff: Optional[float] = None,
        output_path: Optional[PathLike] = None,
        title: Optional[str] = None,
        **primitive_kwargs: Any,
    ) -> Figure:
        return _plot.from_results(
            results,
            kind=kind,
            n_top=n_top,
            cutoff=cutoff,
            output_path=output_path,
            title=title,
            **primitive_kwargs,
        )

    def from_describe(
        self,
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
        return _plot.from_describe(
            phen,
            cols=cols,
            continuous_kind=continuous_kind,
            grid=grid,
            figsize_per_panel=figsize_per_panel,
            dpi=dpi,
            sort_by_kind=sort_by_kind,
            output_path=output_path,
        )

    def from_modify(
        self,
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
        return _plot.from_modify(
            before,
            after,
            var=var,
            metric=metric,
            layout=layout,
            continuous_kind=continuous_kind,
            title=title,
            figsize=figsize,
            dpi=dpi,
            output_path=output_path,
        )

    def from_interaction(
        self,
        results: RegressionResults,
        *,
        kind: str = "auto",
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
        return _plot.from_interaction(
            results,
            kind=kind,
            value=value,
            n_top=n_top,
            cutoff=cutoff,
            cluster=cluster,
            annotate=annotate,
            figsize=figsize,
            dpi=dpi,
            output_path=output_path,
            title=title,
            **primitive_kwargs,
        )

    def suggest_plots(self, obj: Any) -> list[str]:
        return _plot.suggest_plots(obj)

    # ------------------------------------------------------------------
    # Primitives (DataFrames / arrays in — escape hatch)
    # ------------------------------------------------------------------
    def manhattan(
        self,
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
        ax: Optional[Any] = None,
        output_path: Optional[PathLike] = None,
    ) -> Figure:
        return _plot.manhattan(
            df,
            pvalue_col=pvalue_col,
            label_col=label_col,
            chrom_col=chrom_col,
            pos_col=pos_col,
            cutoffs=cutoffs,
            num_labeled=num_labeled,
            figsize=figsize,
            dpi=dpi,
            title=title,
            ax=ax,
            output_path=output_path,
        )

    def manhattan_fdr(
        self,
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
        ax: Optional[Any] = None,
        output_path: Optional[PathLike] = None,
    ) -> Figure:
        return _plot.manhattan_fdr(
            df,
            pvalue_col=pvalue_col,
            q_threshold=q_threshold,
            label_col=label_col,
            chrom_col=chrom_col,
            pos_col=pos_col,
            num_labeled=num_labeled,
            figsize=figsize,
            dpi=dpi,
            title=title,
            ax=ax,
            output_path=output_path,
        )

    def manhattan_bonferroni(
        self,
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
        ax: Optional[Any] = None,
        output_path: Optional[PathLike] = None,
    ) -> Figure:
        return _plot.manhattan_bonferroni(
            df,
            pvalue_col=pvalue_col,
            alpha=alpha,
            label_col=label_col,
            chrom_col=chrom_col,
            pos_col=pos_col,
            num_labeled=num_labeled,
            figsize=figsize,
            dpi=dpi,
            title=title,
            ax=ax,
            output_path=output_path,
        )

    def qq_plot(
        self,
        pvalues,
        *,
        title: Optional[str] = None,
        show_lambda: bool = True,
        ci: bool = True,
        ci_alpha: float = 0.05,
        figsize: tuple[float, float] = (6, 6),
        dpi: int = 150,
        ax: Optional[Any] = None,
        output_path: Optional[PathLike] = None,
    ) -> Figure:
        return _plot.qq_plot(
            pvalues,
            title=title,
            show_lambda=show_lambda,
            ci=ci,
            ci_alpha=ci_alpha,
            figsize=figsize,
            dpi=dpi,
            ax=ax,
            output_path=output_path,
        )

    def dotplot(
        self,
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
        return _plot.dotplot(
            df,
            label_col=label_col,
            pvalue_col=pvalue_col,
            beta_col=beta_col,
            ci_low_col=ci_low_col,
            ci_high_col=ci_high_col,
            n_top=n_top,
            cutoff=cutoff,
            figsize=figsize,
            dpi=dpi,
            title=title,
            output_path=output_path,
        )

    def distribution(
        self,
        series: pd.Series,
        *,
        kind: Optional[str] = None,
        continuous_kind: str = "hist",
        title: Optional[str] = None,
        figsize: tuple[float, float] = (6, 4),
        dpi: int = 150,
        color: str = "#53868B",
        ax: Optional[Any] = None,
        output_path: Optional[PathLike] = None,
    ) -> Figure:
        return _plot.distribution(
            series,
            kind=kind,
            continuous_kind=continuous_kind,
            title=title,
            figsize=figsize,
            dpi=dpi,
            color=color,
            ax=ax,
            output_path=output_path,
        )

    def heatmap(
        self,
        df: pd.DataFrame,
        *,
        row_col: str = "term1",
        col_col: str = "term2",
        value_col: str = "lrt_pvalue",
        sign_col: Optional[str] = None,
        transform: str = "neglog10",
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
        return _plot.heatmap(
            df,
            row_col=row_col,
            col_col=col_col,
            value_col=value_col,
            sign_col=sign_col,
            transform=transform,
            symmetric=symmetric,
            cluster=cluster,
            annotate=annotate,
            cmap=cmap,
            cutoff=cutoff,
            figsize=figsize,
            dpi=dpi,
            title=title,
            output_path=output_path,
        )

    def miami_plot(
        self,
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
        output_path: Optional[PathLike] = None,
    ) -> Figure:
        return _plot.miami_plot(
            df_top,
            df_bottom,
            pvalue_col=pvalue_col,
            label_col=label_col,
            chrom_col=chrom_col,
            pos_col=pos_col,
            cutoffs=cutoffs,
            num_labeled=num_labeled,
            top_label=top_label,
            bottom_label=bottom_label,
            figsize=figsize,
            dpi=dpi,
            title=title,
            output_path=output_path,
        )
