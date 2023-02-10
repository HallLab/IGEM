from typing import Dict, List, Optional, Tuple

import clarite
import matplotlib.pyplot as plt
import pandas as pd


def distributions(
    data,
    filename: str,
    continuous_kind: str = "count",
    nrows: int = 4,
    ncols: int = 3,
    quality: str = "medium",
    variables: Optional[List[str]] = None,
    sort: bool = True,
):
    return clarite.plot.distributions(
        data,
        filename,
        continuous_kind,
        nrows,
        ncols,
        quality,
        variables,
        sort,
    )


def histogram(
    data,
    column: str,
    figsize: Tuple[int, int] = (12, 5),
    title: Optional[str] = None,
    figure: Optional[plt.figure] = None,
    **kwargs,
):
    return clarite.plot.histogram(
        data,
        column,
        figsize,
        title,
        figure,
        **kwargs,
    )


def manhattan(
    dfs: Dict[str, pd.DataFrame],
    categories: Optional[Dict[str, str]] = None,
    bonferroni: Optional[float] = 0.05,
    fdr: Optional[float] = None,
    num_labeled: int = 3,
    label_vars: Optional[List[str]] = None,
    figsize: Tuple[int, int] = (12, 6),
    dpi: int = 300,
    title: Optional[str] = None,
    figure: Optional[plt.figure] = None,
    colors: List[str] = ["#53868B", "#4D4D4D"],
    background_colors: List[str] = ["#EBEBEB", "#FFFFFF"],
    filename: Optional[str] = None,
    return_figure: bool = False,
):
    return clarite.plot.manhattan(
        dfs,
        categories,
        bonferroni,
        fdr,
        num_labeled,
        label_vars,
        figsize,
        dpi,
        title,
        figure,
        colors,
        background_colors,
        filename,
        return_figure,
    )


def manhattan_bonferroni(
    dfs: Dict[str, pd.DataFrame],
    categories: Optional[Dict[str, str]] = None,
    cutoff: Optional[float] = 0.05,
    num_labeled: int = 3,
    label_vars: Optional[List[str]] = None,
    figsize: Tuple[int, int] = (12, 6),
    dpi: int = 300,
    title: Optional[str] = None,
    figure: Optional[plt.figure] = None,
    colors: List[str] = ["#53868B", "#4D4D4D"],
    background_colors: List[str] = ["#EBEBEB", "#FFFFFF"],
    filename: Optional[str] = None,
    return_figure: bool = False,
):
    return clarite.plot.manhattan_bonferroni(
        dfs,
        categories,
        cutoff,
        num_labeled,
        label_vars,
        figsize,
        dpi,
        title,
        figure,
        colors,
        background_colors,
        filename,
        return_figure,
    )


def manhattan_fdr(
    dfs: Dict[str, pd.DataFrame],
    categories: Optional[Dict[str, str]] = None,
    cutoff: Optional[float] = 0.05,
    num_labeled: int = 3,
    label_vars: Optional[List[str]] = None,
    figsize: Tuple[int, int] = (12, 6),
    dpi: int = 300,
    title: Optional[str] = None,
    figure: Optional[plt.figure] = None,
    colors: List[str] = ["#53868B", "#4D4D4D"],
    background_colors: List[str] = ["#EBEBEB", "#FFFFFF"],
    filename: Optional[str] = None,
    return_figure: bool = False,
):
    return clarite.plot.manhattan_fdr(
        dfs,
        categories,
        cutoff,
        num_labeled,
        label_vars,
        figsize,
        dpi,
        title,
        figure,
        colors,
        background_colors,
        filename,
        return_figure,
    )


def top_results(
    ewas_result: pd.DataFrame,
    pvalue_name: str = "pvalue",
    cutoff: Optional[float] = 0.05,
    num_rows: int = 20,
    figsize: Optional[Tuple[int, int]] = None,
    dpi: int = 300,
    title: Optional[str] = None,
    figure: Optional[plt.figure] = None,
    filename: Optional[str] = None,
):
    return clarite.plot.top_results(
        ewas_result,
        pvalue_name,
        cutoff,
        num_rows,
        figsize,
        dpi,
        title,
        figure,
        filename,
    )
