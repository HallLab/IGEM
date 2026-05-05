"""
Phenotype transformations.

Each function takes a :class:`Phenotypes` and returns a new
:class:`Phenotypes`, preserving the original's role metadata
(``sample_id_col``, outcomes, covariates, exposures, survey columns).

Free functions are stateless; the :class:`ModifyComponent` wrapper adds
logging when called via ``igem.modify.*``.
"""
from __future__ import annotations

from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

from igem.modules.data import Phenotypes


# ----------------------------------------------------------------------
# categorize
# ----------------------------------------------------------------------
def categorize(
    phen: Phenotypes,
    col: str,
    *,
    method: str = "quantiles",
    n_bins: int = 4,
    bin_edges: Optional[Iterable[float]] = None,
    labels: Optional[Iterable[str]] = None,
    new_col: Optional[str] = None,
    replace: bool = False,
) -> Phenotypes:
    """
    Bin a continuous column into ordered categories.

    Parameters
    ----------
    phen : Phenotypes
        Input wrapper.
    col : str
        Continuous column to discretise.
    method : {"quantiles", "bins"}, default "quantiles"
        "quantiles" uses ``pd.qcut`` (equal-count bins). "bins" uses
        ``pd.cut`` with equal-width bins or explicit ``bin_edges``.
    n_bins : int, default 4
        Number of bins when no explicit ``bin_edges`` is given.
    bin_edges : sequence of floats, optional
        Explicit cut points when ``method="bins"``.
    labels : sequence of str, optional
        Names for the produced categories.
    new_col : str, optional
        Output column name. Defaults to ``f"{col}_cat"``.
    replace : bool, default False
        If True, overwrite ``col`` in place of creating ``new_col``.
    """
    if col not in phen.df.columns:
        raise ValueError(
            f"column {col!r} not in dataframe: {list(phen.df.columns)}"
        )

    series = phen.df[col]

    if method == "quantiles":
        cats = pd.qcut(
            series, q=n_bins, labels=list(labels) if labels else None,
            duplicates="drop",
        )
    elif method == "bins":
        bins = list(bin_edges) if bin_edges is not None else n_bins
        cats = pd.cut(
            series, bins=bins, labels=list(labels) if labels else None,
            include_lowest=True,
        )
    else:
        raise ValueError(
            f"method must be 'quantiles' or 'bins'; got {method!r}"
        )

    target = col if replace else (new_col or f"{col}_cat")
    new_df = phen.df.copy()
    new_df[target] = cats
    return _clone_with_df(phen, new_df)


# ----------------------------------------------------------------------
# recode
# ----------------------------------------------------------------------
def recode(
    phen: Phenotypes,
    col: str,
    mapping: dict[Any, Any],
    *,
    missing_values: Optional[Iterable[Any]] = None,
    new_col: Optional[str] = None,
    replace: bool = True,
) -> Phenotypes:
    """
    Apply a value mapping to ``col``.

    Values listed in ``missing_values`` are converted to ``NaN`` before
    the mapping is applied. Values that are neither in ``mapping`` nor
    in ``missing_values`` are left untouched.

    By default the transformation is in-place (``replace=True``) — set
    ``new_col`` and ``replace=False`` to write to a new column.
    """
    if col not in phen.df.columns:
        raise ValueError(
            f"column {col!r} not in dataframe: {list(phen.df.columns)}"
        )

    new_df = phen.df.copy()
    series = new_df[col]

    if missing_values is not None:
        missing = list(missing_values)
        series = series.where(~series.isin(missing), other=np.nan)

    recoded = series.map(lambda v: mapping.get(v, v) if pd.notna(v) else v)

    target = col if replace else (new_col or f"{col}_recoded")
    new_df[target] = recoded
    return _clone_with_df(phen, new_df)


# ----------------------------------------------------------------------
# drop_missing
# ----------------------------------------------------------------------
def drop_missing(
    phen: Phenotypes,
    cols: Optional[Iterable[str]] = None,
) -> Phenotypes:
    """
    Drop rows where any of ``cols`` is missing (NaN).

    If ``cols`` is None, the default set is: sample_id_col + outcomes +
    covariates + exposures + any survey-design columns that are set.
    """
    if cols is None:
        defaults = [
            phen.sample_id_col,
            *phen.outcomes,
            *phen.covariates,
            *phen.exposures,
        ]
        for c in (phen.weights_col, phen.strata_col, phen.cluster_col):
            if c is not None:
                defaults.append(c)
        target_cols = [c for c in defaults if c in phen.df.columns]
    else:
        target_cols = list(cols)
        missing = [c for c in target_cols if c not in phen.df.columns]
        if missing:
            raise ValueError(
                f"columns not in dataframe: {missing}; "
                f"available: {list(phen.df.columns)}"
            )

    if not target_cols:
        return _clone_with_df(phen, phen.df.copy())

    new_df = phen.df.dropna(subset=target_cols).copy()
    return _clone_with_df(phen, new_df)


# ----------------------------------------------------------------------
# internal
# ----------------------------------------------------------------------
def _clone_with_df(phen: Phenotypes, df: pd.DataFrame) -> Phenotypes:
    """Re-wrap a dataframe using the role metadata from ``phen``."""
    return Phenotypes(
        df,
        sample_id_col=phen.sample_id_col,
        outcomes=phen.outcomes,
        covariates=phen.covariates,
        exposures=phen.exposures,
        weights_col=phen.weights_col,
        strata_col=phen.strata_col,
        cluster_col=phen.cluster_col,
    )
