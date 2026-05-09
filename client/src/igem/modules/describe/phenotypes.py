"""
Phenotype descriptive statistics.

All functions are read-only — they return new pandas DataFrames /
dicts and never mutate the input :class:`Phenotypes`. They auto-skip
the ``sample_id`` column unless asked for it explicitly via ``cols``.
"""
from __future__ import annotations

from typing import Any, Iterable, Literal, Optional

import numpy as np
import pandas as pd
from scipy.stats import skew as _skew, skewtest as _skewtest

from igem.modules.data import Phenotypes

_SKEWTEST_MIN_N = 8
_BINARY_NUNIQUE = 2
_NEAR_ZERO_VAR_CV = 1e-3
_TUKEY_K = 1.5
_OUTLIER_MIN_N = 4


# ----------------------------------------------------------------------
# summarize
# ----------------------------------------------------------------------
_SUMMARY_COLUMNS = [
    "column", "dtype", "kind",
    "n", "n_missing", "missing_pct", "n_unique",
    "mean", "std", "min", "q25", "median", "q75", "max",
    "mode", "mode_count",
    "near_zero_var", "n_outliers",
]


def summarize(
    phen: Phenotypes,
    *,
    cols: Optional[Iterable[str]] = None,
    weighted: bool = False,
) -> pd.DataFrame:
    """
    Per-column summary statistics. One row per column.

    Numeric columns receive distribution stats (mean, std, min, q25,
    median, q75, max); non-numeric columns receive cardinality stats
    (n_unique, mode, mode_count). Counts (n, n_missing, missing_pct)
    are always computed.

    The ``sample_id`` column is skipped by default. Pass it in ``cols``
    explicitly to include it.

    With ``weighted=True``, distribution stats (mean, std, quantiles,
    weighted mode) are computed using ``phen.weights_col`` via
    ``statsmodels.stats.weightstats.DescrStatsW``. Counts (``n``,
    ``n_missing``) remain unweighted by survey-statistics convention.
    Raises ``ValueError`` if ``weighted=True`` and ``weights_col`` is
    not set on the wrapper.
    """
    df = phen.df
    target_cols = _resolve_cols(phen, cols, exclude_sample_id=cols is None)
    if weighted:
        if phen.weights_col is None:
            raise ValueError(
                "summarize(weighted=True) requires phen.weights_col to be set"
            )
        weights = df[phen.weights_col]
        rows = [_weighted_summary_row(df[c], weights, c) for c in target_cols]
    else:
        rows = [_summary_row(df[c], c) for c in target_cols]
    return pd.DataFrame(rows, columns=_SUMMARY_COLUMNS)


def _summary_row(s: pd.Series, name: str) -> dict:
    n = len(s)
    n_missing = int(s.isna().sum())
    missing_pct = round(100.0 * n_missing / n, 2) if n else 0.0
    n_unique = int(s.nunique(dropna=True))
    is_numeric = (
        pd.api.types.is_numeric_dtype(s)
        and not pd.api.types.is_bool_dtype(s)
    )
    # Binary takes precedence over continuous/categorical: any column
    # with exactly 2 distinct non-NaN values is binary regardless of
    # dtype (covers {0,1} ints, {True,False} bools, {"yes","no"} strs).
    if n_unique == _BINARY_NUNIQUE:
        kind = "binary"
    elif is_numeric:
        kind = "continuous"
    else:
        kind = "categorical"
    row: dict = {
        "column": name,
        "dtype": str(s.dtype),
        "kind": kind,
        "n": n,
        "n_missing": n_missing,
        "missing_pct": missing_pct,
        "n_unique": n_unique,
        "mean": np.nan, "std": np.nan,
        "min": np.nan, "q25": np.nan, "median": np.nan,
        "q75": np.nan, "max": np.nan,
        "mode": np.nan, "mode_count": np.nan,
        "near_zero_var": False, "n_outliers": np.nan,
    }

    if is_numeric:
        non_na = s.dropna()
        if len(non_na):
            row["mean"] = float(non_na.mean())
            row["min"] = float(non_na.min())
            row["q25"] = float(non_na.quantile(0.25))
            row["median"] = float(non_na.median())
            row["q75"] = float(non_na.quantile(0.75))
            row["max"] = float(non_na.max())
        if len(non_na) > 1:
            row["std"] = float(non_na.std())
        row["n_outliers"] = _count_tukey_outliers(
            non_na, row["q25"], row["q75"]
        )
    else:
        modes = s.mode(dropna=True)
        if len(modes):
            mode = modes.iloc[0]
            row["mode"] = mode
            row["mode_count"] = int((s == mode).sum())

    row["near_zero_var"] = _is_near_zero_var(
        n_unique=n_unique,
        is_numeric=is_numeric,
        mean=row["mean"],
        std=row["std"],
    )
    return row


def _is_near_zero_var(
    *, n_unique: int, is_numeric: bool, mean: float, std: float,
) -> bool:
    """
    True for constants (any kind) or numeric columns whose coefficient
    of variation falls below ``_NEAR_ZERO_VAR_CV``.
    """
    if n_unique <= 1:
        return True
    if not is_numeric:
        return False
    if np.isnan(std):
        return False
    if std == 0:
        return True
    if mean == 0:
        return std < 1e-9
    return abs(std / mean) < _NEAR_ZERO_VAR_CV


def _count_tukey_outliers(
    non_na: pd.Series, q25: float, q75: float,
) -> float:
    """
    Count of points outside ``[Q1 - k·IQR, Q3 + k·IQR]`` (k=1.5).
    Returns ``NaN`` if the column has fewer than ``_OUTLIER_MIN_N``
    valid observations or if IQR is undefined.
    """
    if len(non_na) < _OUTLIER_MIN_N or np.isnan(q25) or np.isnan(q75):
        return np.nan
    iqr = q75 - q25
    if iqr <= 0:
        return 0
    lower = q25 - _TUKEY_K * iqr
    upper = q75 + _TUKEY_K * iqr
    return int(((non_na < lower) | (non_na > upper)).sum())


def _weighted_summary_row(
    s: pd.Series, w: pd.Series, name: str,
) -> dict:
    from statsmodels.stats.weightstats import DescrStatsW

    n = len(s)
    n_missing = int(s.isna().sum())
    missing_pct = round(100.0 * n_missing / n, 2) if n else 0.0
    n_unique = int(s.nunique(dropna=True))
    is_numeric = (
        pd.api.types.is_numeric_dtype(s)
        and not pd.api.types.is_bool_dtype(s)
    )
    if n_unique == _BINARY_NUNIQUE:
        kind = "binary"
    elif is_numeric:
        kind = "continuous"
    else:
        kind = "categorical"
    row: dict = {
        "column": name,
        "dtype": str(s.dtype),
        "kind": kind,
        "n": n,
        "n_missing": n_missing,
        "missing_pct": missing_pct,
        "n_unique": n_unique,
        "mean": np.nan, "std": np.nan,
        "min": np.nan, "q25": np.nan, "median": np.nan,
        "q75": np.nan, "max": np.nan,
        "mode": np.nan, "mode_count": np.nan,
        "near_zero_var": False, "n_outliers": np.nan,
    }

    mask = s.notna() & w.notna()
    s_valid = s[mask]
    w_valid = w[mask]
    if len(s_valid) == 0:
        row["near_zero_var"] = _is_near_zero_var(
            n_unique=n_unique, is_numeric=is_numeric,
            mean=row["mean"], std=row["std"],
        )
        return row

    if is_numeric:
        ds = DescrStatsW(
            s_valid.to_numpy(dtype=float),
            weights=w_valid.to_numpy(dtype=float),
        )
        row["mean"] = float(ds.mean)
        # min/max are extrema, weights don't apply.
        row["min"] = float(s_valid.min())
        row["max"] = float(s_valid.max())
        if len(s_valid) > 1:
            row["std"] = float(ds.std)
        try:
            quantiles = ds.quantile([0.25, 0.5, 0.75], return_pandas=False)
            row["q25"] = float(quantiles[0])
            row["median"] = float(quantiles[1])
            row["q75"] = float(quantiles[2])
        except Exception:
            pass
        row["n_outliers"] = _count_tukey_outliers(
            s_valid, row["q25"], row["q75"]
        )
    else:
        # Weighted mode: bucket value → sum of weights → argmax.
        bucket = (
            pd.DataFrame({"v": s_valid.values, "w": w_valid.values})
            .groupby("v")["w"].sum()
        )
        if len(bucket):
            mode_val = bucket.idxmax()
            row["mode"] = mode_val
            # mode_count remains a raw observation count.
            row["mode_count"] = int((s == mode_val).sum())

    row["near_zero_var"] = _is_near_zero_var(
        n_unique=n_unique, is_numeric=is_numeric,
        mean=row["mean"], std=row["std"],
    )
    return row


# ----------------------------------------------------------------------
# missing_report
# ----------------------------------------------------------------------
def missing_report(
    phen: Phenotypes,
    *,
    cols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Missing-value summary per column, sorted by ``missing_pct`` desc.

    Returns columns: ``column``, ``dtype``, ``n_missing``, ``missing_pct``.
    """
    df = phen.df
    target_cols = _resolve_cols(phen, cols, exclude_sample_id=False)
    n = len(df)
    rows = []
    for col in target_cols:
        s = df[col]
        n_missing = int(s.isna().sum())
        rows.append(
            {
                "column": col,
                "dtype": str(s.dtype),
                "n_missing": n_missing,
                "missing_pct": round(100.0 * n_missing / n, 2) if n else 0.0,
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(
            "missing_pct", ascending=False
        ).reset_index(drop=True)
    return out


# ----------------------------------------------------------------------
# correlation_matrix
# ----------------------------------------------------------------------
def correlation_matrix(
    phen: Phenotypes,
    *,
    cols: Optional[Iterable[str]] = None,
    method: Literal["pearson", "spearman", "kendall"] = "pearson",
) -> pd.DataFrame:
    """
    Pairwise correlation between numeric columns.

    Defaults to Pearson. With ``cols=None`` the function picks all
    numeric, non-bool, non-sample-id columns. Passing non-numeric
    columns explicitly raises ``ValueError``.
    """
    if method not in ("pearson", "spearman", "kendall"):
        raise ValueError(
            f"method must be one of 'pearson', 'spearman', 'kendall'; "
            f"got {method!r}"
        )

    df = phen.df
    if cols is None:
        target = [
            c for c in df.columns
            if c != phen.sample_id_col
            and pd.api.types.is_numeric_dtype(df[c])
            and not pd.api.types.is_bool_dtype(df[c])
        ]
    else:
        target = list(cols)
        for c in target:
            if c not in df.columns:
                raise ValueError(
                    f"column {c!r} not in dataframe: {list(df.columns)}"
                )
            if not pd.api.types.is_numeric_dtype(df[c]):
                raise ValueError(
                    f"column {c!r} is not numeric (dtype={df[c].dtype})"
                )

    if not target:
        return pd.DataFrame()
    return df[target].corr(method=method)


# ----------------------------------------------------------------------
# correlation_pairs
# ----------------------------------------------------------------------
def correlation_pairs(
    phen: Phenotypes,
    *,
    cols: Optional[Iterable[str]] = None,
    method: Literal["pearson", "spearman", "kendall"] = "pearson",
    threshold: float = 0.75,
    absolute: bool = True,
) -> pd.DataFrame:
    """
    Pairs of numeric columns whose correlation crosses a threshold.

    Returns a long-format DataFrame with columns ``var1``, ``var2``,
    ``r``, sorted by ``|r|`` desc. With ``absolute=True`` (default), the
    threshold is applied to ``|r|`` so anti-correlated pairs are kept.

    Complements :func:`correlation_matrix`: same column selection rules
    (numeric, non-bool, non-sample-id by default), but output is the
    upper triangle filtered by ``threshold``.
    """
    if not 0.0 <= threshold <= 1.0:
        raise ValueError(
            f"threshold must be in [0, 1]; got {threshold}"
        )

    matrix = correlation_matrix(phen, cols=cols, method=method)
    if matrix.empty:
        return pd.DataFrame(columns=["var1", "var2", "r"])

    arr = matrix.to_numpy()
    names = list(matrix.columns)
    rows = []
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            r = arr[i, j]
            if pd.isna(r):
                continue
            score = abs(r) if absolute else r
            if score >= threshold:
                rows.append({"var1": names[i], "var2": names[j], "r": float(r)})

    out = pd.DataFrame(rows, columns=["var1", "var2", "r"])
    if not out.empty:
        out = out.reindex(
            out["r"].abs().sort_values(ascending=False).index
        ).reset_index(drop=True)
    return out


# ----------------------------------------------------------------------
# crosstab
# ----------------------------------------------------------------------
def crosstab(
    phen: Phenotypes,
    var1: str,
    var2: str,
    *,
    normalize: bool | Literal["all", "index", "columns"] = False,
    margins: bool = False,
    dropna: bool = True,
) -> pd.DataFrame:
    """
    Two-way contingency table between ``var1`` (rows) and ``var2``
    (columns).

    Thin wrapper around :func:`pandas.crosstab` with ``Phenotypes``-aware
    column validation. Useful for GxE / GxG joint distributions and
    for rare-cell detection (small N in a genotype × exposure cell
    means low power for that interaction).

    ``normalize`` follows pandas semantics: ``False`` (counts), ``"all"``
    (proportion of grand total), ``"index"`` (row-normalized),
    ``"columns"`` (column-normalized). ``margins=True`` adds row/column
    totals labelled ``All``.
    """
    df = phen.df
    for v in (var1, var2):
        if v not in df.columns:
            raise ValueError(
                f"column {v!r} not in dataframe: {list(df.columns)}"
            )
    return pd.crosstab(
        df[var1], df[var2],
        normalize=normalize,
        margins=margins,
        dropna=dropna,
    )


# ----------------------------------------------------------------------
# value_counts
# ----------------------------------------------------------------------
def value_counts(
    phen: Phenotypes,
    *,
    cols: Optional[Iterable[str]] = None,
    top: int = 20,
    dropna: bool = False,
) -> dict[str, pd.DataFrame]:
    """
    Per-column frequency tables. Returns ``{column_name: DataFrame}``
    where each DataFrame has columns ``value``, ``count``, ``pct``.

    ``top`` caps the number of rows per column. ``dropna`` controls
    whether NaN counts as its own bucket.
    """
    if top <= 0:
        raise ValueError(f"top must be positive; got {top}")

    df = phen.df
    target_cols = _resolve_cols(phen, cols, exclude_sample_id=cols is None)

    out: dict[str, pd.DataFrame] = {}
    for col in target_cols:
        s = df[col]
        counts = s.value_counts(dropna=dropna)
        head = counts.head(top)
        denom = int(s.notna().sum()) if dropna else int(s.size)
        result = head.reset_index()
        result.columns = ["value", "count"]
        result["pct"] = (
            (result["count"] / denom * 100.0).round(2)
            if denom else 0.0
        )
        out[col] = result
    return out


# ----------------------------------------------------------------------
# summarize_by
# ----------------------------------------------------------------------
def summarize_by(
    phen: Phenotypes,
    *,
    by: str,
    cols: Optional[Iterable[str]] = None,
    dropna_group: bool = True,
) -> pd.DataFrame:
    """
    Per-group, per-column summary statistics — long format.

    Output columns: same as :func:`summarize` plus a leading column
    named after ``by`` carrying the group value. One row per
    ``(group, column)`` pair.

    The grouping column itself is excluded from ``cols`` (its summary
    inside its own group would be trivial). With ``dropna_group=True``
    (default), rows whose ``by`` value is NaN are dropped before
    grouping; set to ``False`` to include a NaN group.

    To pivot to wide format with one row per column and group-suffixed
    stat columns, do::

        long = summarize_by(phen, by="SEX")
        wide = long.set_index([by, "column"]).unstack(by)
    """
    df = phen.df
    if by not in df.columns:
        raise ValueError(
            f"by={by!r} not in dataframe: {list(df.columns)}"
        )

    target_cols = _resolve_cols(phen, cols, exclude_sample_id=cols is None)
    target_cols = [c for c in target_cols if c != by]

    rows = []
    for group_val, group_df in df.groupby(by, dropna=dropna_group):
        for c in target_cols:
            row = _summary_row(group_df[c], c)
            row[by] = group_val
            rows.append(row)

    return pd.DataFrame(rows, columns=[by, *_SUMMARY_COLUMNS])


# ----------------------------------------------------------------------
# dataset_summary
# ----------------------------------------------------------------------
def dataset_summary(phen: Phenotypes) -> dict[str, Any]:
    """
    Dataset-level overview as a flat dict.

    Returned keys:
      - ``n_samples``, ``n_columns`` (excluding ``sample_id_col``)
      - ``n_continuous``, ``n_binary``, ``n_categorical``
      - ``n_with_missing``, ``total_missing_pct``
      - ``n_outcomes``, ``n_covariates``, ``n_exposures``
      - ``has_survey_design`` — true if any of weights/strata/cluster
        columns are set on the wrapper
    """
    summary = summarize(phen)
    kind_counts = summary["kind"].value_counts().to_dict()
    n_with_missing = int((summary["n_missing"] > 0).sum())
    total_cells = int(summary["n"].sum())
    total_missing = int(summary["n_missing"].sum())
    total_missing_pct = (
        round(100.0 * total_missing / total_cells, 2)
        if total_cells else 0.0
    )

    return {
        "n_samples": int(len(phen.df)),
        "n_columns": int(len(summary)),
        "n_continuous": int(kind_counts.get("continuous", 0)),
        "n_binary": int(kind_counts.get("binary", 0)),
        "n_categorical": int(kind_counts.get("categorical", 0)),
        "n_with_missing": n_with_missing,
        "total_missing_pct": total_missing_pct,
        "n_outcomes": len(phen.outcomes),
        "n_covariates": len(phen.covariates),
        "n_exposures": len(phen.exposures),
        "has_survey_design": any(
            col is not None
            for col in (phen.weights_col, phen.strata_col, phen.cluster_col)
        ),
    }


# ----------------------------------------------------------------------
# skewness
# ----------------------------------------------------------------------
def skewness(
    phen: Phenotypes,
    *,
    cols: Optional[Iterable[str]] = None,
    dropna: bool = False,
) -> pd.DataFrame:
    """
    Skewness with z-score and p-value for numeric columns.

    Returns columns: ``column``, ``n``, ``skew``, ``zscore``, ``pvalue``.
    With ``cols=None`` picks all numeric, non-bool, non-sample-id
    columns. Passing non-numeric columns explicitly raises
    ``ValueError``.

    With ``dropna=False`` (default), NaNs in a column propagate and the
    column's statistics are NaN. With ``dropna=True``, NaNs are dropped
    before testing. The z-score / p-value test requires at least
    ``_SKEWTEST_MIN_N`` (8) valid observations; below that, ``zscore``
    and ``pvalue`` are NaN but ``skew`` is still computed when possible.
    """
    df = phen.df
    if cols is None:
        target = [
            c for c in df.columns
            if c != phen.sample_id_col
            and pd.api.types.is_numeric_dtype(df[c])
            and not pd.api.types.is_bool_dtype(df[c])
        ]
    else:
        target = list(cols)
        for c in target:
            if c not in df.columns:
                raise ValueError(
                    f"column {c!r} not in dataframe: {list(df.columns)}"
                )
            if not pd.api.types.is_numeric_dtype(df[c]):
                raise ValueError(
                    f"column {c!r} is not numeric (dtype={df[c].dtype})"
                )
            if pd.api.types.is_bool_dtype(df[c]):
                raise ValueError(
                    f"column {c!r} is boolean; skewness undefined"
                )

    rows = [_skewness_row(df[c], c, dropna=dropna) for c in target]
    return pd.DataFrame(
        rows, columns=["column", "n", "skew", "zscore", "pvalue"]
    )


def _skewness_row(s: pd.Series, name: str, *, dropna: bool) -> dict:
    if dropna:
        s = s.dropna()
    n = int(s.notna().sum())
    row = {
        "column": name,
        "n": n,
        "skew": np.nan,
        "zscore": np.nan,
        "pvalue": np.nan,
    }
    if not dropna and s.isna().any():
        return row
    arr = np.asarray(s, dtype=float)
    if n >= 1:
        row["skew"] = float(_skew(arr, nan_policy="omit"))
    if n >= _SKEWTEST_MIN_N:
        result = _skewtest(arr, nan_policy="omit")
        row["zscore"] = float(result.statistic)
        row["pvalue"] = float(result.pvalue)
    return row


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def _resolve_cols(
    phen: Phenotypes,
    cols: Optional[Iterable[str]],
    *,
    exclude_sample_id: bool,
) -> list[str]:
    if cols is None:
        all_cols = list(phen.df.columns)
        if exclude_sample_id:
            all_cols = [c for c in all_cols if c != phen.sample_id_col]
        return all_cols

    target = list(cols)
    missing = [c for c in target if c not in phen.df.columns]
    if missing:
        raise ValueError(
            f"columns not in dataframe: {missing}; "
            f"available: {list(phen.df.columns)}"
        )
    return target
