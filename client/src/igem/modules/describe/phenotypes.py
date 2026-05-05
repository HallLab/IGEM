"""
Phenotype descriptive statistics.

All functions are read-only — they return new pandas DataFrames /
dicts and never mutate the input :class:`Phenotypes`. They auto-skip
the ``sample_id`` column unless asked for it explicitly via ``cols``.
"""
from __future__ import annotations

from typing import Iterable, Literal, Optional

import numpy as np
import pandas as pd

from igem.modules.data import Phenotypes


# ----------------------------------------------------------------------
# summarize
# ----------------------------------------------------------------------
_SUMMARY_COLUMNS = [
    "column", "dtype", "kind",
    "n", "n_missing", "missing_pct", "n_unique",
    "mean", "std", "min", "q25", "median", "q75", "max",
    "mode", "mode_count",
]


def summarize(
    phen: Phenotypes,
    *,
    cols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Per-column summary statistics. One row per column.

    Numeric columns receive distribution stats (mean, std, min, q25,
    median, q75, max); non-numeric columns receive cardinality stats
    (n_unique, mode, mode_count). Counts (n, n_missing, missing_pct)
    are always computed.

    The ``sample_id`` column is skipped by default. Pass it in ``cols``
    explicitly to include it.
    """
    df = phen.df
    target_cols = _resolve_cols(phen, cols, exclude_sample_id=cols is None)
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
    row: dict = {
        "column": name,
        "dtype": str(s.dtype),
        "kind": "continuous" if is_numeric else "categorical",
        "n": n,
        "n_missing": n_missing,
        "missing_pct": missing_pct,
        "n_unique": n_unique,
        "mean": np.nan, "std": np.nan,
        "min": np.nan, "q25": np.nan, "median": np.nan,
        "q75": np.nan, "max": np.nan,
        "mode": np.nan, "mode_count": np.nan,
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
    else:
        modes = s.mode(dropna=True)
        if len(modes):
            mode = modes.iloc[0]
            row["mode"] = mode
            row["mode_count"] = int((s == mode).sum())
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
