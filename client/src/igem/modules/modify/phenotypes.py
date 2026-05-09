"""
Phenotype transformations.

Each function takes a :class:`Phenotypes` and returns a new
:class:`Phenotypes`, preserving the original's role metadata
(``sample_id_col``, outcomes, covariates, exposures, survey columns).

Free functions are stateless; the :class:`ModifyComponent` wrapper adds
logging when called via ``igem.modify.*``.
"""
from __future__ import annotations

from typing import Any, Callable, Iterable, Literal, Optional

import numpy as np
import pandas as pd

from igem.modules.data import Phenotypes


_TRANSFORM_METHODS = (
    "log", "log1p", "sqrt", "rank_int", "boxcox", "zscore",
)
TransformMethod = Literal[
    "log", "log1p", "sqrt", "rank_int", "boxcox", "zscore",
]


# ----------------------------------------------------------------------
# discretize
# ----------------------------------------------------------------------
def discretize(
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
# transform
# ----------------------------------------------------------------------
def transform(
    phen: Phenotypes,
    col: str,
    *,
    method: Optional[TransformMethod] = None,
    func: Optional[Callable[[pd.Series], pd.Series]] = None,
    new_col: Optional[str] = None,
    replace: bool = False,
) -> Phenotypes:
    """
    Apply a numeric transformation to ``col``.

    Exactly one of ``method`` (whitelist string) or ``func`` (callable)
    must be provided.

    Whitelist methods (apply elementwise to non-NaN values):

    - ``"log"``       — ``log(x)``; NaN for ``x ≤ 0``.
    - ``"log1p"``     — ``log(1 + x)``; NaN for ``x < -1``.
    - ``"sqrt"``      — ``sqrt(x)``; NaN for ``x < 0``.
    - ``"rank_int"``  — Rank Inverse Normal Transform:
      :math:`\\Phi^{-1}\\!\\big((r_i - 0.5) / n\\big)` where
      :math:`r_i` is the average rank of :math:`x_i` among non-NaN
      observations. Ties receive the mean rank. Produces an
      approximately standard-normal output regardless of the source
      distribution — standard practice in EWAS / GxE for non-normal
      phenotypes (Beasley et al., 2009).
    - ``"boxcox"``    — Box-Cox with auto-fit :math:`\\lambda`
      (``scipy.stats.boxcox``); requires all values strictly positive
      (raises ``ValueError`` otherwise).
    - ``"zscore"``    — :math:`(x - \\bar{x}) / \\sigma`.

    Custom ``func`` receives a ``pandas.Series`` and must return a
    Series of the same length. NaNs are passed through unless the
    callable explicitly handles them.

    By default the result goes to ``f"{col}_<method>"`` (or
    ``f"{col}_transformed"`` for callable ``func``); pass ``new_col``
    to override or ``replace=True`` to overwrite ``col`` in place.
    """
    if (method is None) == (func is None):
        raise ValueError(
            "transform() requires exactly one of 'method' or 'func'"
        )
    if col not in phen.df.columns:
        raise ValueError(
            f"column {col!r} not in dataframe: {list(phen.df.columns)}"
        )

    series = phen.df[col]
    if not pd.api.types.is_numeric_dtype(series):
        raise ValueError(
            f"transform() requires a numeric column; "
            f"got {col!r} with dtype {series.dtype}"
        )

    if method is not None:
        if method not in _TRANSFORM_METHODS:
            raise ValueError(
                f"method must be one of {list(_TRANSFORM_METHODS)}; "
                f"got {method!r}"
            )
        transformed = _apply_method(series, method)
        suffix = method
    else:
        transformed = pd.Series(func(series), index=series.index)
        if len(transformed) != len(series):
            raise ValueError(
                f"custom func must return a Series of length {len(series)}; "
                f"got {len(transformed)}"
            )
        suffix = "transformed"

    target = col if replace else (new_col or f"{col}_{suffix}")
    new_df = phen.df.copy()
    new_df[target] = transformed
    return _clone_with_df(phen, new_df)


def _apply_method(series: pd.Series, method: str) -> pd.Series:
    arr = series.to_numpy(dtype=float)
    mask = ~np.isnan(arr)

    if method == "log":
        with np.errstate(invalid="ignore", divide="ignore"):
            out = np.where(arr > 0, np.log(arr), np.nan)
        return pd.Series(out, index=series.index)

    if method == "log1p":
        with np.errstate(invalid="ignore", divide="ignore"):
            out = np.where(arr > -1, np.log1p(arr), np.nan)
        return pd.Series(out, index=series.index)

    if method == "sqrt":
        with np.errstate(invalid="ignore"):
            out = np.where(arr >= 0, np.sqrt(arr), np.nan)
        return pd.Series(out, index=series.index)

    if method == "zscore":
        valid = arr[mask]
        if len(valid) < 2:
            return pd.Series(np.full_like(arr, np.nan), index=series.index)
        mu = float(np.mean(valid))
        sigma = float(np.std(valid, ddof=1))
        if sigma == 0:
            return pd.Series(np.zeros_like(arr) * np.nan, index=series.index)
        return pd.Series((arr - mu) / sigma, index=series.index)

    if method == "rank_int":
        from scipy.stats import norm
        s = pd.Series(arr, index=series.index)
        # Average ranks among non-NaN; NaNs preserved.
        ranks = s.rank(method="average", na_option="keep")
        n = int(mask.sum())
        if n == 0:
            return s
        quantiles = (ranks - 0.5) / n
        return pd.Series(norm.ppf(quantiles), index=series.index)

    if method == "boxcox":
        from scipy.stats import boxcox
        valid = arr[mask]
        if (valid <= 0).any():
            raise ValueError(
                "boxcox requires all values strictly positive; "
                "encountered non-positive values"
            )
        if len(valid) < 2:
            raise ValueError(
                "boxcox requires at least 2 valid observations"
            )
        transformed_valid, _lambda = boxcox(valid)
        out = np.full_like(arr, np.nan)
        out[mask] = transformed_valid
        return pd.Series(out, index=series.index)

    raise ValueError(f"unknown method: {method!r}")  # pragma: no cover


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
# auto_classify
# ----------------------------------------------------------------------
def auto_classify(
    phen: Phenotypes,
    *,
    cat_min: int = 3,
    cat_max: int = 6,
    cont_min: int = 15,
) -> pd.DataFrame:
    """
    Per-column type classification report (no coercion).

    Returns a DataFrame with one row per column (excluding
    ``sample_id_col``):

        column, n_unique, dtype, kind

    Where ``kind`` is one of:

    - ``"constant"`` — :math:`n_{\\text{unique}} \\leq 1`
    - ``"binary"`` — :math:`n_{\\text{unique}} = 2`
    - ``"categorical"`` — :math:`\\text{cat\\_min} \\leq n_{\\text{unique}} \\leq \\text{cat\\_max}`
    - ``"continuous"`` — :math:`n_{\\text{unique}} \\geq \\text{cont\\_min}` and numeric (non-bool)
    - ``"unknown"`` — anything else (e.g., 7–14 unique values, or
      high-cardinality non-numeric)

    Defaults follow CLARITE conventions
    (``cat_min=3``, ``cat_max=6``, ``cont_min=15``).

    This is a *report* — no dtype coercion is applied. To act on the
    classification, dispatch to :func:`make_binary`,
    :func:`make_categorical`, or :func:`make_continuous` using the
    ``column`` lists from each kind.
    """
    rows = []
    for col in phen.df.columns:
        if col == phen.sample_id_col:
            continue
        s = phen.df[col]
        n_unique = int(s.nunique(dropna=True))
        is_numeric = (
            pd.api.types.is_numeric_dtype(s)
            and not pd.api.types.is_bool_dtype(s)
        )
        if n_unique <= 1:
            kind = "constant"
        elif n_unique == 2:
            kind = "binary"
        elif cat_min <= n_unique <= cat_max:
            kind = "categorical"
        elif n_unique >= cont_min and is_numeric:
            kind = "continuous"
        else:
            kind = "unknown"
        rows.append(
            {
                "column": col,
                "n_unique": n_unique,
                "dtype": str(s.dtype),
                "kind": kind,
            }
        )
    return pd.DataFrame(rows, columns=["column", "n_unique", "dtype", "kind"])


# ----------------------------------------------------------------------
# make_binary / make_categorical / make_continuous
# ----------------------------------------------------------------------
def make_binary(
    phen: Phenotypes,
    *,
    skip: Optional[Iterable[str]] = None,
    only: Optional[Iterable[str]] = None,
) -> Phenotypes:
    """
    Coerce columns to a binary representation.

    Each target column must have exactly 2 distinct non-NaN values,
    otherwise ``ValueError`` is raised. Numeric ``{0, 1}`` columns are
    coerced to nullable ``Int64``; everything else is coerced to a
    ``pandas.CategoricalDtype`` with the 2 observed levels (ordered by
    sort).
    """
    target = _resolve_target_cols(phen, skip=skip, only=only)
    new_df = phen.df.copy()
    for col in target:
        s = new_df[col]
        n_unique = int(s.nunique(dropna=True))
        if n_unique != 2:
            raise ValueError(
                f"make_binary: {col!r} has {n_unique} distinct non-NaN "
                f"values, expected exactly 2"
            )
        unique_vals = sorted(s.dropna().unique().tolist())
        if pd.api.types.is_numeric_dtype(s) and set(unique_vals) <= {0, 1}:
            new_df[col] = s.astype("Int64")
        else:
            new_df[col] = s.astype(
                pd.CategoricalDtype(categories=unique_vals)
            )
    return _clone_with_df(phen, new_df)


def make_categorical(
    phen: Phenotypes,
    *,
    skip: Optional[Iterable[str]] = None,
    only: Optional[Iterable[str]] = None,
) -> Phenotypes:
    """
    Coerce columns to ``pandas.CategoricalDtype``.

    Useful when low-cardinality numeric codes (e.g., ICD chapter,
    education level 1–5) should not enter linear models as continuous.
    """
    target = _resolve_target_cols(phen, skip=skip, only=only)
    new_df = phen.df.copy()
    for col in target:
        new_df[col] = new_df[col].astype("category")
    return _clone_with_df(phen, new_df)


def make_continuous(
    phen: Phenotypes,
    *,
    skip: Optional[Iterable[str]] = None,
    only: Optional[Iterable[str]] = None,
) -> Phenotypes:
    """
    Coerce columns to ``float64``.

    Uses ``pd.to_numeric(errors="raise")`` — non-coercible values
    cause a ``ValueError`` rather than silently becoming NaN. To
    tolerate dirty input, clean it with :func:`recode` first.
    """
    target = _resolve_target_cols(phen, skip=skip, only=only)
    new_df = phen.df.copy()
    for col in target:
        new_df[col] = pd.to_numeric(new_df[col], errors="raise")
    return _clone_with_df(phen, new_df)


def _resolve_target_cols(
    phen: Phenotypes,
    *,
    skip: Optional[Iterable[str]],
    only: Optional[Iterable[str]],
) -> list[str]:
    """skip/only resolution shared by colfilter and make_* functions."""
    cols = [c for c in phen.df.columns if c != phen.sample_id_col]
    if only is not None:
        only_set = set(only)
        cols = [c for c in cols if c in only_set]
    if skip is not None:
        skip_set = set(skip)
        cols = [c for c in cols if c not in skip_set]
    return cols


# ----------------------------------------------------------------------
# remove_outliers
# ----------------------------------------------------------------------
def remove_outliers(
    phen: Phenotypes,
    cols: Optional[Iterable[str]] = None,
    *,
    method: Literal["iqr", "gaussian"] = "iqr",
    k: float = 1.5,
    cutoff: float = 3.0,
) -> Phenotypes:
    """
    Replace outliers in numeric columns with NaN.

    Two detection methods are supported:

    - ``"iqr"`` (default) — Tukey's rule
      :math:`x \\notin [Q_1 - k \\cdot \\text{IQR},\\; Q_3 + k \\cdot \\text{IQR}]`,
      with :math:`\\text{IQR} = Q_3 - Q_1`. ``k=1.5`` is the standard
      "outlier" threshold; ``k=3.0`` flags only "extreme" outliers
      (Tukey, 1977).
    - ``"gaussian"`` — z-score rule
      :math:`|x - \\bar{x}| / \\sigma > \\text{cutoff}`. Default
      ``cutoff=3.0`` matches the common 3-σ convention.

    With ``cols=None`` operates on all numeric (non-bool, non-sample-id)
    columns. Non-numeric columns are skipped silently when picked
    automatically; passing them explicitly raises ``ValueError``.

    Outliers are *replaced with NaN*, not dropped — paired with
    ``drop_missing`` if you want to remove the rows entirely.
    """
    if method not in ("iqr", "gaussian"):
        raise ValueError(
            f"method must be 'iqr' or 'gaussian'; got {method!r}"
        )
    if k <= 0:
        raise ValueError(f"k must be positive; got {k}")
    if cutoff <= 0:
        raise ValueError(f"cutoff must be positive; got {cutoff}")

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

    new_df = df.copy()
    for col in target:
        new_df[col] = _mask_outliers(
            new_df[col], method=method, k=k, cutoff=cutoff,
        )
    return _clone_with_df(phen, new_df)


def _mask_outliers(
    s: pd.Series, *, method: str, k: float, cutoff: float,
) -> pd.Series:
    arr = s.to_numpy(dtype=float)
    mask_valid = ~np.isnan(arr)
    if mask_valid.sum() < 2:
        return s

    if method == "iqr":
        q1 = float(np.nanquantile(arr, 0.25))
        q3 = float(np.nanquantile(arr, 0.75))
        iqr = q3 - q1
        if iqr <= 0:
            return s
        lower = q1 - k * iqr
        upper = q3 + k * iqr
        outlier_mask = (arr < lower) | (arr > upper)
    else:  # gaussian
        valid = arr[mask_valid]
        mu = float(np.mean(valid))
        sigma = float(np.std(valid, ddof=1))
        if sigma == 0:
            return s
        outlier_mask = np.abs(arr - mu) / sigma > cutoff

    out = arr.copy()
    out[outlier_mask & mask_valid] = np.nan
    return pd.Series(out, index=s.index)


# ----------------------------------------------------------------------
# merge_observations / merge_variables
# ----------------------------------------------------------------------
def merge_observations(
    top: Phenotypes,
    bottom: Phenotypes,
) -> Phenotypes:
    """
    Vertical concatenation of two phenotype frames.

    Both wrappers must use the same ``sample_id_col``. Only columns
    present in both frames are kept (intersection); columns unique to
    one side are silently dropped — this is the CLARITE convention and
    mirrors the most defensive interpretation of "same study, more
    samples".

    Roles are unioned: if ``top`` has ``outcomes=["BMI"]`` and
    ``bottom`` has ``outcomes=["GLUCOSE"]`` and both columns survive
    the intersection, the result has ``outcomes=["BMI", "GLUCOSE"]``.
    Sample-IDs are *not* deduplicated — overlapping IDs raise
    ``ValueError`` to prevent silent duplication.
    """
    if top.sample_id_col != bottom.sample_id_col:
        raise ValueError(
            f"sample_id_col mismatch: {top.sample_id_col!r} vs "
            f"{bottom.sample_id_col!r}"
        )
    common_cols = [c for c in top.df.columns if c in bottom.df.columns]
    if top.sample_id_col not in common_cols:
        raise ValueError(
            f"sample_id_col {top.sample_id_col!r} missing from one of "
            f"the input frames"
        )

    overlap = set(top.df[top.sample_id_col]).intersection(
        set(bottom.df[bottom.sample_id_col])
    )
    if overlap:
        raise ValueError(
            f"merge_observations: {len(overlap)} sample_id(s) appear in "
            f"both frames (e.g. {sorted(overlap)[:3]}); deduplicate "
            f"before merging"
        )

    merged = pd.concat(
        [top.df[common_cols], bottom.df[common_cols]],
        ignore_index=True,
    )
    return _build_filtered(
        merged,
        sample_id_col=top.sample_id_col,
        outcomes=_dedup_preserve_order(list(top.outcomes) + list(bottom.outcomes)),
        covariates=_dedup_preserve_order(list(top.covariates) + list(bottom.covariates)),
        exposures=_dedup_preserve_order(list(top.exposures) + list(bottom.exposures)),
        weights_col=top.weights_col or bottom.weights_col,
        strata_col=top.strata_col or bottom.strata_col,
        cluster_col=top.cluster_col or bottom.cluster_col,
    )


def merge_variables(
    left: Phenotypes,
    right: Phenotypes,
    *,
    how: Literal["outer", "inner", "left", "right"] = "outer",
) -> Phenotypes:
    """
    Horizontal merge of two phenotype frames by ``sample_id_col``.

    Both wrappers must use the same ``sample_id_col``. The merge
    follows ``pandas.merge`` semantics with ``how`` controlling how
    sample-IDs unique to one side are handled (default ``"outer"``
    keeps all). Overlapping non-id columns receive ``_x`` / ``_y``
    suffixes — clean those up with :func:`colfilter` /
    :func:`recode` if needed.

    Roles are unioned across both wrappers and filtered to columns
    that survived the merge.
    """
    if left.sample_id_col != right.sample_id_col:
        raise ValueError(
            f"sample_id_col mismatch: {left.sample_id_col!r} vs "
            f"{right.sample_id_col!r}"
        )
    if how not in ("outer", "inner", "left", "right"):
        raise ValueError(
            f"how must be one of outer/inner/left/right; got {how!r}"
        )

    merged = left.df.merge(right.df, on=left.sample_id_col, how=how)
    return _build_filtered(
        merged,
        sample_id_col=left.sample_id_col,
        outcomes=_dedup_preserve_order(list(left.outcomes) + list(right.outcomes)),
        covariates=_dedup_preserve_order(list(left.covariates) + list(right.covariates)),
        exposures=_dedup_preserve_order(list(left.exposures) + list(right.exposures)),
        weights_col=left.weights_col or right.weights_col,
        strata_col=left.strata_col or right.strata_col,
        cluster_col=left.cluster_col or right.cluster_col,
    )


def _build_filtered(
    df: pd.DataFrame,
    *,
    sample_id_col: str,
    outcomes: Iterable[str],
    covariates: Iterable[str],
    exposures: Iterable[str],
    weights_col: Optional[str],
    strata_col: Optional[str],
    cluster_col: Optional[str],
) -> Phenotypes:
    """Build a Phenotypes from raw role lists, dropping any role
    entries that point to columns not present in ``df``."""
    cols = set(df.columns)
    return Phenotypes(
        df,
        sample_id_col=sample_id_col,
        outcomes=[c for c in outcomes if c in cols],
        covariates=[c for c in covariates if c in cols],
        exposures=[c for c in exposures if c in cols],
        weights_col=(
            weights_col if weights_col is None or weights_col in cols
            else None
        ),
        strata_col=(
            strata_col if strata_col is None or strata_col in cols
            else None
        ),
        cluster_col=(
            cluster_col if cluster_col is None or cluster_col in cols
            else None
        ),
    )


def _dedup_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# ----------------------------------------------------------------------
# colfilter family
# ----------------------------------------------------------------------
def colfilter(
    phen: Phenotypes,
    *,
    skip: Optional[Iterable[str]] = None,
    only: Optional[Iterable[str]] = None,
) -> Phenotypes:
    """
    Keep / drop columns by name.

    With ``only=`` keeps the listed columns (plus ``sample_id_col``
    always); with ``skip=`` drops the listed columns. The two can be
    combined: ``only`` is applied first, then ``skip``. The
    ``sample_id_col`` is never dropped — passing it to ``skip`` is a
    no-op for that column.

    Role metadata is filtered automatically: any
    ``outcomes`` / ``covariates`` / ``exposures`` / survey-design
    columns that are dropped are also removed from the wrapper roles.
    """
    cols = list(phen.df.columns)
    if only is not None:
        only_set = set(only) | {phen.sample_id_col}
        cols = [c for c in cols if c in only_set]
    if skip is not None:
        skip_set = set(skip) - {phen.sample_id_col}
        cols = [c for c in cols if c not in skip_set]
    return _clone_filtered(phen, phen.df[cols].copy())


def colfilter_min_n(
    phen: Phenotypes,
    n: int = 200,
    *,
    skip: Optional[Iterable[str]] = None,
    only: Optional[Iterable[str]] = None,
) -> Phenotypes:
    """
    Drop columns with fewer than ``n`` non-NaN values.

    EWAS / PheWAS convention: variables with sparse data inflate the
    type-I error rate at the multiple-testing tail. CLARITE default
    ``n=200`` (Hall et al., 2014).
    """
    if n < 0:
        raise ValueError(f"n must be non-negative; got {n}")
    target = _resolve_target_cols(phen, skip=skip, only=only)
    drop_cols = [c for c in target if int(phen.df[c].notna().sum()) < n]
    keep_cols = [c for c in phen.df.columns if c not in drop_cols]
    return _clone_filtered(phen, phen.df[keep_cols].copy())


def colfilter_min_cat_n(
    phen: Phenotypes,
    n: int = 200,
    *,
    cat_max: int = 6,
    skip: Optional[Iterable[str]] = None,
    only: Optional[Iterable[str]] = None,
) -> Phenotypes:
    """
    Drop binary / categorical columns where any level has fewer than
    ``n`` occurrences.

    A column is treated as binary or categorical when it is a
    ``pandas.CategoricalDtype`` **or** has between 2 and ``cat_max``
    unique non-NaN values (consistent with :func:`auto_classify`
    defaults). Continuous and high-cardinality columns are skipped
    silently.
    """
    if n < 0:
        raise ValueError(f"n must be non-negative; got {n}")
    target = _resolve_target_cols(phen, skip=skip, only=only)
    drop_cols = []
    for col in target:
        s = phen.df[col]
        n_unique = int(s.nunique(dropna=True))
        is_cat_dtype = isinstance(s.dtype, pd.CategoricalDtype)
        is_low_card = 2 <= n_unique <= cat_max
        if not (is_cat_dtype or is_low_card):
            continue
        counts = s.value_counts(dropna=True)
        if (counts < n).any():
            drop_cols.append(col)
    keep_cols = [c for c in phen.df.columns if c not in drop_cols]
    return _clone_filtered(phen, phen.df[keep_cols].copy())


def colfilter_percent_zero(
    phen: Phenotypes,
    *,
    max_zero_pct: float = 90.0,
    skip: Optional[Iterable[str]] = None,
    only: Optional[Iterable[str]] = None,
) -> Phenotypes:
    """
    Drop continuous columns where the share of zeros (among non-NaN
    observations) reaches ``max_zero_pct``.

    Default ``max_zero_pct=90.0`` matches CLARITE. Useful for sparse
    exposures (physical-activity counters, occupational ratings) where
    a feature dominated by zeros carries little discriminative signal.

    Categorical / binary columns are skipped silently — applying a
    zero threshold to a coded factor would be misleading.
    """
    if not 0.0 <= max_zero_pct <= 100.0:
        raise ValueError(
            f"max_zero_pct must be in [0, 100]; got {max_zero_pct}"
        )
    target = _resolve_target_cols(phen, skip=skip, only=only)
    drop_cols = []
    for col in target:
        s = phen.df[col]
        is_numeric = (
            pd.api.types.is_numeric_dtype(s)
            and not pd.api.types.is_bool_dtype(s)
        )
        if not is_numeric:
            continue
        non_na = s.dropna()
        if len(non_na) == 0:
            continue
        zero_pct = 100.0 * float((non_na == 0).sum()) / len(non_na)
        if zero_pct >= max_zero_pct:
            drop_cols.append(col)
    keep_cols = [c for c in phen.df.columns if c not in drop_cols]
    return _clone_filtered(phen, phen.df[keep_cols].copy())


def _clone_filtered(phen: Phenotypes, df: pd.DataFrame) -> Phenotypes:
    """
    Re-wrap a dataframe whose columns may be a subset of the original.
    Roles pointing to columns no longer present are filtered out.
    """
    cols = set(df.columns)
    return Phenotypes(
        df,
        sample_id_col=phen.sample_id_col,
        outcomes=[c for c in phen.outcomes if c in cols],
        covariates=[c for c in phen.covariates if c in cols],
        exposures=[c for c in phen.exposures if c in cols],
        weights_col=(
            phen.weights_col
            if phen.weights_col is None or phen.weights_col in cols
            else None
        ),
        strata_col=(
            phen.strata_col
            if phen.strata_col is None or phen.strata_col in cols
            else None
        ),
        cluster_col=(
            phen.cluster_col
            if phen.cluster_col is None or phen.cluster_col in cols
            else None
        ),
    )


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
# ----------------------------------------------------------------------
# move_variables
# ----------------------------------------------------------------------
def move_variables(
    src: Phenotypes,
    dst: Phenotypes,
    *,
    skip: Optional[Iterable[str]] = None,
    only: Optional[Iterable[str]] = None,
) -> tuple[Phenotypes, Phenotypes]:
    """
    Move columns from ``src`` to ``dst``, returning ``(new_src, new_dst)``.

    Useful when an upstream load lumped variables that belong in
    different roles or different conceptual frames (e.g., a single
    NHANES export with both diet questionnaire items and biomarker
    measurements that you want to track separately).

    ``only`` restricts the move to a list of columns; ``skip``
    excludes columns from the move. Both wrappers must share the same
    ``sample_id_col`` and have matching sample IDs in the same order
    — call :func:`filter_samples` on both first if alignment is
    needed. The ``sample_id_col`` itself is never moved.
    """
    if src.sample_id_col != dst.sample_id_col:
        raise ValueError(
            f"sample_id_col mismatch: {src.sample_id_col!r} vs "
            f"{dst.sample_id_col!r}"
        )
    if not src.df[src.sample_id_col].equals(dst.df[dst.sample_id_col]):
        raise ValueError(
            "src and dst must have matching sample IDs in the same order; "
            "use filter_samples / sort to align them first"
        )

    src_cols = [c for c in src.df.columns if c != src.sample_id_col]
    if only is not None:
        only_set = set(only)
        src_cols = [c for c in src_cols if c in only_set]
    if skip is not None:
        skip_set = set(skip)
        src_cols = [c for c in src_cols if c not in skip_set]

    if not src_cols:
        return src, dst

    # New src: drop the moved columns.
    new_src_df = src.df.drop(columns=src_cols).copy()
    new_src = _clone_filtered(src, new_src_df)

    # New dst: append the moved columns (overwrite collisions silently).
    new_dst_df = dst.df.copy()
    for col in src_cols:
        new_dst_df[col] = src.df[col].values
    new_dst = _clone_with_df(dst, new_dst_df)

    return new_src, new_dst


# ----------------------------------------------------------------------
# rowfilter_incomplete_obs
# ----------------------------------------------------------------------
def rowfilter_incomplete_obs(
    phen: Phenotypes,
    *,
    skip: Optional[Iterable[str]] = None,
    only: Optional[Iterable[str]] = None,
) -> Phenotypes:
    """
    Drop rows where any column has a missing value.

    By default scans every column in the frame. ``only=`` restricts the
    scope to a subset of columns; ``skip=`` removes columns from
    consideration. The ``sample_id_col`` is always considered (a
    missing identifier means the row is unusable).

    Equivalent to ``drop_missing(phen, cols=<all_columns>)`` modulo
    ``skip`` / ``only`` filtering. Provided to match the CLARITE name —
    use whichever name reads better in your pipeline.
    """
    cols = list(phen.df.columns)
    if only is not None:
        only_set = set(only) | {phen.sample_id_col}
        cols = [c for c in cols if c in only_set]
    if skip is not None:
        skip_set = set(skip) - {phen.sample_id_col}
        cols = [c for c in cols if c not in skip_set]
    new_df = phen.df.dropna(subset=cols).copy()
    return _clone_with_df(phen, new_df)


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
