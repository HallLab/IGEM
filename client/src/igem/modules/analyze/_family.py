"""Family detection for regression outcomes."""
from __future__ import annotations

import pandas as pd

VALID_FAMILIES = ("linear", "logistic")


def infer_family(series: pd.Series) -> str:
    """
    Pick a regression family from the dtype and values of an outcome.

    - boolean dtype → ``logistic``
    - numeric with exactly two unique non-NaN values that are
      ``{0, 1}`` (int or float) → ``logistic``
    - any other numeric dtype → ``linear``
    - everything else → raise ``ValueError``
    """
    if pd.api.types.is_bool_dtype(series):
        return "logistic"
    if pd.api.types.is_numeric_dtype(series):
        non_na = series.dropna()
        if len(non_na) == 0:
            raise ValueError(
                "outcome has no non-NaN values; cannot infer family"
            )
        unique = set(non_na.unique().tolist())
        if unique <= {0, 1, 0.0, 1.0}:
            return "logistic"
        return "linear"
    raise ValueError(
        f"cannot infer regression family from outcome dtype={series.dtype}"
    )


def validate_family(family: str) -> None:
    if family not in VALID_FAMILIES:
        raise ValueError(
            f"family must be one of {VALID_FAMILIES}; got {family!r}"
        )
