"""Multi-test correction adapters around statsmodels."""
from __future__ import annotations

from typing import Iterable

import numpy as np

# Method aliases → statsmodels.stats.multitest names
_ALIAS = {
    "bonferroni": "bonferroni",
    "fdr_bh": "fdr_bh",       # Benjamini-Hochberg
    "fdr_by": "fdr_by",       # Benjamini-Yekutieli
    "holm": "holm",
    "sidak": "sidak",
    "hommel": "hommel",
}


def list_methods() -> list[str]:
    return sorted(_ALIAS.keys())


def apply_correction(
    pvalues: Iterable[float],
    method: str = "bonferroni",
) -> np.ndarray:
    """
    Return adjusted p-values for ``method``.

    NaN p-values pass through unchanged so failed regressions don't
    contaminate the correction. The remaining (finite) p-values are
    sent to ``statsmodels.stats.multitest.multipletests`` and the
    output is woven back into the original positions.
    """
    if method not in _ALIAS:
        raise ValueError(
            f"unknown correction method {method!r}; "
            f"valid: {list_methods()}"
        )

    from statsmodels.stats.multitest import multipletests

    pv = np.asarray(list(pvalues), dtype=float)
    out = np.full_like(pv, np.nan, dtype=float)
    valid = ~np.isnan(pv)
    if not valid.any():
        return out

    _, adjusted, _, _ = multipletests(pv[valid], method=_ALIAS[method])
    out[valid] = adjusted
    return out
