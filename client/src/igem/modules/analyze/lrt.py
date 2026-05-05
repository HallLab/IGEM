"""Likelihood Ratio Test between nested and full models."""
from __future__ import annotations

from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd
from scipy import stats as _scipy_stats

from igem.modules.analyze._family import infer_family, validate_family
from igem.modules.data import Phenotypes


def lrt(
    phen: Phenotypes,
    outcome: str,
    *,
    full: Iterable[str],
    nested: Iterable[str],
    family: Optional[str] = None,
) -> dict[str, Any]:
    """
    Likelihood Ratio Test of two nested models on the same data.

    Both ``full`` and ``nested`` are lists of regressor column names;
    ``nested`` must be a subset of ``full`` (same outcome, same dataset).
    Both models are fitted on the rows that have no NaN in any of the
    columns referenced by ``full`` (so likelihoods are comparable).

    Returns
    -------
    dict
        ``{"chi2", "df", "p_value", "ll_full", "ll_nested", "n"}``.
    """
    full_list = list(full)
    nested_list = list(nested)

    if outcome not in phen.df.columns:
        raise ValueError(
            f"outcome {outcome!r} not in phenotype dataframe"
        )

    missing_full = [c for c in full_list if c not in phen.df.columns]
    if missing_full:
        raise ValueError(
            f"columns from 'full' not in dataframe: {missing_full}"
        )
    missing_nested = [c for c in nested_list if c not in phen.df.columns]
    if missing_nested:
        raise ValueError(
            f"columns from 'nested' not in dataframe: {missing_nested}"
        )
    if not set(nested_list).issubset(full_list):
        extras = sorted(set(nested_list) - set(full_list))
        raise ValueError(
            f"'nested' must be a subset of 'full'; "
            f"extras in nested: {extras}"
        )
    if len(full_list) == len(nested_list):
        raise ValueError(
            "'full' and 'nested' have the same regressors; LRT requires "
            "the full model to have at least one extra term"
        )

    fam = family or infer_family(phen.df[outcome])
    validate_family(fam)

    cols = [outcome, *full_list]
    sub = phen.df.loc[:, cols].dropna()
    n = len(sub)
    if n < (len(full_list) + 3):
        raise ValueError(
            f"insufficient samples after dropna: n={n} for "
            f"{len(full_list) + 1} parameters"
        )

    ll_full, df_full = _fit_loglik(sub, outcome, full_list, fam)
    ll_nested, df_nested = _fit_loglik(sub, outcome, nested_list, fam)

    chi2 = float(2.0 * (ll_full - ll_nested))
    df_diff = int(df_full - df_nested)
    if chi2 < 0 or df_diff <= 0:
        raise ValueError(
            f"invalid LRT result: chi2={chi2}, df={df_diff} "
            "(check that nested truly nests in full)"
        )
    p_value = float(_scipy_stats.chi2.sf(chi2, df_diff))

    return {
        "chi2": chi2,
        "df": df_diff,
        "p_value": p_value,
        "ll_full": float(ll_full),
        "ll_nested": float(ll_nested),
        "n": int(n),
    }


# ----------------------------------------------------------------------
# internal
# ----------------------------------------------------------------------
def _fit_loglik(
    sub: pd.DataFrame,
    outcome: str,
    regressors: list[str],
    family: str,
) -> tuple[float, int]:
    import statsmodels.api as sm

    if regressors:
        X = sm.add_constant(sub[regressors].astype(float))
    else:
        X = pd.DataFrame(
            np.ones((len(sub), 1), dtype=float),
            columns=["const"],
            index=sub.index,
        )
    y = sub[outcome].astype(float)

    if family == "linear":
        fit = sm.OLS(y, X).fit()
    else:
        fit = sm.GLM(y, X, family=sm.families.Binomial()).fit()

    return float(fit.llf), int(X.shape[1])
