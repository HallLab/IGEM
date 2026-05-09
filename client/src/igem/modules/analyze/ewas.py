"""
Exposure-Wide Association Study (EWAS) — thin wrapper of
:func:`igem.modules.analyze.association_study`.

Kept as a separate function for ergonomic reasons:

- The name signals intent in pipelines ("``ewas(phen, outcome)``"
  reads better than the more general ``association_study``).
- It enforces single-outcome phenotype-only semantics, with the
  legacy result schema (``variable, n, beta, se, ci_low, ci_high,
  p_value``) so existing notebooks keep working unchanged.

For multi-outcome runs, list-of-regressors PheWAS, or genotype
inputs, use :func:`association_study` directly.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Iterable, Optional

import pandas as pd

from igem.modules.analyze.association import association_study
from igem.modules.analyze.results import RegressionResults
from igem.modules.data import Phenotypes


_LEGACY_EWAS_COLUMNS = [
    "variable", "n", "beta", "se", "ci_low", "ci_high", "p_value",
]


def ewas(
    phen: Phenotypes,
    outcome: str,
    *,
    exposures: Optional[Iterable[str]] = None,
    covariates: Optional[Iterable[str]] = None,
    family: Optional[str] = None,
    use_survey: bool = False,
    progress: bool = True,
) -> RegressionResults:
    """
    Run an exposure-wide association study against a single outcome.

    Wraps :func:`association_study` (single outcome, phenotype
    regressors, no genotype, no encoding, sequential by default) and
    re-exposes the legacy result columns for retrocompatibility.
    """
    # Pre-validate to preserve legacy error messages.
    exp_list = (
        list(exposures) if exposures is not None
        else list(phen.exposures)
    )
    if not exp_list:
        raise ValueError(
            "no exposures to test; pass exposures=[...] or set "
            "phen.exposures"
        )
    df_cols = list(phen.df.columns)
    missing = [e for e in exp_list if e not in df_cols]
    if missing:
        raise ValueError(
            f"exposures missing from phenotype dataframe: {missing}; "
            f"available: {df_cols}"
        )

    result = association_study(
        phen,
        outcome,
        regression_variables=exp_list,
        covariates=covariates,
        family=family,
        use_survey=use_survey,
        # Legacy ewas had no min_n filter — pin to 1 so only the
        # parameter-count feasibility check applies.
        min_n=1,
        progress=progress,
    )

    # Adapt the canonical schema to the legacy ewas schema.
    df_legacy = result.df.copy()
    if "beta_pvalue" in df_legacy.columns:
        df_legacy["p_value"] = df_legacy["beta_pvalue"]
    df_legacy = df_legacy[_LEGACY_EWAS_COLUMNS]

    # Strip the leading "outcome" column on errors_df (legacy ewas
    # only had ``[variable, error]``).
    err_df = result.errors
    if "outcome" in err_df.columns:
        err_df = err_df[["variable", "error"]].copy()

    formula = result.formula_template.replace("{variable}", "{exposure}")

    extras = dict(result.metadata)
    extras["call"] = "ewas"
    extras["n_exposures"] = len(exp_list)

    return replace(
        result,
        df=df_legacy,
        outcome=outcome,
        errors=err_df,
        formula_template=formula,
        metadata=extras,
    )
