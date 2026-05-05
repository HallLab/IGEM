"""Exposure-Wide Association Study (EWAS).

For each *exposure* variable (a column of the phenotype frame), fit a
regression of the form::

    outcome ~ exposure + covariates

independently and collect the ``exposure`` coefficient, its standard
error, 95% confidence interval and p-value into a single
:class:`RegressionResults`.

Supports linear and logistic families (auto-detected from the outcome
dtype unless explicitly provided). Categorical exposures are not
handled in v1 — encode them with ``igem.modify.recode`` /
``igem.modify.categorize`` first.

Survey-aware mode (``use_survey=True``) uses the survey columns
recorded on the :class:`Phenotypes` wrapper:

  - ``weights_col`` → switches the fit to WLS (linear) or GLM with
    ``freq_weights`` (logistic). Required when ``use_survey=True``.
  - ``cluster_col`` → cluster-robust standard errors via statsmodels
    ``cov_type="cluster"``. Optional.
  - ``strata_col`` → recorded but not used for variance estimation in
    this phase. Full Taylor-series stratified survey variance is a
    Phase-3 enhancement (likely via the ``samplics`` package).
"""
from __future__ import annotations

import warnings
from typing import Iterable, Optional

import numpy as np
import pandas as pd

from igem.modules.analyze._family import infer_family, validate_family
from igem.modules.analyze.results import RegressionResults, make_metadata
from igem.modules.data import Phenotypes


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
    Run an exposure-wide association study.

    Parameters
    ----------
    phen :
        Phenotype wrapper. ``phen.exposures`` and ``phen.covariates``
        provide the defaults when ``exposures`` / ``covariates`` are
        omitted.
    outcome :
        Column name in ``phen.df`` to use as the regression outcome.
    exposures :
        Variables to test, one regression per element. Defaults to
        ``phen.exposures``. Must be numeric.
    covariates :
        Variables included in every model alongside the exposure under
        test. Defaults to ``phen.covariates``.
    family :
        ``"linear"`` or ``"logistic"``. Auto-detected from ``outcome``
        when ``None``: numeric → linear, binary 0/1 → logistic.
    use_survey :
        Apply NHANES-style sample weights from ``phen.weights_col``
        (required when True) and cluster-robust standard errors when
        ``phen.cluster_col`` is set.
    progress :
        Show a tqdm progress bar (default ``True``). Disable for
        non-interactive runs.

    Returns
    -------
    RegressionResults
        Per-exposure rows: ``variable, n, beta, se, ci_low, ci_high,
        p_value``. Failed regressions are recorded on
        ``result.errors`` instead of crashing the loop.
    """
    df = phen.df

    if outcome not in df.columns:
        raise ValueError(
            f"outcome {outcome!r} not in phenotype dataframe; "
            f"columns: {list(df.columns)}"
        )

    cov_list = (
        list(covariates) if covariates is not None else list(phen.covariates)
    )
    exp_list = (
        list(exposures) if exposures is not None else list(phen.exposures)
    )
    if not exp_list:
        raise ValueError(
            "no exposures to test; pass exposures=[...] or set "
            "phen.exposures"
        )

    survey = _resolve_survey_options(phen, use_survey)
    extra_cols = [c for c in (survey.weights_col, survey.cluster_col) if c]
    _validate_columns(df, [outcome, *exp_list, *cov_list, *extra_cols])

    fam = family or infer_family(df[outcome])
    validate_family(fam)

    rows, errors = _run_loop(
        df=df, outcome=outcome,
        exposures=exp_list, covariates=cov_list,
        family=fam, survey=survey, progress=progress,
    )

    result_df = pd.DataFrame(
        rows,
        columns=[
            "variable", "n", "beta", "se",
            "ci_low", "ci_high", "p_value",
        ],
    )
    errors_df = pd.DataFrame(errors, columns=["variable", "error"])

    formula = (
        f"{outcome} ~ {{exposure}} + " + " + ".join(cov_list)
        if cov_list
        else f"{outcome} ~ {{exposure}}"
    )

    extras: dict = {"call": "ewas", "n_exposures": len(exp_list)}
    if survey.enabled:
        extras["survey"] = {
            "weights_col": survey.weights_col,
            "cluster_col": survey.cluster_col,
            "strata_col": survey.strata_col,
            "strata_used_for_variance": False,
        }

    return RegressionResults(
        df=result_df,
        family=fam,
        outcome=outcome,
        covariates=cov_list,
        formula_template=formula,
        errors=errors_df,
        metadata=make_metadata(
            n_samples=int(df.shape[0]),
            n_dropped=0,
            extras=extras,
        ),
    )


# ----------------------------------------------------------------------
# internal — survey options
# ----------------------------------------------------------------------
class _SurveyOpts:
    __slots__ = (
        "enabled", "weights_col", "cluster_col", "strata_col",
    )

    def __init__(
        self,
        enabled: bool,
        weights_col: Optional[str],
        cluster_col: Optional[str],
        strata_col: Optional[str],
    ) -> None:
        self.enabled = enabled
        self.weights_col = weights_col
        self.cluster_col = cluster_col
        self.strata_col = strata_col


def _resolve_survey_options(
    phen: Phenotypes,
    use_survey: bool,
) -> _SurveyOpts:
    if not use_survey:
        return _SurveyOpts(False, None, None, None)
    if phen.weights_col is None:
        raise ValueError(
            "use_survey=True requires Phenotypes.weights_col to be set"
        )
    if phen.strata_col is not None:
        warnings.warn(
            "Phenotypes.strata_col is set but stratified variance is "
            "not implemented in this phase — the strata column is "
            "recorded in metadata but ignored for SE computation.",
            stacklevel=3,
        )
    return _SurveyOpts(
        enabled=True,
        weights_col=phen.weights_col,
        cluster_col=phen.cluster_col,
        strata_col=phen.strata_col,
    )


# ----------------------------------------------------------------------
# internal — fitting loop
# ----------------------------------------------------------------------
def _validate_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"columns missing from phenotype dataframe: {missing}; "
            f"available: {list(df.columns)}"
        )


def _run_loop(
    *,
    df: pd.DataFrame,
    outcome: str,
    exposures: list[str],
    covariates: list[str],
    family: str,
    survey: _SurveyOpts,
    progress: bool,
) -> tuple[list[dict], list[dict]]:
    rows: list[dict] = []
    errors: list[dict] = []

    iterable: Iterable[str]
    if progress:
        from tqdm.auto import tqdm
        iterable = tqdm(exposures, desc="ewas", unit="var")
    else:
        iterable = exposures

    for exp in iterable:
        try:
            row = _fit_single(
                df=df, outcome=outcome, exposure=exp,
                covariates=covariates, family=family, survey=survey,
            )
            rows.append(row)
        except Exception as exc:
            errors.append({"variable": exp, "error": _short_err(exc)})
    return rows, errors


def _fit_single(
    *,
    df: pd.DataFrame,
    outcome: str,
    exposure: str,
    covariates: list[str],
    family: str,
    survey: _SurveyOpts,
) -> dict:
    import statsmodels.api as sm

    extra = [c for c in (survey.weights_col, survey.cluster_col) if c]
    cols = [outcome, exposure, *covariates, *extra]
    sub = df.loc[:, cols].dropna()
    n = len(sub)
    if n < (len(covariates) + 3):
        raise ValueError(
            f"insufficient samples after dropna: n={n} for "
            f"{len(covariates) + 1} covariates + intercept"
        )

    if not pd.api.types.is_numeric_dtype(sub[exposure]):
        raise ValueError(
            f"exposure {exposure!r} is not numeric "
            f"(dtype={sub[exposure].dtype}); recode first"
        )

    X = sm.add_constant(sub[[exposure, *covariates]].astype(float))
    y = sub[outcome].astype(float)

    fit_kwargs: dict = {}
    if survey.enabled and survey.cluster_col:
        fit_kwargs["cov_type"] = "cluster"
        fit_kwargs["cov_kwds"] = {
            "groups": sub[survey.cluster_col].to_numpy(),
        }

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if family == "linear":
            if survey.enabled:
                weights = sub[survey.weights_col].to_numpy(dtype=float)
                fit = sm.WLS(y, X, weights=weights).fit(**fit_kwargs)
            else:
                fit = sm.OLS(y, X).fit(**fit_kwargs)
        else:  # logistic
            if survey.enabled:
                weights = sub[survey.weights_col].to_numpy(dtype=float)
                fit = sm.GLM(
                    y, X,
                    family=sm.families.Binomial(),
                    freq_weights=weights,
                ).fit()
                if survey.cluster_col:
                    fit = fit.get_robustcov_results(
                        cov_type="cluster",
                        groups=sub[survey.cluster_col].to_numpy(),
                    )
            else:
                fit = sm.GLM(
                    y, X, family=sm.families.Binomial()
                ).fit()

    beta = float(fit.params[exposure])
    se = float(fit.bse[exposure])
    pvalue = float(fit.pvalues[exposure])
    ci = fit.conf_int().loc[exposure]
    ci_low = float(ci.iloc[0])
    ci_high = float(ci.iloc[1])

    if not np.isfinite(beta) or not np.isfinite(pvalue):
        raise ValueError(
            f"non-finite estimate for {exposure!r}: beta={beta}, "
            f"p={pvalue}"
        )

    return {
        "variable": exposure,
        "n": n,
        "beta": beta,
        "se": se,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "p_value": pvalue,
    }


def _short_err(exc: BaseException) -> str:
    msg = str(exc).strip().splitlines()
    return msg[0][:200] if msg else type(exc).__name__
