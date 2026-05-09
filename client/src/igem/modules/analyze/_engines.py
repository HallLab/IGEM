"""
Regression engines used by ``association_study`` and
``interaction_study``.

An *engine* is a callable that fits a single regression model and
returns a standardised :class:`EngineResult`. The engines wrap
statsmodels (``glm_engine``) and optionally R's ``survey`` package
(``r_survey_engine``, requires ``rpy2``).

Custom backends can be plugged in via ``regression_kind=Callable`` —
they only need to honour the contract of returning an
:class:`EngineResult` with the documented fields populated.

Names ``glm`` / ``weighted_glm`` / ``r_survey`` mirror the CLARITE
``regression_kind`` aliases for cross-codebase familiarity, but the
underlying implementation is unified: ``glm_engine`` covers both
``glm`` and ``weighted_glm`` via the optional ``weights`` argument.
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional, Union

import numpy as np
import pandas as pd


EngineCallable = Callable[..., "EngineResult"]
EngineKind = Literal["auto", "glm", "weighted_glm", "r_survey"]
RegressionKind = Union[EngineKind, EngineCallable]


@dataclass
class EngineResult:
    """
    Standardised output of any regression engine.

    Engines must populate the model-wide statistics (``params``,
    ``bse``, ``pvalues``, ``conf_int``, ``log_likelihood``, ``aic``,
    ``n``, ``converged``). The ``_backend`` slot holds the underlying
    fit object (statsmodels result, R object, etc.) for downstream
    LRT computation; ``None`` when the engine does not support LRT
    via this object.
    """

    params: pd.Series
    bse: pd.Series
    pvalues: pd.Series
    conf_int: pd.DataFrame
    log_likelihood: float
    aic: float
    n: int
    converged: bool
    _backend: Any = None


# ----------------------------------------------------------------------
# GLM engine — covers "glm" and "weighted_glm"
# ----------------------------------------------------------------------
def glm_engine(
    y: pd.Series,
    X: pd.DataFrame,
    family: str,
    *,
    weights: Optional[np.ndarray] = None,
    cluster: Optional[np.ndarray] = None,
) -> EngineResult:
    """
    Fit a Generalised Linear Model via statsmodels.

    - ``family="linear"`` → Gaussian GLM (equivalent to OLS / WLS).
    - ``family="logistic"`` → Binomial GLM with logit link.

    When ``weights`` is provided, uses ``freq_weights`` for survey-
    style weighting. When ``cluster`` is provided, applies
    cluster-robust covariance via ``get_robustcov_results``.
    """
    import statsmodels.api as sm

    if family == "linear":
        fam_obj = sm.families.Gaussian()
    elif family == "logistic":
        fam_obj = sm.families.Binomial()
    else:
        raise ValueError(
            f"family must be 'linear' or 'logistic'; got {family!r}"
        )

    glm_kwargs: dict = {"family": fam_obj}
    if weights is not None:
        glm_kwargs["freq_weights"] = np.asarray(weights, dtype=float)

    fit_kwargs: dict = {}
    if cluster is not None:
        fit_kwargs["cov_type"] = "cluster"
        fit_kwargs["cov_kwds"] = {"groups": np.asarray(cluster)}

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = sm.GLM(y, X, **glm_kwargs)
        fit = model.fit(**fit_kwargs)

    params = pd.Series(np.asarray(fit.params), index=X.columns)
    bse = pd.Series(np.asarray(fit.bse), index=X.columns)
    pvalues = pd.Series(np.asarray(fit.pvalues), index=X.columns)
    ci_arr = np.asarray(fit.conf_int())
    conf_int = pd.DataFrame(
        ci_arr, index=X.columns, columns=["lo", "hi"],
    )
    return EngineResult(
        params=params,
        bse=bse,
        pvalues=pvalues,
        conf_int=conf_int,
        log_likelihood=float(getattr(fit, "llf", np.nan)),
        aic=float(getattr(fit, "aic", np.nan)),
        n=int(getattr(fit, "nobs", len(y))),
        converged=bool(getattr(fit, "converged", True)),
        _backend=fit,
    )


# ----------------------------------------------------------------------
# R-Survey engine — optional, requires rpy2
# ----------------------------------------------------------------------
def r_survey_engine(
    y: pd.Series,
    X: pd.DataFrame,
    family: str,
    *,
    weights: np.ndarray,
    cluster: Optional[np.ndarray] = None,
    strata: Optional[np.ndarray] = None,
) -> EngineResult:
    """
    Survey-weighted regression via R's ``survey`` package (gold
    standard for NHANES-style complex sample designs).

    Requires the optional dependency ``rpy2`` and a working R install
    with the ``survey`` package available. Install with::

        poetry install --with r-survey

    The Taylor-series stratified variance produced by R's ``survey``
    differs from statsmodels' robust SE on stratified designs; for
    NHANES production use this engine when ``phen.strata_col`` is set.
    """
    try:
        import rpy2.robjects as ro                       # noqa: F401
        from rpy2.robjects import pandas2ri              # noqa: F401
        from rpy2.robjects.packages import importr       # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "regression_kind='r_survey' requires the 'rpy2' optional "
            "dependency. Install with: poetry install --with r-survey"
        ) from exc

    raise NotImplementedError(
        "r_survey_engine implementation is pending in this phase. "
        "Use regression_kind='weighted_glm' as a near-equivalent for "
        "non-stratified designs, or contribute the rpy2 binding."
    )


# ----------------------------------------------------------------------
# regression_kind resolver
# ----------------------------------------------------------------------
_BUILTIN_ENGINES: dict[str, EngineCallable] = {
    "glm": glm_engine,
    "weighted_glm": glm_engine,   # same callable, different invocation
    "r_survey": r_survey_engine,
}


def resolve_engine(
    regression_kind: RegressionKind = "auto",
    *,
    use_survey: bool = False,
) -> EngineCallable:
    """
    Resolve ``regression_kind`` (string alias or callable) to the
    actual engine callable.

    - ``"auto"`` (default): ``glm_engine``. Pass ``weights`` /
      ``cluster`` at call site when ``use_survey=True``.
    - ``"glm"`` / ``"weighted_glm"`` / ``"r_survey"``: the named
      engine. ``weighted_glm`` is identical to ``glm`` at the engine
      level — the difference is whether the caller supplies
      ``weights``.
    - Callable: returned as-is. Must satisfy the engine contract
      (return :class:`EngineResult`).
    """
    if callable(regression_kind):
        return regression_kind
    if regression_kind == "auto":
        return glm_engine
    if regression_kind in _BUILTIN_ENGINES:
        return _BUILTIN_ENGINES[regression_kind]
    raise ValueError(
        f"regression_kind must be one of {list(_BUILTIN_ENGINES) + ['auto']} "
        f"or a callable; got {regression_kind!r}"
    )
