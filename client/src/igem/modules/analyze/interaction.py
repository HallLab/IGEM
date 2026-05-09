"""
Pairwise interaction tests via likelihood-ratio.

For every pair of variables :math:`(v_1, v_2)` and each outcome,
fits two nested models and reports the LRT p-value:

- **Restricted** ``outcome ~ v1 + v2 + covariates``
- **Full**       ``outcome ~ v1 + v2 + v1:v2 + covariates``

Test statistic:

.. math::

    \\Lambda = -2 \\,(\\ell_R - \\ell_F) \\sim \\chi^2_{df}

with :math:`df` equal to the number of additional parameters in the
full model (1 for continuous × continuous; :math:`(k_1 - 1)(k_2 - 1)`
for categorical pairs of arity :math:`k_1, k_2`).

This is the IGEM core value-prop: efficient screening of GxG / GxE
interactions before multiple-testing correction. With biological
filtering from the knowledge graph, the universe of pairs is reduced
upstream so the LRT scan stays computationally tractable.
"""
from __future__ import annotations

import warnings
from itertools import combinations
from typing import Any, Callable, Iterable, Optional, Union

import numpy as np
import pandas as pd
from scipy import stats as _scipy_stats

from igem.modules.analyze._engines import (
    RegressionKind,
    resolve_engine,
)
from igem.modules.analyze._family import infer_family, validate_family
from igem.modules.analyze.results import (
    INTERACTION_RESULT_COLUMNS,
    RegressionResults,
    make_metadata,
)
from igem.modules.data import Phenotypes


InteractionsArg = Optional[
    Union[str, Iterable[tuple[str, str]]]
]


def interaction_study(
    phen: Phenotypes,
    outcomes: Union[str, Iterable[str]],
    interactions: InteractionsArg = None,
    *,
    covariates: Optional[Iterable[str]] = None,
    family: Optional[str] = None,
    regression_kind: RegressionKind = "auto",
    use_survey: bool = False,
    report_betas: bool = False,
    min_n: int = 200,
    max_pairs: int = 1000,
    n_jobs: int = 1,
    progress: bool = True,
) -> RegressionResults:
    """
    Run pairwise interaction tests over a phenotype frame.

    Parameters
    ----------
    phen :
        Phenotype wrapper supplying outcomes / covariates / exposures
        defaults plus the survey-design columns.
    outcomes :
        Outcome column name(s). Each is fitted independently against
        every interaction pair.
    interactions :
        Three accepted forms (CLARITE convention):

        - ``None`` (default): all unordered pairs of
          ``phen.exposures``. Capped at ``max_pairs``.
        - ``str X``: every pair ``(X, other)`` where ``other`` is
          another exposure.
        - list of tuples ``[(v1, v2), ...]``: explicit pair list.

    covariates :
        Variables included in *both* full and restricted models.
        Defaults to ``phen.covariates``.
    family :
        ``"linear"`` / ``"logistic"``. Auto-detected per outcome
        when ``None``.
    regression_kind :
        Engine selector — passed through to
        :func:`igem.modules.analyze._engines.resolve_engine`.
    use_survey :
        Apply NHANES-style sample weights from ``phen.weights_col``
        and cluster-robust SEs from ``phen.cluster_col``.
    report_betas :
        If ``True``, expand each interaction term into one row per
        regression coefficient (with ``term1`` / ``term2`` / ``beta``
        / ``se`` / ``beta_pvalue``). The summary LRT row is always
        emitted regardless.
    min_n :
        Skip pairs whose post-dropna sample count is below ``min_n``.
        Default 200 (CLARITE convention).
    max_pairs :
        Safety cap on the number of pairs when ``interactions=None``.
        Raises ``ValueError`` if the cap is exceeded — use an explicit
        list or pre-filter via the knowledge graph instead.
    n_jobs :
        Parallel workers (joblib). Default ``1`` is sequential.
    progress :
        tqdm progress bar.

    Returns
    -------
    RegressionResults
        Long-format result indexed by ``(outcome, term1, term2)``. The
        canonical interaction schema is::

            outcome, term1, term2, n, lrt_chi2, lrt_df, lrt_pvalue,
            diff_aic, converged, error

        With ``report_betas=True`` an extra ``term_beta`` /
        ``term_se`` / ``term_beta_pvalue`` block is emitted as
        additional rows tagged ``variable_type="interaction_dummy"``.
    """
    df = phen.df
    outcomes_list = [outcomes] if isinstance(outcomes, str) else list(outcomes)
    cov_list = (
        list(covariates) if covariates is not None else list(phen.covariates)
    )
    pairs = _resolve_pairs(
        interactions, phen=phen, max_pairs=max_pairs,
    )

    _validate_outcomes(df, outcomes_list)
    _validate_covariates(df, cov_list)
    _validate_pair_columns(df, pairs)

    if min_n < 1:
        raise ValueError(f"min_n must be ≥ 1; got {min_n}")
    if n_jobs == 0:
        raise ValueError("n_jobs must be non-zero")

    fam_per_outcome: dict[str, str] = {}
    for oc in outcomes_list:
        fam = family or infer_family(df[oc])
        validate_family(fam)
        fam_per_outcome[oc] = fam

    engine = resolve_engine(regression_kind, use_survey=use_survey)
    survey = _resolve_survey(phen, use_survey)

    tasks = [
        (outcome, pair) for outcome in outcomes_list for pair in pairs
    ]

    rows, errors, beta_rows = _run_interaction_tasks(
        tasks=tasks,
        df=df,
        engine=engine,
        cov_list=cov_list,
        fam_per_outcome=fam_per_outcome,
        survey=survey,
        min_n=min_n,
        report_betas=report_betas,
        n_jobs=n_jobs,
        progress=progress,
    )

    summary_df = pd.DataFrame(rows, columns=INTERACTION_RESULT_COLUMNS)
    if report_betas and beta_rows:
        beta_df = pd.DataFrame(beta_rows)
        # Mark them so a downstream filter can separate.
        beta_df["lrt_chi2"] = float("nan")
        beta_df["lrt_df"] = pd.NA
        beta_df["diff_aic"] = float("nan")
        beta_df["error"] = None
        # Order columns consistent with summary plus extras.
        result_df = pd.concat([summary_df, beta_df], ignore_index=True)
    else:
        result_df = summary_df

    errors_df = pd.DataFrame(
        errors, columns=["outcome", "term1", "term2", "error"]
    )

    formula = (
        "{outcome} ~ {term1} + {term2} + {term1}:{term2} + "
        + " + ".join(cov_list)
        if cov_list
        else "{outcome} ~ {term1} + {term2} + {term1}:{term2}"
    )

    extras: dict[str, Any] = {
        "call": "interaction_study",
        "n_outcomes": len(outcomes_list),
        "n_pairs": len(pairs),
        "regression_kind": (
            regression_kind if isinstance(regression_kind, str) else "<callable>"
        ),
        "report_betas": report_betas,
        "min_n": min_n,
        "n_jobs": n_jobs,
    }
    if survey.enabled:
        extras["survey"] = {
            "weights_col": survey.weights_col,
            "cluster_col": survey.cluster_col,
            "strata_col": survey.strata_col,
            "strata_used_for_variance": False,
        }

    return RegressionResults(
        df=result_df,
        family=(
            fam_per_outcome[outcomes_list[0]]
            if len(set(fam_per_outcome.values())) == 1
            else "mixed"
        ),
        outcome=(
            outcomes_list[0] if len(outcomes_list) == 1 else "(multiple)"
        ),
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
# Pair resolution
# ----------------------------------------------------------------------
def _resolve_pairs(
    interactions: InteractionsArg,
    *,
    phen: Phenotypes,
    max_pairs: int,
) -> list[tuple[str, str]]:
    if interactions is None:
        exposures = list(phen.exposures)
        if len(exposures) < 2:
            raise ValueError(
                "interaction_study with interactions=None requires at "
                "least 2 exposures on the Phenotypes wrapper"
            )
        pairs = [(a, b) for a, b in combinations(exposures, 2)]
    elif isinstance(interactions, str):
        anchor = interactions
        exposures = [e for e in phen.exposures if e != anchor]
        if not exposures:
            raise ValueError(
                f"interactions={anchor!r} requires phen.exposures to "
                f"contain at least one variable other than {anchor!r}"
            )
        pairs = [(anchor, other) for other in exposures]
    else:
        pairs = []
        for item in interactions:
            if not (isinstance(item, tuple) and len(item) == 2):
                raise ValueError(
                    f"interactions list must contain (var1, var2) tuples; "
                    f"got {item!r}"
                )
            pairs.append((str(item[0]), str(item[1])))

    if len(pairs) > max_pairs:
        raise ValueError(
            f"interaction pair count {len(pairs)} exceeds max_pairs="
            f"{max_pairs}; pre-filter with the knowledge graph or pass "
            f"an explicit list"
        )
    return pairs


def _validate_outcomes(df: pd.DataFrame, outcomes: list[str]) -> None:
    missing = [o for o in outcomes if o not in df.columns]
    if missing:
        raise ValueError(
            f"outcomes not in phenotype dataframe: {missing}"
        )


def _validate_covariates(df: pd.DataFrame, covariates: list[str]) -> None:
    missing = [c for c in covariates if c not in df.columns]
    if missing:
        raise ValueError(
            f"covariates not in phenotype dataframe: {missing}"
        )


def _validate_pair_columns(
    df: pd.DataFrame, pairs: list[tuple[str, str]]
) -> None:
    seen = set(c for pair in pairs for c in pair)
    missing = [c for c in seen if c not in df.columns]
    if missing:
        raise ValueError(
            f"interaction terms not in phenotype dataframe: {missing}"
        )


# ----------------------------------------------------------------------
# Survey opts (mirrors association.py)
# ----------------------------------------------------------------------
class _SurveyOpts:
    __slots__ = ("enabled", "weights_col", "cluster_col", "strata_col")

    def __init__(
        self, enabled: bool, weights_col, cluster_col, strata_col,
    ) -> None:
        self.enabled = enabled
        self.weights_col = weights_col
        self.cluster_col = cluster_col
        self.strata_col = strata_col


def _resolve_survey(phen: Phenotypes, use_survey: bool) -> _SurveyOpts:
    if not use_survey:
        return _SurveyOpts(False, None, None, None)
    if phen.weights_col is None:
        raise ValueError(
            "use_survey=True requires Phenotypes.weights_col to be set"
        )
    if phen.strata_col is not None:
        warnings.warn(
            "Phenotypes.strata_col is set but Taylor-series stratified "
            "variance is not implemented in glm_engine.",
            stacklevel=4,
        )
    return _SurveyOpts(
        True, phen.weights_col, phen.cluster_col, phen.strata_col,
    )


# ----------------------------------------------------------------------
# Task execution
# ----------------------------------------------------------------------
def _run_interaction_tasks(
    *,
    tasks: list[tuple[str, tuple[str, str]]],
    df: pd.DataFrame,
    engine: Callable,
    cov_list: list[str],
    fam_per_outcome: dict[str, str],
    survey: _SurveyOpts,
    min_n: int,
    report_betas: bool,
    n_jobs: int,
    progress: bool,
) -> tuple[list[dict], list[dict], list[dict]]:
    work_args = dict(
        df=df,
        engine=engine,
        cov_list=cov_list,
        fam_per_outcome=fam_per_outcome,
        survey=survey,
        min_n=min_n,
        report_betas=report_betas,
    )

    if n_jobs == 1:
        iterable: Iterable = tasks
        if progress:
            from tqdm.auto import tqdm
            iterable = tqdm(tasks, desc="interaction", unit="pair")
        outputs = [
            _fit_pair(outcome, pair, **work_args)
            for outcome, pair in iterable
        ]
    else:
        from joblib import Parallel, delayed

        outputs = Parallel(n_jobs=n_jobs, prefer="processes")(
            delayed(_fit_pair)(outcome, pair, **work_args)
            for outcome, pair in tasks
        )

    rows: list[dict] = []
    errors: list[dict] = []
    beta_rows: list[dict] = []
    for r, errs, br in outputs:
        rows.extend(r)
        errors.extend(errs)
        beta_rows.extend(br)
    return rows, errors, beta_rows


def _fit_pair(
    outcome: str,
    pair: tuple[str, str],
    *,
    df: pd.DataFrame,
    engine: Callable,
    cov_list: list[str],
    fam_per_outcome: dict[str, str],
    survey: _SurveyOpts,
    min_n: int,
    report_betas: bool,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Fit one (outcome × pair) interaction and emit summary + optional betas."""
    family = fam_per_outcome[outcome]
    v1, v2 = pair
    try:
        return _fit_pair_inner(
            outcome=outcome, v1=v1, v2=v2,
            df=df, engine=engine,
            cov_list=cov_list, family=family,
            survey=survey, min_n=min_n,
            report_betas=report_betas,
        )
    except Exception as exc:
        return [], [
            {
                "outcome": outcome,
                "term1": v1,
                "term2": v2,
                "error": _short_err(exc),
            }
        ], []


def _fit_pair_inner(
    *,
    outcome: str,
    v1: str,
    v2: str,
    df: pd.DataFrame,
    engine: Callable,
    cov_list: list[str],
    family: str,
    survey: _SurveyOpts,
    min_n: int,
    report_betas: bool,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Build full / restricted designs, fit, compute LRT, emit rows."""
    survey_cols = [c for c in (survey.weights_col, survey.cluster_col) if c]
    keep = [outcome, v1, v2, *cov_list, *survey_cols]
    sub = df.loc[:, keep].dropna()
    n = int(len(sub))
    if n < min_n:
        raise ValueError(
            f"insufficient samples: n={n} < min_n={min_n}"
        )

    # Encode each term (numeric → as-is; non-numeric → dummies, drop_first).
    v1_frame, v1_cols = _encode_term(sub[v1], v1)
    v2_frame, v2_cols = _encode_term(sub[v2], v2)

    # Interaction columns: pairwise products of v1 dummies × v2 dummies.
    inter_cols: list[str] = []
    inter_frame_data: dict[str, np.ndarray] = {}
    for c1 in v1_cols:
        for c2 in v2_cols:
            inter_name = f"{c1}_X_{c2}"
            inter_frame_data[inter_name] = (
                v1_frame[c1].to_numpy() * v2_frame[c2].to_numpy()
            )
            inter_cols.append(inter_name)
    inter_frame = pd.DataFrame(inter_frame_data, index=sub.index)

    cov_frame = (
        sub[cov_list].astype(float).copy() if cov_list else pd.DataFrame(
            index=sub.index,
        )
    )
    base = pd.concat([v1_frame, v2_frame, cov_frame], axis=1)
    base.insert(0, "const", 1.0)
    full = pd.concat([base, inter_frame], axis=1)

    if n < (full.shape[1] + 1):
        raise ValueError(
            f"insufficient samples for parameter count: n={n} for "
            f"{full.shape[1]} parameters"
        )

    y = sub[outcome].astype(float)
    weights = (
        sub[survey.weights_col].to_numpy(dtype=float)
        if survey.enabled and survey.weights_col else None
    )
    cluster = (
        sub[survey.cluster_col].to_numpy()
        if survey.enabled and survey.cluster_col else None
    )

    fit_full = engine(y, full.astype(float), family,
                      weights=weights, cluster=cluster)
    fit_restricted = engine(y, base.astype(float), family,
                            weights=weights, cluster=cluster)

    df_extra = len(inter_cols)
    chi2 = 2.0 * float(
        fit_full.log_likelihood - fit_restricted.log_likelihood
    )
    if not np.isfinite(chi2) or chi2 < 0 or df_extra <= 0:
        lrt_p = float("nan")
    else:
        lrt_p = float(_scipy_stats.chi2.sf(chi2, df_extra))
    diff_aic = float(fit_full.aic - fit_restricted.aic)

    summary = {
        "outcome": outcome,
        "term1": v1,
        "term2": v2,
        "n": n,
        "lrt_chi2": chi2,
        "lrt_df": df_extra,
        "lrt_pvalue": lrt_p,
        "diff_aic": diff_aic,
        "converged": bool(fit_full.converged),
        "error": None,
    }

    beta_rows: list[dict] = []
    if report_betas:
        for col in inter_cols:
            beta_rows.append(
                {
                    "outcome": outcome,
                    "term1": v1,
                    "term2": v2,
                    "n": n,
                    "term": col,
                    "term_beta": float(fit_full.params[col]),
                    "term_se": float(fit_full.bse[col]),
                    "term_beta_pvalue": float(fit_full.pvalues[col]),
                    "lrt_pvalue": lrt_p,
                    "converged": bool(fit_full.converged),
                }
            )

    return [summary], [], beta_rows


def _encode_term(s: pd.Series, name: str) -> tuple[pd.DataFrame, list[str]]:
    """
    Encode an interaction term: numeric → 1 column; non-numeric →
    dummies (drop_first=True). Booleans become ``int``.
    """
    if pd.api.types.is_bool_dtype(s):
        out = s.astype(int).to_frame(name=name).astype(float)
        return out, [name]
    if pd.api.types.is_numeric_dtype(s):
        return s.to_frame(name=name).astype(float), [name]
    dummies = pd.get_dummies(s, prefix=name, drop_first=True, dummy_na=False)
    if dummies.empty:
        raise ValueError(
            f"term {name!r} produced no non-reference dummy levels"
        )
    return dummies.astype(float), list(dummies.columns)


def _short_err(exc: BaseException) -> str:
    msg = str(exc).strip().splitlines()
    return msg[0][:200] if msg else type(exc).__name__
