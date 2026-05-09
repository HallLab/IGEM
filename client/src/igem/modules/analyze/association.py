"""
Unified association study — EWAS / PheWAS / GWAS in one entrypoint.

Accepts lists of outcomes and/or regression variables, dispatches to
the appropriate regression engine (statsmodels GLM by default), and
returns a long-format :class:`RegressionResults` indexed by
``(outcome, variable)``. Each row reports Wald and LRT p-values,
ΔAIC, convergence flag and per-row error string.

Designed to mirror :func:`clarite.modules.analyze.association_study`
while preserving the IGEM-specific extras (chainable
``RegressionResults``, knowledge-graph annotation, sgkit fast-path
for additive-linear GWAS, optional R-survey backend).
"""
from __future__ import annotations

import warnings
from typing import Any, Callable, Iterable, Optional, Union

import numpy as np
import pandas as pd
from scipy import stats as _scipy_stats

from igem.modules.analyze._encoding import EncodingName, encode
from igem.modules.analyze._engines import (
    RegressionKind,
    glm_engine,
    resolve_engine,
)
from igem.modules.analyze._family import infer_family, validate_family
from igem.modules.analyze.results import (
    ASSOCIATION_RESULT_COLUMNS,
    RegressionResults,
    make_metadata,
)
from igem.modules.data import Genotypes, Phenotypes


# ----------------------------------------------------------------------
# Public entrypoint
# ----------------------------------------------------------------------
def association_study(
    phen: Phenotypes,
    outcomes: Union[str, Iterable[str]],
    regression_variables: Optional[Union[str, Iterable[str]]] = None,
    *,
    geno: Optional[Genotypes] = None,
    covariates: Optional[Iterable[str]] = None,
    family: Optional[str] = None,
    regression_kind: RegressionKind = "auto",
    encoding: EncodingName = "additive",
    edge_encoding_info: Optional[pd.DataFrame] = None,
    use_survey: bool = False,
    min_n: int = 200,
    n_jobs: int = 1,
    standardize_data: bool = False,
    report_categorical_betas: bool = False,
    progress: bool = True,
) -> RegressionResults:
    """
    Run an association study over one or more outcomes and one or
    more regression variables.

    Parameters
    ----------
    phen :
        Phenotype wrapper. Provides ``outcomes`` / ``covariates`` /
        ``exposures`` defaults and survey-design columns.
    outcomes :
        Outcome column name (str) or list of names. Each is fitted
        independently against every regression variable.
    regression_variables :
        Variables to test. Defaults: when ``geno=None`` uses
        ``phen.exposures``; when ``geno`` is supplied uses every
        ``variant_id`` on the genotype dataset.
    geno :
        Optional :class:`Genotypes`. When supplied, ``regression_variables``
        must be ``variant_id`` strings; the dosage is encoded per
        ``encoding=`` and fed as a single regressor (or 2 columns for
        ``codominant``).
    covariates :
        Variables included in every model. Defaults to ``phen.covariates``.
    family :
        ``"linear"`` / ``"logistic"``. Auto-detected per outcome when
        ``None``.
    regression_kind :
        ``"auto"`` (default) → :func:`igem.modules.analyze._engines.glm_engine`.
        Explicit aliases: ``"glm"``, ``"weighted_glm"``, ``"r_survey"``.
        A custom callable is also accepted (must return
        :class:`igem.modules.analyze._engines.EngineResult`).
    encoding :
        Genotype encoding when ``geno`` is supplied — ``"additive"``,
        ``"dominant"``, ``"recessive"``, ``"codominant"``, or
        ``"edge"``. No-op for phenotype-only runs.
    edge_encoding_info :
        Required when ``encoding="edge"`` — DataFrame indexed by
        ``variant_id`` with columns ``score_0`` / ``score_1`` /
        ``score_2``.
    use_survey :
        Apply NHANES-style sample weights (and cluster-robust SEs)
        from ``phen.weights_col`` / ``phen.cluster_col``.
    min_n :
        Skip regressions with fewer than ``min_n`` complete cases.
        Default ``200`` matches CLARITE convention.
    n_jobs :
        Parallel workers via joblib. ``1`` (default) is sequential;
        ``-1`` uses every available core.
    standardize_data :
        Z-score continuous regressors and continuous covariates before
        the fit (does **not** standardise the outcome). Useful for
        comparing effect magnitudes across features of different
        scales.
    report_categorical_betas :
        For categorical / codominant-genotype regressors, emit one
        row per dummy contrast (with ``category`` field) instead of
        a single LRT-only row.
    progress :
        Show a tqdm progress bar.

    Returns
    -------
    RegressionResults
        DataFrame columns: ``outcome, variable, variable_type, n,
        beta, se, ci_low, ci_high, beta_pvalue, lrt_pvalue, diff_aic,
        converged, error``. Failed regressions are recorded in
        ``result.errors``.
    """
    outcomes_list = [outcomes] if isinstance(outcomes, str) else list(outcomes)
    cov_list = (
        list(covariates) if covariates is not None else list(phen.covariates)
    )
    regressors_list = _resolve_regressors(
        regression_variables, phen=phen, geno=geno,
    )

    df = phen.df
    _validate_outcomes(df, outcomes_list)
    _validate_covariates(df, cov_list)
    if geno is None:
        _validate_phen_regressors(df, regressors_list)

    if min_n < 1:
        raise ValueError(f"min_n must be ≥ 1; got {min_n}")
    if n_jobs == 0:
        raise ValueError("n_jobs must be non-zero (1 sequential, -1 all cores)")

    fam_per_outcome: dict[str, str] = {}
    for oc in outcomes_list:
        fam = family or infer_family(df[oc])
        validate_family(fam)
        fam_per_outcome[oc] = fam

    engine = resolve_engine(regression_kind, use_survey=use_survey)
    survey = _resolve_survey(phen, use_survey)

    encoded_frame: Optional[pd.DataFrame]
    if geno is not None:
        encoded_frame = _build_encoded_genotype(
            geno=geno,
            phen=phen,
            variant_ids=regressors_list,
            encoding=encoding,
            edge_encoding_info=edge_encoding_info,
        )
    else:
        encoded_frame = None

    tasks = [
        (outcome, regressor)
        for outcome in outcomes_list
        for regressor in regressors_list
    ]
    rows, errors = _run_tasks(
        tasks=tasks,
        df=df,
        encoded_frame=encoded_frame,
        engine=engine,
        cov_list=cov_list,
        fam_per_outcome=fam_per_outcome,
        survey=survey,
        encoding=encoding,
        min_n=min_n,
        standardize_data=standardize_data,
        report_categorical_betas=report_categorical_betas,
        is_genotype=(geno is not None),
        n_jobs=n_jobs,
        progress=progress,
    )

    result_df = pd.DataFrame(rows, columns=ASSOCIATION_RESULT_COLUMNS)
    errors_df = pd.DataFrame(errors, columns=["outcome", "variable", "error"])

    formula = (
        "{outcome} ~ {variable} + " + " + ".join(cov_list)
        if cov_list
        else "{outcome} ~ {variable}"
    )

    extras: dict = {
        "call": "association_study",
        "n_outcomes": len(outcomes_list),
        "n_regressors": len(regressors_list),
        "regression_kind": (
            regression_kind if isinstance(regression_kind, str)
            else "<callable>"
        ),
        "encoding": encoding if geno is not None else None,
        "n_jobs": n_jobs,
        "standardize_data": standardize_data,
        "min_n": min_n,
    }
    if survey.enabled:
        extras["survey"] = {
            "weights_col": survey.weights_col,
            "cluster_col": survey.cluster_col,
            "strata_col": survey.strata_col,
            # glm_engine doesn't compute Taylor-series stratified
            # variance — that requires regression_kind="r_survey".
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
# Input resolution / validation
# ----------------------------------------------------------------------
def _resolve_regressors(
    regression_variables: Optional[Union[str, Iterable[str]]],
    *,
    phen: Phenotypes,
    geno: Optional[Genotypes],
) -> list[str]:
    if regression_variables is None:
        if geno is not None:
            return list(geno.variants["variant_id"].astype(str))
        return list(phen.exposures)
    if isinstance(regression_variables, str):
        return [regression_variables]
    return list(regression_variables)


def _validate_outcomes(df: pd.DataFrame, outcomes: list[str]) -> None:
    missing = [o for o in outcomes if o not in df.columns]
    if missing:
        raise ValueError(
            f"outcomes not in phenotype dataframe: {missing}; "
            f"available: {list(df.columns)}"
        )


def _validate_covariates(df: pd.DataFrame, covariates: list[str]) -> None:
    missing = [c for c in covariates if c not in df.columns]
    if missing:
        raise ValueError(
            f"covariates not in phenotype dataframe: {missing}"
        )


def _validate_phen_regressors(df: pd.DataFrame, regressors: list[str]) -> None:
    if not regressors:
        raise ValueError(
            "no regression_variables to test; pass a list explicitly or "
            "set phen.exposures"
        )
    missing = [r for r in regressors if r not in df.columns]
    if missing:
        raise ValueError(
            f"regression_variables not in phenotype dataframe: "
            f"{missing[:5]}{'...' if len(missing) > 5 else ''}"
        )


# ----------------------------------------------------------------------
# Survey options (shared with legacy ewas)
# ----------------------------------------------------------------------
class _SurveyOpts:
    __slots__ = ("enabled", "weights_col", "cluster_col", "strata_col")

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
            "variance is not implemented in glm_engine. Use "
            "regression_kind='r_survey' for production NHANES analysis.",
            stacklevel=4,
        )
    return _SurveyOpts(
        enabled=True,
        weights_col=phen.weights_col,
        cluster_col=phen.cluster_col,
        strata_col=phen.strata_col,
    )


# ----------------------------------------------------------------------
# Genotype frame assembly
# ----------------------------------------------------------------------
def _build_encoded_genotype(
    *,
    geno: Genotypes,
    phen: Phenotypes,
    variant_ids: list[str],
    encoding: EncodingName,
    edge_encoding_info: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Build a (samples × variants) dosage frame from the Genotypes,
    align it to the phenotype sample IDs, and apply the requested
    encoding. Returns a frame with phenotype sample-IDs as index.
    """
    geno_samples = (
        np.asarray(geno.ds["sample_id"].values).astype(str).tolist()
    )
    phen_samples = phen.df[phen.sample_id_col].astype(str).tolist()

    common = [s for s in geno_samples if s in set(phen_samples)]
    if not common:
        raise ValueError(
            "no overlapping samples between geno and phen"
        )
    geno_sub = geno.select(samples=common, variants=list(variant_ids))

    cg = np.asarray(geno_sub.ds["call_genotype"].values, dtype=np.int16)
    valid = cg >= 0
    alt_count = np.where(valid, (cg > 0).astype(np.int16), 0).sum(axis=-1)
    all_missing = (~valid).all(axis=-1)
    dosage_arr = np.where(all_missing, -1, alt_count)
    # dosage_arr shape: (variants, samples) → transpose to (samples, variants)
    dosage = pd.DataFrame(
        dosage_arr.T,
        index=common,
        columns=list(variant_ids),
        dtype=int,
    )
    # Align to phenotype sample-id order.
    dosage = dosage.reindex(phen_samples)
    return encode(
        dosage, method=encoding, edge_encoding_info=edge_encoding_info,
    )


# ----------------------------------------------------------------------
# Task execution loop
# ----------------------------------------------------------------------
def _run_tasks(
    *,
    tasks: list[tuple[str, str]],
    df: pd.DataFrame,
    encoded_frame: Optional[pd.DataFrame],
    engine: Callable,
    cov_list: list[str],
    fam_per_outcome: dict[str, str],
    survey: _SurveyOpts,
    encoding: str,
    min_n: int,
    standardize_data: bool,
    report_categorical_betas: bool,
    is_genotype: bool,
    n_jobs: int,
    progress: bool,
) -> tuple[list[dict], list[dict]]:
    work_args = dict(
        df=df,
        encoded_frame=encoded_frame,
        engine=engine,
        cov_list=cov_list,
        fam_per_outcome=fam_per_outcome,
        survey=survey,
        encoding=encoding,
        min_n=min_n,
        standardize_data=standardize_data,
        report_categorical_betas=report_categorical_betas,
        is_genotype=is_genotype,
    )

    if n_jobs == 1:
        iterable: Iterable = tasks
        if progress:
            from tqdm.auto import tqdm
            iterable = tqdm(tasks, desc="association", unit="fit")
        outputs = [
            _fit_task(outcome, regressor, **work_args)
            for outcome, regressor in iterable
        ]
    else:
        from joblib import Parallel, delayed

        outputs = Parallel(n_jobs=n_jobs, prefer="processes")(
            delayed(_fit_task)(outcome, regressor, **work_args)
            for outcome, regressor in tasks
        )

    rows: list[dict] = []
    errors: list[dict] = []
    for produced_rows, produced_errors in outputs:
        rows.extend(produced_rows)
        errors.extend(produced_errors)
    return rows, errors


def _fit_task(
    outcome: str,
    regressor: str,
    *,
    df: pd.DataFrame,
    encoded_frame: Optional[pd.DataFrame],
    engine: Callable,
    cov_list: list[str],
    fam_per_outcome: dict[str, str],
    survey: _SurveyOpts,
    encoding: str,
    min_n: int,
    standardize_data: bool,
    report_categorical_betas: bool,
    is_genotype: bool,
) -> tuple[list[dict], list[dict]]:
    """Fit a single (outcome × regressor) and emit row(s) + error(s)."""
    family = fam_per_outcome[outcome]
    try:
        return _fit_one(
            outcome=outcome,
            regressor=regressor,
            df=df,
            encoded_frame=encoded_frame,
            engine=engine,
            cov_list=cov_list,
            family=family,
            survey=survey,
            min_n=min_n,
            standardize_data=standardize_data,
            report_categorical_betas=report_categorical_betas,
            is_genotype=is_genotype,
        )
    except Exception as exc:
        return [], [
            {
                "outcome": outcome,
                "variable": regressor,
                "error": _short_err(exc),
            }
        ]


def _fit_one(
    *,
    outcome: str,
    regressor: str,
    df: pd.DataFrame,
    encoded_frame: Optional[pd.DataFrame],
    engine: Callable,
    cov_list: list[str],
    family: str,
    survey: _SurveyOpts,
    min_n: int,
    standardize_data: bool,
    report_categorical_betas: bool,
    is_genotype: bool,
) -> tuple[list[dict], list[dict]]:
    """Inner fit. Builds X, y, runs engine, computes LRT/AIC vs null."""
    # Assemble regressor columns (1 for additive/dominant/recessive/edge,
    # 2 for codominant).
    if is_genotype:
        if regressor in encoded_frame.columns:
            reg_cols = [regressor]
        else:
            # codominant emits "<v>_het" / "<v>_hom_alt" pairs.
            reg_cols = [
                c for c in encoded_frame.columns
                if c.startswith(f"{regressor}_")
            ]
            if not reg_cols:
                raise ValueError(
                    f"regressor {regressor!r} not found in encoded frame"
                )
        # Position-based alignment: encoded_frame is built in phen.df
        # sample order in :func:`_build_encoded_genotype`.
        reg_frame = encoded_frame[reg_cols].reset_index(drop=True)
        reg_frame.index = df.index
        variable_type = "genotype"
    else:
        # Phenotype regressor: pull from df.
        s = df[regressor]
        if pd.api.types.is_numeric_dtype(s) and not pd.api.types.is_bool_dtype(s):
            # binary numeric vs continuous: use n_unique
            n_unique = int(s.nunique(dropna=True))
            variable_type = "binary" if n_unique == 2 else "continuous"
            reg_frame = s.to_frame(name=regressor)
            reg_cols = [regressor]
        elif pd.api.types.is_bool_dtype(s):
            variable_type = "binary"
            reg_frame = s.astype(int).to_frame(name=regressor)
            reg_cols = [regressor]
        else:
            # Categorical: dummy-encode (drop first level).
            dummies = pd.get_dummies(
                s, prefix=regressor, drop_first=True, dummy_na=False,
            )
            if dummies.empty:
                raise ValueError(
                    f"categorical regressor {regressor!r} produced no "
                    f"non-reference levels"
                )
            variable_type = "categorical"
            reg_frame = dummies.astype(float)
            reg_cols = list(dummies.columns)

    # Survey columns to keep around for fit.
    survey_cols = [c for c in (survey.weights_col, survey.cluster_col) if c]

    # Assemble combined frame, drop NaN-bearing rows.
    keep_cols = [outcome, *cov_list, *survey_cols]
    base = df.loc[:, keep_cols].copy()
    full = pd.concat([base, reg_frame], axis=1)
    full = full.dropna()
    n = int(len(full))
    if n < min_n:
        raise ValueError(
            f"insufficient samples: n={n} < min_n={min_n}"
        )
    if n < (len(cov_list) + len(reg_cols) + 2):
        raise ValueError(
            f"insufficient samples for parameter count: "
            f"n={n} for {len(cov_list) + len(reg_cols) + 1} parameters"
        )

    y = full[outcome].astype(float)
    Xfull = _design_matrix(
        full=full,
        regressor_cols=reg_cols,
        cov_list=cov_list,
        standardize_data=standardize_data,
        family=family,
    )
    Xnull = _design_matrix(
        full=full,
        regressor_cols=[],
        cov_list=cov_list,
        standardize_data=standardize_data,
        family=family,
    )

    weights = (
        full[survey.weights_col].to_numpy(dtype=float)
        if survey.enabled and survey.weights_col else None
    )
    cluster = (
        full[survey.cluster_col].to_numpy()
        if survey.enabled and survey.cluster_col else None
    )

    fit_full = engine(
        y, Xfull, family,
        weights=weights, cluster=cluster,
    )
    fit_null = engine(
        y, Xnull, family,
        weights=weights, cluster=cluster,
    )

    lrt_pvalue, lrt_chi2 = _lrt_against_null(fit_full, fit_null, len(reg_cols))
    diff_aic = float(fit_full.aic - fit_null.aic)

    # Emit rows.
    rows: list[dict] = []
    if len(reg_cols) == 1:
        col = reg_cols[0]
        rows.append(
            _row_for_single_term(
                outcome=outcome,
                variable=regressor,
                variable_type=variable_type,
                col=col,
                fit=fit_full,
                n=n,
                lrt_pvalue=lrt_pvalue,
                diff_aic=diff_aic,
            )
        )
    else:
        # Multi-column (categorical or codominant). Always emit a
        # summary row for the variable; if report_categorical_betas
        # is True, also one row per term.
        rows.append(
            _row_summary_multiterm(
                outcome=outcome,
                variable=regressor,
                variable_type=variable_type,
                fit=fit_full,
                n=n,
                lrt_pvalue=lrt_pvalue,
                diff_aic=diff_aic,
            )
        )
        if report_categorical_betas:
            for col in reg_cols:
                rows.append(
                    _row_for_single_term(
                        outcome=outcome,
                        variable=col,
                        variable_type=f"{variable_type}_dummy",
                        col=col,
                        fit=fit_full,
                        n=n,
                        lrt_pvalue=lrt_pvalue,
                        diff_aic=diff_aic,
                    )
                )

    return rows, []


def _design_matrix(
    *,
    full: pd.DataFrame,
    regressor_cols: list[str],
    cov_list: list[str],
    standardize_data: bool,
    family: str,
) -> pd.DataFrame:
    """Build the X matrix (with intercept, optionally standardized)."""
    cols = [*regressor_cols, *cov_list]
    if not cols:
        return pd.DataFrame(
            {"const": np.ones(len(full))}, index=full.index,
        )
    X = full[cols].astype(float).copy()
    if standardize_data:
        for c in X.columns:
            col = X[c]
            n_unique = col.nunique(dropna=True)
            if n_unique > 2 and pd.api.types.is_numeric_dtype(col):
                std = col.std(ddof=1)
                if std and not np.isnan(std):
                    X[c] = (col - col.mean()) / std
    X.insert(0, "const", 1.0)
    return X


def _lrt_against_null(
    fit_full,
    fit_null,
    df_extra: int,
) -> tuple[float, float]:
    """Likelihood-ratio test comparing the full model vs the null."""
    if df_extra <= 0:
        return float("nan"), float("nan")
    chi2 = 2.0 * float(fit_full.log_likelihood - fit_null.log_likelihood)
    if not np.isfinite(chi2) or chi2 < 0:
        return float("nan"), float(chi2)
    pval = float(_scipy_stats.chi2.sf(chi2, df_extra))
    return pval, chi2


def _row_for_single_term(
    *,
    outcome: str,
    variable: str,
    variable_type: str,
    col: str,
    fit,
    n: int,
    lrt_pvalue: float,
    diff_aic: float,
) -> dict:
    beta = float(fit.params[col])
    se = float(fit.bse[col])
    pvalue = float(fit.pvalues[col])
    ci = fit.conf_int.loc[col]
    return {
        "outcome": outcome,
        "variable": variable,
        "variable_type": variable_type,
        "n": n,
        "beta": beta,
        "se": se,
        "ci_low": float(ci["lo"]),
        "ci_high": float(ci["hi"]),
        "beta_pvalue": pvalue,
        "lrt_pvalue": lrt_pvalue,
        "diff_aic": diff_aic,
        "converged": bool(fit.converged),
        "error": None,
    }


def _row_summary_multiterm(
    *,
    outcome: str,
    variable: str,
    variable_type: str,
    fit,
    n: int,
    lrt_pvalue: float,
    diff_aic: float,
) -> dict:
    """Aggregate row for multi-column regressors (categorical / codominant)."""
    return {
        "outcome": outcome,
        "variable": variable,
        "variable_type": variable_type,
        "n": n,
        "beta": float("nan"),     # no single beta for multi-term
        "se": float("nan"),
        "ci_low": float("nan"),
        "ci_high": float("nan"),
        "beta_pvalue": float("nan"),
        "lrt_pvalue": lrt_pvalue,
        "diff_aic": diff_aic,
        "converged": bool(fit.converged),
        "error": None,
    }


def _short_err(exc: BaseException) -> str:
    msg = str(exc).strip().splitlines()
    return msg[0][:200] if msg else type(exc).__name__
