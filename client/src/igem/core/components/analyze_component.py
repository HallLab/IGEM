from __future__ import annotations

from typing import Any, Callable, Iterable, Literal, Optional, Union

import pandas as pd

from igem.core.components.base_component import BaseComponent
from igem.modules import analyze as _analyze
from igem.modules.analyze._engines import RegressionKind
from igem.modules.analyze.results import RegressionResults
from igem.modules.data import Genotypes, Phenotypes


class AnalyzeComponent(BaseComponent):
    """
    Statistical analysis (EWAS, GWAS, LRT, multi-test correction).

    Phase-1 surface (``ewas``, ``lrt``) plus Phase-2 additions
    (``gwas`` via sgkit, survey-aware mode for ``ewas``, and
    annotation chaining via :meth:`RegressionResults.annotate`).
    Each call logs a one-line header / footer with sample and test
    counts so the user has a breadcrumb of what was computed.
    """

    def ewas(
        self,
        phen: Phenotypes,
        outcome: str,
        *,
        exposures: Optional[Iterable[str]] = None,
        covariates: Optional[Iterable[str]] = None,
        family: Optional[str] = None,
        use_survey: bool = False,
        progress: bool = True,
    ) -> RegressionResults:
        n_exp = (
            len(list(exposures))
            if exposures is not None
            else len(phen.exposures)
        )
        survey_tag = " survey=on" if use_survey else ""
        self.core.logger.log(
            f"[analyze] ewas(outcome={outcome!r}, "
            f"n_exposures={n_exp}){survey_tag}",
            "INFO",
        )
        result = _analyze.ewas(
            phen,
            outcome,
            exposures=exposures,
            covariates=covariates,
            family=family,
            use_survey=use_survey,
            progress=progress,
        )
        self.core.logger.footer(
            f"[analyze] ewas: family={result.family}, "
            f"tests={result.n_tests}, errors={result.n_errors}"
        )
        return result

    def gwas(
        self,
        geno: Genotypes,
        phen: Phenotypes,
        outcome: str,
        *,
        covariates: Optional[Iterable[str]] = None,
        family: Optional[str] = None,
    ) -> RegressionResults:
        self.core.logger.log(
            f"[analyze] gwas(outcome={outcome!r}, "
            f"n_variants={geno.n_variants}, n_samples={geno.n_samples})",
            "INFO",
        )
        result = _analyze.gwas(
            geno, phen, outcome,
            covariates=covariates, family=family,
        )
        self.core.logger.footer(
            f"[analyze] gwas: family={result.family}, "
            f"variants_tested={result.n_tests}, "
            f"n_samples={result.metadata.get('n_samples_used', '?')}"
        )
        return result

    def lrt(
        self,
        phen: Phenotypes,
        outcome: str,
        *,
        full: Iterable[str],
        nested: Iterable[str],
        family: Optional[str] = None,
    ) -> dict[str, Any]:
        self.core.logger.log(
            f"[analyze] lrt(outcome={outcome!r})",
            "INFO",
        )
        result = _analyze.lrt(
            phen,
            outcome,
            full=full,
            nested=nested,
            family=family,
        )
        self.core.logger.footer(
            f"[analyze] lrt: chi2={result['chi2']:.3f}, "
            f"df={result['df']}, p={result['p_value']:.3e}, "
            f"n={result['n']}"
        )
        return result

    def association_study(
        self,
        phen: Phenotypes,
        outcomes: Union[str, Iterable[str]],
        regression_variables: Optional[Union[str, Iterable[str]]] = None,
        *,
        geno: Optional[Genotypes] = None,
        covariates: Optional[Iterable[str]] = None,
        family: Optional[str] = None,
        regression_kind: RegressionKind = "auto",
        encoding: str = "additive",
        edge_encoding_info: Optional[pd.DataFrame] = None,
        use_survey: bool = False,
        min_n: int = 200,
        n_jobs: int = 1,
        standardize_data: bool = False,
        report_categorical_betas: bool = False,
        progress: bool = True,
    ) -> RegressionResults:
        n_outcomes = (
            1 if isinstance(outcomes, str) else len(list(outcomes))
        )
        survey_tag = " survey=on" if use_survey else ""
        self.core.logger.log(
            f"[analyze] association_study(n_outcomes={n_outcomes}, "
            f"regression_kind={regression_kind!r}){survey_tag}",
            "INFO",
        )
        result = _analyze.association_study(
            phen, outcomes, regression_variables,
            geno=geno, covariates=covariates, family=family,
            regression_kind=regression_kind,
            encoding=encoding, edge_encoding_info=edge_encoding_info,
            use_survey=use_survey, min_n=min_n, n_jobs=n_jobs,
            standardize_data=standardize_data,
            report_categorical_betas=report_categorical_betas,
            progress=progress,
        )
        self.core.logger.footer(
            f"[analyze] association_study: family={result.family}, "
            f"tests={result.n_tests}, errors={result.n_errors}"
        )
        return result

    def interaction_study(
        self,
        phen: Phenotypes,
        outcomes: Union[str, Iterable[str]],
        interactions: Optional[
            Union[str, Iterable[tuple[str, str]]]
        ] = None,
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
        n_outcomes = (
            1 if isinstance(outcomes, str) else len(list(outcomes))
        )
        self.core.logger.log(
            f"[analyze] interaction_study(n_outcomes={n_outcomes}, "
            f"report_betas={report_betas})",
            "INFO",
        )
        result = _analyze.interaction_study(
            phen, outcomes, interactions,
            covariates=covariates, family=family,
            regression_kind=regression_kind,
            use_survey=use_survey, report_betas=report_betas,
            min_n=min_n, max_pairs=max_pairs, n_jobs=n_jobs,
            progress=progress,
        )
        self.core.logger.footer(
            f"[analyze] interaction_study: pairs_tested={result.n_tests}, "
            f"errors={result.n_errors}"
        )
        return result
