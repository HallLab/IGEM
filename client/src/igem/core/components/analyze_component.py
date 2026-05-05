from __future__ import annotations

from typing import Any, Iterable, Optional

from igem.core.components.base_component import BaseComponent
from igem.modules import analyze as _analyze
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
