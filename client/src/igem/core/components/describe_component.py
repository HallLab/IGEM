from __future__ import annotations

from typing import Any, Iterable, Literal, Optional

import pandas as pd

from igem.core.components.base_component import BaseComponent
from igem.modules import describe as _describe
from igem.modules.data import Genotypes, Phenotypes


class DescribeComponent(BaseComponent):
    """
    Descriptive statistics for phenotypes and genotypes.

    Phenotype methods are quiet — descriptive ops are typically called
    many times during EDA so verbose logs would be noise. Genotype ops
    log a header / footer because they trigger sgkit computations that
    can be expensive on biobank-scale inputs.

    Free functions in :mod:`igem.modules.describe` remain importable
    for callers who do not want to instantiate :class:`IGEM`.
    """

    # ------------------------------------------------------------------
    # Phenotypes (silent — output is the value)
    # ------------------------------------------------------------------
    def summarize(
        self,
        phen: Phenotypes,
        *,
        cols: Optional[Iterable[str]] = None,
        weighted: bool = False,
    ) -> pd.DataFrame:
        return _describe.summarize(phen, cols=cols, weighted=weighted)

    def summarize_by(
        self,
        phen: Phenotypes,
        *,
        by: str,
        cols: Optional[Iterable[str]] = None,
        dropna_group: bool = True,
    ) -> pd.DataFrame:
        return _describe.summarize_by(
            phen, by=by, cols=cols, dropna_group=dropna_group,
        )

    def dataset_summary(self, phen: Phenotypes) -> dict[str, Any]:
        return _describe.dataset_summary(phen)

    def missing_report(
        self,
        phen: Phenotypes,
        *,
        cols: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        return _describe.missing_report(phen, cols=cols)

    def correlation_matrix(
        self,
        phen: Phenotypes,
        *,
        cols: Optional[Iterable[str]] = None,
        method: Literal["pearson", "spearman", "kendall"] = "pearson",
    ) -> pd.DataFrame:
        return _describe.correlation_matrix(phen, cols=cols, method=method)

    def correlation_pairs(
        self,
        phen: Phenotypes,
        *,
        cols: Optional[Iterable[str]] = None,
        method: Literal["pearson", "spearman", "kendall"] = "pearson",
        threshold: float = 0.75,
        absolute: bool = True,
    ) -> pd.DataFrame:
        return _describe.correlation_pairs(
            phen,
            cols=cols,
            method=method,
            threshold=threshold,
            absolute=absolute,
        )

    def crosstab(
        self,
        phen: Phenotypes,
        var1: str,
        var2: str,
        *,
        normalize: bool | Literal["all", "index", "columns"] = False,
        margins: bool = False,
        dropna: bool = True,
    ) -> pd.DataFrame:
        return _describe.crosstab(
            phen, var1, var2,
            normalize=normalize, margins=margins, dropna=dropna,
        )

    def value_counts(
        self,
        phen: Phenotypes,
        *,
        cols: Optional[Iterable[str]] = None,
        top: int = 20,
        dropna: bool = False,
    ) -> dict[str, pd.DataFrame]:
        return _describe.value_counts(
            phen, cols=cols, top=top, dropna=dropna
        )

    def skewness(
        self,
        phen: Phenotypes,
        *,
        cols: Optional[Iterable[str]] = None,
        dropna: bool = False,
    ) -> pd.DataFrame:
        return _describe.skewness(phen, cols=cols, dropna=dropna)

    # ------------------------------------------------------------------
    # Genotypes (logged — sgkit ops can be heavy)
    # ------------------------------------------------------------------
    def variant_stats(self, geno: Genotypes) -> pd.DataFrame:
        self.core.logger.log(
            f"[describe] variant_stats over {geno.n_variants} variants",
            "INFO",
        )
        result = _describe.variant_stats(geno)
        self.core.logger.footer(
            f"[describe] variant_stats: {len(result)} rows, "
            f"{len(result.columns)} columns"
        )
        return result

    def sample_stats(self, geno: Genotypes) -> pd.DataFrame:
        self.core.logger.log(
            f"[describe] sample_stats over {geno.n_samples} samples",
            "INFO",
        )
        result = _describe.sample_stats(geno)
        self.core.logger.footer(
            f"[describe] sample_stats: {len(result)} rows, "
            f"{len(result.columns)} columns"
        )
        return result

    def heterozygosity(
        self,
        geno: Genotypes,
        *,
        outlier_sd: float = 3.0,
    ) -> pd.DataFrame:
        self.core.logger.log(
            f"[describe] heterozygosity over {geno.n_samples} samples "
            f"(outlier_sd={outlier_sd})",
            "INFO",
        )
        result = _describe.heterozygosity(geno, outlier_sd=outlier_sd)
        n_outliers = int(result["is_outlier"].sum())
        self.core.logger.footer(
            f"[describe] heterozygosity: {len(result)} samples, "
            f"{n_outliers} outliers"
        )
        return result

    def genotype_summary(self, geno: Genotypes) -> dict[str, Any]:
        self.core.logger.log(
            f"[describe] genotype_summary "
            f"({geno.n_samples} samples × {geno.n_variants} variants)",
            "INFO",
        )
        result = _describe.genotype_summary(geno)
        self.core.logger.footer(
            f"[describe] genotype_summary: {len(result)} fields"
        )
        return result
