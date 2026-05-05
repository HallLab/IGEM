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
    """

    # ------------------------------------------------------------------
    # Phenotypes (silent — output is the value)
    # ------------------------------------------------------------------
    def summarize(
        self,
        phen: Phenotypes,
        *,
        cols: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        return _describe.summarize(phen, cols=cols)

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
