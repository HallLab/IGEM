from __future__ import annotations

from typing import Any, Iterable, Optional

from igem.core.components.base_component import BaseComponent
from igem.modules import modify as _modify
from igem.modules.data import Genotypes, Phenotypes


class ModifyComponent(BaseComponent):
    """
    Quality-control filters and phenotype transformations.

    Thin wrappers around :mod:`igem.modules.modify` that emit a header /
    footer log so the user has a breadcrumb of how many samples or
    variants each step dropped. Free functions in the module remain
    importable for callers who do not need an :class:`IGEM` instance.
    """

    # ------------------------------------------------------------------
    # Genotype filters
    # ------------------------------------------------------------------
    def filter_biallelic(self, geno: Genotypes) -> Genotypes:
        before = geno.n_variants
        self.core.logger.log(
            f"[modify] filter_biallelic (before: {before} variants)",
            "INFO",
        )
        result = _modify.filter_biallelic(geno)
        self.core.logger.footer(
            f"[modify] filter_biallelic: "
            f"{before} → {result.n_variants} variants "
            f"(dropped {before - result.n_variants})"
        )
        return result

    def filter_maf(
        self, geno: Genotypes, *, min_maf: float = 0.01
    ) -> Genotypes:
        before = geno.n_variants
        self.core.logger.log(
            f"[modify] filter_maf(min_maf={min_maf}) "
            f"(before: {before} variants)",
            "INFO",
        )
        result = _modify.filter_maf(geno, min_maf=min_maf)
        self.core.logger.footer(
            f"[modify] filter_maf(min_maf={min_maf}): "
            f"{before} → {result.n_variants} variants "
            f"(dropped {before - result.n_variants})"
        )
        return result

    def filter_missingness_variants(
        self, geno: Genotypes, *, max_missing: float = 0.05
    ) -> Genotypes:
        before = geno.n_variants
        self.core.logger.log(
            f"[modify] filter_missingness_variants(max_missing={max_missing})"
            f" (before: {before} variants)",
            "INFO",
        )
        result = _modify.filter_missingness_variants(
            geno, max_missing=max_missing
        )
        self.core.logger.footer(
            f"[modify] filter_missingness_variants: "
            f"{before} → {result.n_variants} variants "
            f"(dropped {before - result.n_variants})"
        )
        return result

    def filter_missingness_samples(
        self, geno: Genotypes, *, max_missing: float = 0.05
    ) -> Genotypes:
        before = geno.n_samples
        self.core.logger.log(
            f"[modify] filter_missingness_samples(max_missing={max_missing})"
            f" (before: {before} samples)",
            "INFO",
        )
        result = _modify.filter_missingness_samples(
            geno, max_missing=max_missing
        )
        self.core.logger.footer(
            f"[modify] filter_missingness_samples: "
            f"{before} → {result.n_samples} samples "
            f"(dropped {before - result.n_samples})"
        )
        return result

    def filter_hwe(
        self, geno: Genotypes, *, min_pvalue: float = 1e-6
    ) -> Genotypes:
        before = geno.n_variants
        self.core.logger.log(
            f"[modify] filter_hwe(min_pvalue={min_pvalue}) "
            f"(before: {before} variants)",
            "INFO",
        )
        result = _modify.filter_hwe(geno, min_pvalue=min_pvalue)
        self.core.logger.footer(
            f"[modify] filter_hwe(min_pvalue={min_pvalue}): "
            f"{before} → {result.n_variants} variants "
            f"(dropped {before - result.n_variants})"
        )
        return result

    # ------------------------------------------------------------------
    # Phenotype transformations
    # ------------------------------------------------------------------
    def categorize(
        self,
        phen: Phenotypes,
        col: str,
        *,
        method: str = "quantiles",
        n_bins: int = 4,
        bin_edges: Optional[Iterable[float]] = None,
        labels: Optional[Iterable[str]] = None,
        new_col: Optional[str] = None,
        replace: bool = False,
    ) -> Phenotypes:
        self.core.logger.log(
            f"[modify] categorize(col={col!r}, method={method!r}, "
            f"n_bins={n_bins})",
            "INFO",
        )
        result = _modify.categorize(
            phen,
            col,
            method=method,
            n_bins=n_bins,
            bin_edges=bin_edges,
            labels=labels,
            new_col=new_col,
            replace=replace,
        )
        target = col if replace else (new_col or f"{col}_cat")
        self.core.logger.footer(
            f"[modify] categorize: {col!r} → {target!r} "
            f"({result.df[target].nunique(dropna=True)} unique categories)"
        )
        return result

    def recode(
        self,
        phen: Phenotypes,
        col: str,
        mapping: dict[Any, Any],
        *,
        missing_values: Optional[Iterable[Any]] = None,
        new_col: Optional[str] = None,
        replace: bool = True,
    ) -> Phenotypes:
        self.core.logger.log(
            f"[modify] recode(col={col!r}, mapping_size={len(mapping)})",
            "INFO",
        )
        result = _modify.recode(
            phen,
            col,
            mapping,
            missing_values=missing_values,
            new_col=new_col,
            replace=replace,
        )
        target = col if replace else (new_col or f"{col}_recoded")
        self.core.logger.footer(
            f"[modify] recode: {col!r} → {target!r} "
            f"({result.df[target].nunique(dropna=True)} unique values)"
        )
        return result

    def drop_missing(
        self,
        phen: Phenotypes,
        cols: Optional[Iterable[str]] = None,
    ) -> Phenotypes:
        before = phen.n_samples
        self.core.logger.log(
            f"[modify] drop_missing(cols={list(cols) if cols else 'default'})"
            f" (before: {before} samples)",
            "INFO",
        )
        result = _modify.drop_missing(phen, cols=cols)
        self.core.logger.footer(
            f"[modify] drop_missing: {before} → {result.n_samples} samples "
            f"(dropped {before - result.n_samples})"
        )
        return result
