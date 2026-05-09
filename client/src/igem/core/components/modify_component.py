from __future__ import annotations

from typing import Any, Callable, Iterable, Literal, Optional

import pandas as pd

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

    def filter_heterozygosity_outliers(
        self, geno: Genotypes, *, outlier_sd: float = 3.0,
    ) -> Genotypes:
        before = geno.n_samples
        self.core.logger.log(
            f"[modify] filter_heterozygosity_outliers"
            f"(outlier_sd={outlier_sd}) (before: {before} samples)",
            "INFO",
        )
        result = _modify.filter_heterozygosity_outliers(
            geno, outlier_sd=outlier_sd,
        )
        self.core.logger.footer(
            f"[modify] filter_heterozygosity_outliers: "
            f"{before} → {result.n_samples} samples "
            f"(dropped {before - result.n_samples})"
        )
        return result

    def keep_snvs_only(self, geno: Genotypes) -> Genotypes:
        before = geno.n_variants
        self.core.logger.log(
            f"[modify] keep_snvs_only (before: {before} variants)",
            "INFO",
        )
        result = _modify.keep_snvs_only(geno)
        self.core.logger.footer(
            f"[modify] keep_snvs_only: "
            f"{before} → {result.n_variants} variants "
            f"(dropped {before - result.n_variants} indels)"
        )
        return result

    def filter_variants(
        self,
        geno: Genotypes,
        variant_ids: Iterable[str],
        *,
        keep: bool = True,
    ) -> Genotypes:
        before = geno.n_variants
        action = "keep" if keep else "drop"
        self.core.logger.log(
            f"[modify] filter_variants ({action} {len(list(variant_ids))} ids)",
            "INFO",
        )
        # variant_ids was consumed by len(); re-pass via list().
        result = _modify.filter_variants(geno, variant_ids, keep=keep)
        self.core.logger.footer(
            f"[modify] filter_variants: "
            f"{before} → {result.n_variants} variants"
        )
        return result

    def filter_by_region(
        self,
        geno: Genotypes,
        chrom: str,
        *,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> Genotypes:
        before = geno.n_variants
        self.core.logger.log(
            f"[modify] filter_by_region(chrom={chrom!r}, "
            f"start={start}, end={end})",
            "INFO",
        )
        result = _modify.filter_by_region(
            geno, chrom, start=start, end=end,
        )
        self.core.logger.footer(
            f"[modify] filter_by_region: "
            f"{before} → {result.n_variants} variants"
        )
        return result

    def filter_samples(
        self,
        obj,
        sample_ids: Iterable[str],
        *,
        keep: bool = True,
    ):
        action = "keep" if keep else "drop"
        kind = type(obj).__name__
        self.core.logger.log(
            f"[modify] filter_samples ({action} on {kind})",
            "INFO",
        )
        result = _modify.filter_samples(obj, sample_ids, keep=keep)
        self.core.logger.footer(
            f"[modify] filter_samples: {result.n_samples} samples remain"
        )
        return result

    def prune_ld(
        self,
        geno: Genotypes,
        *,
        window: int = 50,
        step: int = 5,
        r2: float = 0.5,
        unit: Literal["variants", "kb"] = "variants",
    ) -> Genotypes:
        before = geno.n_variants
        self.core.logger.log(
            f"[modify] prune_ld(window={window}, step={step}, r2={r2}, "
            f"unit={unit!r}) (before: {before} variants)",
            "INFO",
        )
        result = _modify.prune_ld(
            geno, window=window, step=step, r2=r2, unit=unit,
        )
        self.core.logger.footer(
            f"[modify] prune_ld: "
            f"{before} → {result.n_variants} variants "
            f"(dropped {before - result.n_variants})"
        )
        return result

    # ------------------------------------------------------------------
    # Phenotype transformations
    # ------------------------------------------------------------------
    def discretize(
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
            f"[modify] discretize(col={col!r}, method={method!r}, "
            f"n_bins={n_bins})",
            "INFO",
        )
        result = _modify.discretize(
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
            f"[modify] discretize: {col!r} → {target!r} "
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

    def transform(
        self,
        phen: Phenotypes,
        col: str,
        *,
        method: Optional[str] = None,
        func: Optional[Callable[[pd.Series], pd.Series]] = None,
        new_col: Optional[str] = None,
        replace: bool = False,
    ) -> Phenotypes:
        label = method if method is not None else "<callable>"
        self.core.logger.log(
            f"[modify] transform(col={col!r}, method={label!r})",
            "INFO",
        )
        result = _modify.transform(
            phen, col, method=method, func=func,
            new_col=new_col, replace=replace,
        )
        target = (
            col if replace
            else (new_col or f"{col}_{method or 'transformed'}")
        )
        self.core.logger.footer(
            f"[modify] transform: {col!r} → {target!r}"
        )
        return result

    def remove_outliers(
        self,
        phen: Phenotypes,
        cols: Optional[Iterable[str]] = None,
        *,
        method: Literal["iqr", "gaussian"] = "iqr",
        k: float = 1.5,
        cutoff: float = 3.0,
    ) -> Phenotypes:
        self.core.logger.log(
            f"[modify] remove_outliers(method={method!r}, k={k}, "
            f"cutoff={cutoff})",
            "INFO",
        )
        result = _modify.remove_outliers(
            phen, cols, method=method, k=k, cutoff=cutoff,
        )
        # Compare NaN counts to summarise impact.
        before_na = int(phen.df.isna().sum().sum())
        after_na = int(result.df.isna().sum().sum())
        self.core.logger.footer(
            f"[modify] remove_outliers: {after_na - before_na} values "
            f"replaced with NaN"
        )
        return result

    def auto_classify(
        self,
        phen: Phenotypes,
        *,
        cat_min: int = 3,
        cat_max: int = 6,
        cont_min: int = 15,
    ) -> "pd.DataFrame":
        # Pure report — silent, like describe.
        return _modify.auto_classify(
            phen, cat_min=cat_min, cat_max=cat_max, cont_min=cont_min,
        )

    def make_binary(
        self,
        phen: Phenotypes,
        *,
        skip: Optional[Iterable[str]] = None,
        only: Optional[Iterable[str]] = None,
    ) -> Phenotypes:
        self.core.logger.log(
            f"[modify] make_binary(only={list(only) if only else None}, "
            f"skip={list(skip) if skip else None})",
            "INFO",
        )
        result = _modify.make_binary(phen, skip=skip, only=only)
        self.core.logger.footer(
            f"[modify] make_binary: {phen.n_samples} samples coerced"
        )
        return result

    def make_categorical(
        self,
        phen: Phenotypes,
        *,
        skip: Optional[Iterable[str]] = None,
        only: Optional[Iterable[str]] = None,
    ) -> Phenotypes:
        self.core.logger.log(
            f"[modify] make_categorical(only={list(only) if only else None}, "
            f"skip={list(skip) if skip else None})",
            "INFO",
        )
        result = _modify.make_categorical(phen, skip=skip, only=only)
        self.core.logger.footer(
            f"[modify] make_categorical: {phen.n_samples} samples coerced"
        )
        return result

    def make_continuous(
        self,
        phen: Phenotypes,
        *,
        skip: Optional[Iterable[str]] = None,
        only: Optional[Iterable[str]] = None,
    ) -> Phenotypes:
        self.core.logger.log(
            f"[modify] make_continuous(only={list(only) if only else None}, "
            f"skip={list(skip) if skip else None})",
            "INFO",
        )
        result = _modify.make_continuous(phen, skip=skip, only=only)
        self.core.logger.footer(
            f"[modify] make_continuous: {phen.n_samples} samples coerced"
        )
        return result

    def colfilter(
        self,
        phen: Phenotypes,
        *,
        skip: Optional[Iterable[str]] = None,
        only: Optional[Iterable[str]] = None,
    ) -> Phenotypes:
        before = len(phen.df.columns)
        self.core.logger.log(
            f"[modify] colfilter (before: {before} columns)",
            "INFO",
        )
        result = _modify.colfilter(phen, skip=skip, only=only)
        self.core.logger.footer(
            f"[modify] colfilter: {before} → {len(result.df.columns)} columns"
        )
        return result

    def colfilter_min_n(
        self,
        phen: Phenotypes,
        n: int = 200,
        *,
        skip: Optional[Iterable[str]] = None,
        only: Optional[Iterable[str]] = None,
    ) -> Phenotypes:
        before = len(phen.df.columns)
        self.core.logger.log(
            f"[modify] colfilter_min_n(n={n}) (before: {before} columns)",
            "INFO",
        )
        result = _modify.colfilter_min_n(phen, n, skip=skip, only=only)
        self.core.logger.footer(
            f"[modify] colfilter_min_n: {before} → {len(result.df.columns)} "
            f"columns"
        )
        return result

    def colfilter_min_cat_n(
        self,
        phen: Phenotypes,
        n: int = 200,
        *,
        cat_max: int = 6,
        skip: Optional[Iterable[str]] = None,
        only: Optional[Iterable[str]] = None,
    ) -> Phenotypes:
        before = len(phen.df.columns)
        self.core.logger.log(
            f"[modify] colfilter_min_cat_n(n={n}) (before: {before} columns)",
            "INFO",
        )
        result = _modify.colfilter_min_cat_n(
            phen, n, cat_max=cat_max, skip=skip, only=only,
        )
        self.core.logger.footer(
            f"[modify] colfilter_min_cat_n: {before} → "
            f"{len(result.df.columns)} columns"
        )
        return result

    def colfilter_percent_zero(
        self,
        phen: Phenotypes,
        *,
        max_zero_pct: float = 90.0,
        skip: Optional[Iterable[str]] = None,
        only: Optional[Iterable[str]] = None,
    ) -> Phenotypes:
        before = len(phen.df.columns)
        self.core.logger.log(
            f"[modify] colfilter_percent_zero(max_zero_pct={max_zero_pct}) "
            f"(before: {before} columns)",
            "INFO",
        )
        result = _modify.colfilter_percent_zero(
            phen, max_zero_pct=max_zero_pct, skip=skip, only=only,
        )
        self.core.logger.footer(
            f"[modify] colfilter_percent_zero: {before} → "
            f"{len(result.df.columns)} columns"
        )
        return result

    def merge_observations(
        self,
        top: Phenotypes,
        bottom: Phenotypes,
    ) -> Phenotypes:
        self.core.logger.log(
            f"[modify] merge_observations "
            f"({top.n_samples} + {bottom.n_samples} samples)",
            "INFO",
        )
        result = _modify.merge_observations(top, bottom)
        self.core.logger.footer(
            f"[modify] merge_observations: {result.n_samples} samples × "
            f"{len(result.df.columns)} columns"
        )
        return result

    def merge_variables(
        self,
        left: Phenotypes,
        right: Phenotypes,
        *,
        how: Literal["outer", "inner", "left", "right"] = "outer",
    ) -> Phenotypes:
        self.core.logger.log(
            f"[modify] merge_variables(how={how!r})",
            "INFO",
        )
        result = _modify.merge_variables(left, right, how=how)
        self.core.logger.footer(
            f"[modify] merge_variables: {result.n_samples} samples × "
            f"{len(result.df.columns)} columns"
        )
        return result

    def move_variables(
        self,
        src: Phenotypes,
        dst: Phenotypes,
        *,
        skip: Optional[Iterable[str]] = None,
        only: Optional[Iterable[str]] = None,
    ) -> tuple[Phenotypes, Phenotypes]:
        self.core.logger.log(
            "[modify] move_variables",
            "INFO",
        )
        new_src, new_dst = _modify.move_variables(
            src, dst, skip=skip, only=only,
        )
        moved = (
            len(src.df.columns) - len(new_src.df.columns)
        )
        self.core.logger.footer(
            f"[modify] move_variables: {moved} column(s) moved "
            f"(src: {len(new_src.df.columns)} cols, "
            f"dst: {len(new_dst.df.columns)} cols)"
        )
        return new_src, new_dst

    def rowfilter_incomplete_obs(
        self,
        phen: Phenotypes,
        *,
        skip: Optional[Iterable[str]] = None,
        only: Optional[Iterable[str]] = None,
    ) -> Phenotypes:
        before = phen.n_samples
        self.core.logger.log(
            f"[modify] rowfilter_incomplete_obs (before: {before} samples)",
            "INFO",
        )
        result = _modify.rowfilter_incomplete_obs(
            phen, skip=skip, only=only,
        )
        self.core.logger.footer(
            f"[modify] rowfilter_incomplete_obs: "
            f"{before} → {result.n_samples} samples "
            f"(dropped {before - result.n_samples})"
        )
        return result
