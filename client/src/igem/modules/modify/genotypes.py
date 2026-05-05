"""
Genotype QC filters.

Each function takes a :class:`Genotypes` and returns a new :class:`Genotypes`
restricted to the variants/samples that pass the filter. Statistics are
computed via sgkit where available (``variant_stats``, ``sample_stats``,
``hardy_weinberg_test``), so calls stay lazy / Dask-backed until the
final mask is materialised.
"""
from __future__ import annotations

import numpy as np

from igem.modules.data import Genotypes


# ----------------------------------------------------------------------
# Allele structure
# ----------------------------------------------------------------------
def filter_biallelic(geno: Genotypes) -> Genotypes:
    """
    Keep only biallelic variants (exactly 2 distinct non-empty alleles).

    For datasets where the ``alleles`` dimension has size 2 this is a
    no-op. For VCF/VCZ-derived datasets with multi-allelic rows, this
    filter drops anything with >2 populated allele slots.
    """
    ds = geno.ds
    if "variant_allele" not in ds.variables:
        return geno

    alleles = np.asarray(ds["variant_allele"].values)
    if alleles.ndim != 2:
        return geno
    if alleles.shape[1] <= 2:
        return geno

    non_empty = np.vectorize(_is_non_empty)(alleles)
    n_alleles_per_variant = non_empty.sum(axis=1)
    mask = n_alleles_per_variant == 2
    return geno.select(variant_mask=mask)


def _is_non_empty(x) -> bool:
    if isinstance(x, (bytes, bytearray)):
        return len(x) > 0 and x != b"."
    if isinstance(x, str):
        return len(x) > 0 and x != "."
    return x is not None


# ----------------------------------------------------------------------
# MAF
# ----------------------------------------------------------------------
def filter_maf(geno: Genotypes, *, min_maf: float = 0.01) -> Genotypes:
    """
    Keep variants whose minor allele frequency is at least ``min_maf``.

    Assumes biallelic variants. For multi-allelic sites, the MAF is
    conservatively defined as the smallest allele frequency across all
    alleles of that variant.
    """
    if not 0.0 <= min_maf <= 0.5:
        raise ValueError(
            f"min_maf must be in [0.0, 0.5]; got {min_maf}"
        )

    import sgkit

    ds = sgkit.variant_stats(geno.ds)
    af = np.asarray(ds["variant_allele_frequency"].values)
    if af.ndim == 2 and af.shape[1] >= 2:
        maf = np.nanmin(af, axis=1)
    else:
        maf = np.minimum(af, 1.0 - af)

    mask = (~np.isnan(maf)) & (maf >= min_maf)
    return geno.select(variant_mask=mask)


# ----------------------------------------------------------------------
# Missingness
# ----------------------------------------------------------------------
def filter_missingness_variants(
    geno: Genotypes, *, max_missing: float = 0.05
) -> Genotypes:
    """
    Drop variants where the fraction of missing calls across samples is
    above ``max_missing`` (i.e. keep variants with call rate ≥
    ``1 - max_missing``).
    """
    _check_rate(max_missing, "max_missing")
    import sgkit

    ds = sgkit.variant_stats(geno.ds)
    call_rate = np.asarray(ds["variant_call_rate"].values)
    mask = call_rate >= (1.0 - max_missing)
    return geno.select(variant_mask=mask)


def filter_missingness_samples(
    geno: Genotypes, *, max_missing: float = 0.05
) -> Genotypes:
    """
    Drop samples whose fraction of missing calls across variants is
    above ``max_missing``.
    """
    _check_rate(max_missing, "max_missing")
    import sgkit

    ds = sgkit.sample_stats(geno.ds)
    call_rate = np.asarray(ds["sample_call_rate"].values)
    mask = call_rate >= (1.0 - max_missing)
    return geno.select(sample_mask=mask)


# ----------------------------------------------------------------------
# Hardy-Weinberg equilibrium
# ----------------------------------------------------------------------
def filter_hwe(geno: Genotypes, *, min_pvalue: float = 1e-6) -> Genotypes:
    """
    Drop variants whose Hardy-Weinberg equilibrium exact-test p-value is
    below ``min_pvalue``. A higher ``min_pvalue`` is more aggressive.

    Uses :func:`sgkit.hardy_weinberg_test`, which operates on biallelic
    variants — run :func:`filter_biallelic` first when working with
    multi-allelic input.
    """
    if not 0.0 <= min_pvalue <= 1.0:
        raise ValueError(
            f"min_pvalue must be in [0.0, 1.0]; got {min_pvalue}"
        )
    import sgkit

    ds = sgkit.hardy_weinberg_test(geno.ds)
    pvalues = np.asarray(ds["variant_hwe_p_value"].values)
    mask = (~np.isnan(pvalues)) & (pvalues >= min_pvalue)
    return geno.select(variant_mask=mask)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _check_rate(value: float, name: str) -> None:
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be in [0.0, 1.0]; got {value}")
