"""
Genotype QC filters.

Each function takes a :class:`Genotypes` and returns a new :class:`Genotypes`
restricted to the variants/samples that pass the filter. Statistics are
computed via sgkit where available (``variant_stats``, ``sample_stats``,
``hardy_weinberg_test``), so calls stay lazy / Dask-backed until the
final mask is materialised.
"""
from __future__ import annotations

from typing import Iterable, Literal, Optional, Union

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


def _allele_length(x) -> int:
    if isinstance(x, (bytes, bytearray, str)):
        return len(x)
    return 0


def keep_snvs_only(geno: Genotypes) -> Genotypes:
    """
    Keep only SNV (single-nucleotide variant) sites.

    A site is an SNV if every non-empty allele has length 1.
    Insertions, deletions and other multi-base substitutions are
    dropped. No-op on datasets that lack the ``variant_allele``
    array.

    Common QC step in GWAS pipelines: indels increase computational
    cost and many association tools or LD reference panels only
    support SNVs.
    """
    ds = geno.ds
    if "variant_allele" not in ds.variables:
        return geno

    alleles = np.asarray(ds["variant_allele"].values)
    if alleles.ndim != 2:
        return geno

    def _row_is_snv(row) -> bool:
        return all(
            _allele_length(a) <= 1
            for a in row
            if _is_non_empty(a)
        )

    mask = np.array([_row_is_snv(row) for row in alleles])
    return geno.select(variant_mask=mask)


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
# Variant selection — by ID list
# ----------------------------------------------------------------------
def filter_variants(
    geno: Genotypes,
    variant_ids: Iterable[str],
    *,
    keep: bool = True,
) -> Genotypes:
    """
    Keep or drop variants by ``variant_id``.

    With ``keep=True`` (default), retain only variants whose
    ``variant_id`` is in the supplied iterable. With ``keep=False``,
    drop those variants instead. IDs not present in the dataset are
    ignored silently.

    Useful for restricting analysis to a custom panel (gene-set
    pathway, ClinVar-pathogenic, top-hits panel) or for excluding a
    blacklist (e.g., flagged probes from genotyping QC reports).
    """
    target = set(variant_ids)
    available = np.asarray(geno.ds["variant_id"].values)
    in_set = np.isin(available, list(target))
    mask = in_set if keep else ~in_set
    return geno.select(variant_mask=mask)


# ----------------------------------------------------------------------
# Variant selection — by genomic region
# ----------------------------------------------------------------------
def filter_by_region(
    geno: Genotypes,
    chrom: str,
    *,
    start: Optional[int] = None,
    end: Optional[int] = None,
) -> Genotypes:
    """
    Keep variants within a genomic region.

    ``chrom`` selects a single contig (matched against the
    ``contigs`` attribute attached by sgkit). ``start`` and ``end``
    are inclusive position bounds on that contig:

    - both ``None`` → keep the whole contig.
    - only ``start`` → keep positions ``≥ start``.
    - only ``end`` → keep positions ``≤ end``.
    - both → keep ``start ≤ position ≤ end``.

    Common use cases: chrX exclusion (call with another chrom),
    locus-specific fine-mapping, gene-region eQTL analysis.
    """
    ds = geno.ds
    if "contigs" not in ds.attrs:
        raise ValueError(
            "filter_by_region requires a 'contigs' attribute on the dataset"
        )
    contigs = list(ds.attrs["contigs"])
    if chrom not in contigs:
        raise ValueError(
            f"chrom {chrom!r} not in dataset contigs: {contigs}"
        )
    target_idx = contigs.index(chrom)

    contig_arr = np.asarray(ds["variant_contig"].values)
    on_chrom = contig_arr == target_idx

    if start is None and end is None:
        mask = on_chrom
    else:
        if "variant_position" not in ds.variables:
            raise ValueError(
                "start/end filtering requires 'variant_position' on the "
                "dataset"
            )
        positions = np.asarray(ds["variant_position"].values)
        mask = on_chrom.copy()
        if start is not None:
            mask &= positions >= start
        if end is not None:
            mask &= positions <= end

    return geno.select(variant_mask=mask)


# ----------------------------------------------------------------------
# Heterozygosity outlier filter
# ----------------------------------------------------------------------
def filter_heterozygosity_outliers(
    geno: Genotypes,
    *,
    outlier_sd: float = 3.0,
) -> Genotypes:
    """
    Drop samples flagged as heterozygosity outliers.

    Uses :func:`igem.modules.describe.heterozygosity` to compute the
    per-sample het rate and z-score, then removes samples with
    ``|z| > outlier_sd``. The default ``outlier_sd=3.0`` matches the
    PLINK convention (Anderson et al., 2010); samples beyond 3 SD are
    typically interpreted as contaminated (high het rate) or
    inbred / cryptically related (low het rate).

    Closes the standard QC loop: ``describe.heterozygosity`` reports →
    ``modify.filter_heterozygosity_outliers`` acts.
    """
    if outlier_sd <= 0:
        raise ValueError(
            f"outlier_sd must be positive; got {outlier_sd}"
        )
    from igem.modules.describe import heterozygosity

    het = heterozygosity(geno, outlier_sd=outlier_sd)
    keep_mask = ~het["is_outlier"].to_numpy(dtype=bool)
    return geno.select(sample_mask=keep_mask)


# ----------------------------------------------------------------------
# LD pruning
# ----------------------------------------------------------------------
def prune_ld(
    geno: Genotypes,
    *,
    window: int = 50,
    step: int = 5,
    r2: float = 0.5,
    unit: Literal["variants", "kb"] = "variants",
) -> Genotypes:
    """
    Greedy sliding-window LD pruning.

    Iterates over windows of size ``window`` (variants or kb) advancing
    by ``step`` and drops one variant from each pair whose squared
    correlation :math:`r^2` exceeds the threshold. Output is a
    near-LD-independent subset suitable for null-model fitting (SAIGE
    step 1), kinship estimation, and PCA.

    Defaults ``(window=50, step=5, r2=0.5)`` follow the SAIGE /
    PLINK ``--indep-pairwise 50 5 0.5`` convention. Set
    ``unit="kb"`` to interpret ``window`` and ``step`` in kilobases
    (typical alternative: ``--indep-pairwise 1000kb 50kb 0.05``).

    Implementation wraps :func:`sgkit.ld_prune`. Requires biallelic
    diploid data; missing genotypes are handled by sgkit's internal
    masking.
    """
    if window <= 0:
        raise ValueError(f"window must be positive; got {window}")
    if step <= 0:
        raise ValueError(f"step must be positive; got {step}")
    if not 0.0 <= r2 <= 1.0:
        raise ValueError(f"r2 must be in [0.0, 1.0]; got {r2}")
    if unit not in ("variants", "kb"):
        raise ValueError(
            f"unit must be 'variants' or 'kb'; got {unit!r}"
        )
    import sgkit

    ds = geno.ds
    if "call_dosage" not in ds.variables:
        if "call_genotype" not in ds.variables:
            raise ValueError(
                "prune_ld requires either 'call_dosage' or "
                "'call_genotype' on the dataset"
            )
        gt = ds["call_genotype"]
        # Sum over ploidy axis; NaN if any ploidy slot is missing (-1).
        dosage = gt.where(gt != -1).sum(dim="ploidy", skipna=False)
        ds = ds.assign(call_dosage=dosage)

    if unit == "variants":
        ds_win = sgkit.window_by_variant(ds, size=window, step=step)
    else:  # kb
        ds_win = sgkit.window_by_position(
            ds, size=window * 1000, step=step * 1000,
        )

    pruned = sgkit.ld_prune(ds_win, threshold=r2)
    return Genotypes(pruned)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _check_rate(value: float, name: str) -> None:
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be in [0.0, 1.0]; got {value}")
