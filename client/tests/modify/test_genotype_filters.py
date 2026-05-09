"""Tests for igem.modules.modify.genotypes (QC filters)."""
from __future__ import annotations

import numpy as np
import pytest

from igem.modules.modify import (
    filter_biallelic,
    filter_by_region,
    filter_heterozygosity_outliers,
    filter_hwe,
    filter_maf,
    filter_missingness_samples,
    filter_missingness_variants,
    filter_samples,
    filter_variants,
    keep_snvs_only,
    prune_ld,
)


# ---------------------------------------------------------------------------
# filter_biallelic
# ---------------------------------------------------------------------------
class TestFilterBiallelic:
    def test_drops_triallelic_variants(self, multiallelic_geno):
        result = filter_biallelic(multiallelic_geno)
        assert result.n_variants == 2
        assert list(result.samples) == list(multiallelic_geno.samples)
        kept_ids = set(np.asarray(result.ds["variant_id"].values).tolist())
        assert kept_ids == {"v1", "v3"}

    def test_noop_when_alleles_dim_is_two(self, maf_geno):
        result = filter_biallelic(maf_geno)
        assert result.n_variants == maf_geno.n_variants


# ---------------------------------------------------------------------------
# filter_maf
# ---------------------------------------------------------------------------
class TestFilterMaf:
    def test_drops_rare_variants(self, maf_geno):
        result = filter_maf(maf_geno, min_maf=0.10)
        kept_ids = set(np.asarray(result.ds["variant_id"].values).tolist())
        assert kept_ids == {"v_common"}

    def test_keeps_everything_at_zero_threshold(self, maf_geno):
        result = filter_maf(maf_geno, min_maf=0.0)
        assert result.n_variants == maf_geno.n_variants

    def test_rejects_invalid_threshold(self, maf_geno):
        with pytest.raises(ValueError, match="min_maf"):
            filter_maf(maf_geno, min_maf=-0.1)
        with pytest.raises(ValueError, match="min_maf"):
            filter_maf(maf_geno, min_maf=0.8)


# ---------------------------------------------------------------------------
# filter_missingness_variants / filter_missingness_samples
# ---------------------------------------------------------------------------
class TestFilterMissingnessVariants:
    def test_keeps_clean_drops_mostly_missing(self, missingness_geno):
        # v_clean: 0.0 missing, v_part: 0.4 missing, v_missing: 0.8 missing
        result = filter_missingness_variants(
            missingness_geno, max_missing=0.5
        )
        kept_ids = set(np.asarray(result.ds["variant_id"].values).tolist())
        assert kept_ids == {"v_clean", "v_part"}

    def test_strict_threshold_keeps_only_clean(self, missingness_geno):
        result = filter_missingness_variants(
            missingness_geno, max_missing=0.01
        )
        kept_ids = set(np.asarray(result.ds["variant_id"].values).tolist())
        assert kept_ids == {"v_clean"}

    def test_rejects_invalid_threshold(self, missingness_geno):
        with pytest.raises(ValueError, match="max_missing"):
            filter_missingness_variants(missingness_geno, max_missing=1.5)


class TestFilterMissingnessSamples:
    def test_drops_samples_with_high_missingness(self, missingness_geno):
        # s0 missing at 0 of 3 variants (0.0)
        # s1,s2 missing at 1 of 3 (~0.33)
        # s3 missing at 2 of 3 (~0.67)
        # s4 missing at 2 of 3 (~0.67)
        result = filter_missingness_samples(
            missingness_geno, max_missing=0.5
        )
        kept_ids = set(np.asarray(result.ds["sample_id"].values).tolist())
        assert kept_ids == {"s0", "s1", "s2"}

    def test_keep_all_at_full_tolerance(self, missingness_geno):
        result = filter_missingness_samples(
            missingness_geno, max_missing=1.0
        )
        assert result.n_samples == missingness_geno.n_samples


# ---------------------------------------------------------------------------
# filter_hwe
# ---------------------------------------------------------------------------
class TestFilterHwe:
    def test_drops_strong_violators(self, hwe_geno):
        result = filter_hwe(hwe_geno, min_pvalue=1e-6)
        kept_ids = set(np.asarray(result.ds["variant_id"].values).tolist())
        assert "v_bad" not in kept_ids
        assert {"v_ok_a", "v_ok_b"}.issubset(kept_ids)

    def test_keeps_all_at_zero_threshold(self, hwe_geno):
        result = filter_hwe(hwe_geno, min_pvalue=0.0)
        assert result.n_variants == hwe_geno.n_variants

    def test_rejects_invalid_threshold(self, hwe_geno):
        with pytest.raises(ValueError, match="min_pvalue"):
            filter_hwe(hwe_geno, min_pvalue=-0.1)
        with pytest.raises(ValueError, match="min_pvalue"):
            filter_hwe(hwe_geno, min_pvalue=2.0)


# ---------------------------------------------------------------------------
# filter_heterozygosity_outliers
# ---------------------------------------------------------------------------
class TestFilterHeterozygosityOutliers:
    def _build_outlier_geno(self):
        """1 sample with 0% het, 9 with 100% het → s0 is outlier."""
        from igem.modules.data import Genotypes
        import xarray as xr
        n_var, n_samp = 30, 10
        gt = np.zeros((n_var, n_samp, 2), dtype=np.int8)
        gt[:, 1:, 0] = 0
        gt[:, 1:, 1] = 1
        alleles = np.array([[b"A", b"C"]] * n_var, dtype="|S1")
        ds = xr.Dataset(
            {
                "call_genotype": (
                    ("variants", "samples", "ploidy"), gt,
                ),
                "variant_allele": (("variants", "alleles"), alleles),
                "variant_id": (
                    "variants",
                    np.array([f"v{i}" for i in range(n_var)], dtype=object),
                ),
                "sample_id": (
                    "samples",
                    np.array([f"s{i}" for i in range(n_samp)], dtype=object),
                ),
                "variant_contig": (
                    "variants", np.zeros(n_var, dtype=np.int32),
                ),
                "variant_position": (
                    "variants", np.arange(n_var, dtype=np.int32),
                ),
            },
            attrs={"contigs": ["1"]},
        )
        return Genotypes(ds)

    def test_drops_outlier_sample(self):
        geno = self._build_outlier_geno()
        result = filter_heterozygosity_outliers(geno, outlier_sd=2.0)
        kept = set(np.asarray(result.ds["sample_id"].values).tolist())
        assert "s0" not in kept
        assert result.n_samples == 9

    def test_high_threshold_keeps_all(self):
        geno = self._build_outlier_geno()
        result = filter_heterozygosity_outliers(geno, outlier_sd=10.0)
        assert result.n_samples == geno.n_samples

    def test_invalid_sd_raises(self, synthetic_geno_ds):
        from igem.modules.data import Genotypes
        with pytest.raises(ValueError, match="outlier_sd"):
            filter_heterozygosity_outliers(
                Genotypes(synthetic_geno_ds), outlier_sd=0,
            )


# ---------------------------------------------------------------------------
# keep_snvs_only
# ---------------------------------------------------------------------------
class TestKeepSnvsOnly:
    def test_drops_indels(self, indel_geno):
        result = keep_snvs_only(indel_geno)
        kept = set(np.asarray(result.ds["variant_id"].values).tolist())
        assert kept == {"v_snv"}
        assert result.n_variants == 1

    def test_noop_on_pure_snv_dataset(self, maf_geno):
        result = keep_snvs_only(maf_geno)
        assert result.n_variants == maf_geno.n_variants


# ---------------------------------------------------------------------------
# filter_variants (by ID)
# ---------------------------------------------------------------------------
class TestFilterVariants:
    def test_keep_listed(self, maf_geno):
        result = filter_variants(maf_geno, ["v_common", "v_rare_a"])
        kept = set(np.asarray(result.ds["variant_id"].values).tolist())
        assert kept == {"v_common", "v_rare_a"}

    def test_drop_listed(self, maf_geno):
        result = filter_variants(maf_geno, ["v_common"], keep=False)
        kept = set(np.asarray(result.ds["variant_id"].values).tolist())
        assert "v_common" not in kept
        assert kept == {"v_rare_a", "v_rare_b"}

    def test_unknown_ids_silently_ignored(self, maf_geno):
        result = filter_variants(
            maf_geno, ["v_common", "not_a_variant"]
        )
        assert result.n_variants == 1


# ---------------------------------------------------------------------------
# filter_by_region
# ---------------------------------------------------------------------------
class TestFilterByRegion:
    def test_full_chrom(self, indel_geno):
        result = filter_by_region(indel_geno, "1")
        assert result.n_variants == indel_geno.n_variants

    def test_position_range(self, indel_geno):
        # indel_geno positions: 100, 200, 300 (all on chrom "1")
        result = filter_by_region(indel_geno, "1", start=150, end=250)
        positions = np.asarray(result.ds["variant_position"].values).tolist()
        assert positions == [200]

    def test_start_only(self, indel_geno):
        result = filter_by_region(indel_geno, "1", start=200)
        positions = np.asarray(result.ds["variant_position"].values).tolist()
        assert sorted(positions) == [200, 300]

    def test_end_only(self, indel_geno):
        result = filter_by_region(indel_geno, "1", end=200)
        positions = np.asarray(result.ds["variant_position"].values).tolist()
        assert sorted(positions) == [100, 200]

    def test_unknown_chrom_raises(self, indel_geno):
        with pytest.raises(ValueError, match="not in dataset contigs"):
            filter_by_region(indel_geno, "X")


# ---------------------------------------------------------------------------
# filter_samples
# ---------------------------------------------------------------------------
class TestFilterSamplesGeno:
    def test_keep_listed(self, missingness_geno):
        result = filter_samples(missingness_geno, ["s0", "s1"])
        kept = set(np.asarray(result.ds["sample_id"].values).tolist())
        assert kept == {"s0", "s1"}

    def test_drop_listed(self, missingness_geno):
        result = filter_samples(
            missingness_geno, ["s0", "s1"], keep=False,
        )
        kept = set(np.asarray(result.ds["sample_id"].values).tolist())
        assert {"s0", "s1"}.isdisjoint(kept)


class TestFilterSamplesPhen:
    def test_keep_phenotype_subset(self, transform_phen):
        result = filter_samples(transform_phen, ["S0", "S2", "S4"])
        ids = list(result.df[result.sample_id_col])
        assert ids == ["S0", "S2", "S4"]

    def test_drop_phenotype_subset(self, transform_phen):
        result = filter_samples(transform_phen, ["S0"], keep=False)
        assert "S0" not in list(result.df[result.sample_id_col])
        assert result.n_samples == transform_phen.n_samples - 1

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError, match="filter_samples expects"):
            filter_samples("not_a_wrapper", ["x"])


# ---------------------------------------------------------------------------
# prune_ld
# ---------------------------------------------------------------------------
class TestPruneLd:
    def test_drops_perfectly_correlated_pair(self, ld_geno):
        # ld_geno has v0==v1 and v2==v3 → at any r² threshold ≤ 1 we
        # expect the pruner to drop one of each pair (keep 2 of 4).
        result = prune_ld(ld_geno, window=4, step=4, r2=0.5)
        assert result.n_variants == 2

    def test_window_isolates_pairs(self, ld_geno):
        # window=2 step=2 means each pair sits in its own window — each
        # pair still produces 1 surviving variant (4 → 2 total).
        result = prune_ld(ld_geno, window=2, step=2, r2=0.5)
        assert result.n_variants == 2

    def test_invalid_window_raises(self, ld_geno):
        with pytest.raises(ValueError, match="window must"):
            prune_ld(ld_geno, window=0)

    def test_invalid_step_raises(self, ld_geno):
        with pytest.raises(ValueError, match="step must"):
            prune_ld(ld_geno, step=0)

    def test_invalid_r2_raises(self, ld_geno):
        with pytest.raises(ValueError, match="r2 must"):
            prune_ld(ld_geno, r2=1.5)
        with pytest.raises(ValueError, match="r2 must"):
            prune_ld(ld_geno, r2=-0.1)

    def test_invalid_unit_raises(self, ld_geno):
        with pytest.raises(ValueError, match="unit must"):
            prune_ld(ld_geno, unit="bogus")
