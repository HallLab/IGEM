"""Tests for igem.modules.modify.genotypes (QC filters)."""
from __future__ import annotations

import numpy as np
import pytest

from igem.modules.modify import (
    filter_biallelic,
    filter_hwe,
    filter_maf,
    filter_missingness_samples,
    filter_missingness_variants,
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
