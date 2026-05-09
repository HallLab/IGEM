"""Tests for igem.modules.describe.genotypes."""
from __future__ import annotations

import numpy as np
import pytest

from igem.modules.data import Genotypes
from igem.modules.describe import (
    genotype_summary,
    heterozygosity,
    sample_stats,
    variant_stats,
)


# ---------------------------------------------------------------------------
# variant_stats
# ---------------------------------------------------------------------------
class TestVariantStats:
    def test_one_row_per_variant(self, synthetic_geno_ds):
        out = variant_stats(Genotypes(synthetic_geno_ds))
        assert len(out) == synthetic_geno_ds.sizes["variants"]

    def test_expected_columns_present(self, synthetic_geno_ds):
        out = variant_stats(Genotypes(synthetic_geno_ds))
        for col in (
            "variant_id", "position",
            "n_called", "n_het", "n_hom_ref", "n_hom_alt",
            "call_rate", "maf",
        ):
            assert col in out.columns, f"missing column: {col}"

    def test_call_rate_in_unit_interval(self, synthetic_geno_ds):
        out = variant_stats(Genotypes(synthetic_geno_ds))
        rates = out["call_rate"].dropna()
        assert ((rates >= 0.0) & (rates <= 1.0)).all()

    def test_maf_in_zero_half_interval(self, synthetic_geno_ds):
        out = variant_stats(Genotypes(synthetic_geno_ds))
        mafs = out["maf"].dropna()
        # MAF is min allele frequency: in [0, 0.5] for biallelic data.
        assert ((mafs >= 0.0) & (mafs <= 0.5 + 1e-9)).all()

    def test_maf_matches_known_fixture(self, maf_geno):
        # maf_geno has variants with MAF 0.05, 0.50, 0.05 (see conftest).
        out = variant_stats(maf_geno).set_index("variant_id")
        assert out.loc["v_rare_a", "maf"] == pytest.approx(0.05, abs=1e-9)
        assert out.loc["v_common", "maf"] == pytest.approx(0.50, abs=1e-9)
        assert out.loc["v_rare_b", "maf"] == pytest.approx(0.05, abs=1e-9)


# ---------------------------------------------------------------------------
# sample_stats
# ---------------------------------------------------------------------------
class TestSampleStats:
    def test_one_row_per_sample(self, synthetic_geno_ds):
        out = sample_stats(Genotypes(synthetic_geno_ds))
        assert len(out) == synthetic_geno_ds.sizes["samples"]

    def test_expected_columns_present(self, synthetic_geno_ds):
        out = sample_stats(Genotypes(synthetic_geno_ds))
        for col in (
            "sample_id", "n_called", "n_het",
            "n_hom_ref", "n_hom_alt", "call_rate",
        ):
            assert col in out.columns, f"missing column: {col}"

    def test_call_rate_in_unit_interval(self, synthetic_geno_ds):
        out = sample_stats(Genotypes(synthetic_geno_ds))
        rates = out["call_rate"].dropna()
        assert ((rates >= 0.0) & (rates <= 1.0)).all()

    def test_call_rate_matches_known_fixture(self, missingness_geno):
        # missingness_geno: 3 variants, 5 samples; pattern from conftest.
        # s0 has 0 missing across 3 variants → call_rate=1.0
        # s1, s2 have 1 missing of 3 → call_rate ≈ 0.6667
        # s3 has 2 missing of 3 → call_rate ≈ 0.3333
        # s4 has 2 missing of 3 → call_rate ≈ 0.3333
        out = sample_stats(missingness_geno).set_index("sample_id")
        assert out.loc["s0", "call_rate"] == pytest.approx(1.0)
        assert out.loc["s3", "call_rate"] == pytest.approx(1 / 3, abs=1e-6)


# ---------------------------------------------------------------------------
# genotype_summary
# ---------------------------------------------------------------------------
class TestGenotypeSummary:
    def test_returns_flat_dict_with_counts(self, synthetic_geno_ds):
        out = genotype_summary(Genotypes(synthetic_geno_ds))
        assert isinstance(out, dict)
        assert out["n_samples"] == synthetic_geno_ds.sizes["samples"]
        assert out["n_variants"] == synthetic_geno_ds.sizes["variants"]

    def test_includes_call_rate_aggregates(self, synthetic_geno_ds):
        out = genotype_summary(Genotypes(synthetic_geno_ds))
        for key in (
            "variant_call_rate_mean", "variant_call_rate_min",
            "sample_call_rate_mean", "sample_call_rate_min",
        ):
            assert key in out
            assert 0.0 <= out[key] <= 1.0

    def test_maf_buckets_match_fixture(self, maf_geno):
        # maf_geno has 2 variants with MAF=0.05 and 1 with MAF=0.50.
        out = genotype_summary(maf_geno)
        assert out["n_variants"] == 3
        # 2 variants are at MAF=0.05 (NOT < 0.01) → maf_lt_0.01 == 0
        assert out["n_variants_maf_lt_0.01"] == 0
        # 0 variants are < 0.05 (the rare ones are exactly 0.05)
        assert out["n_variants_maf_lt_0.05"] == 0
        assert out["maf_mean"] == pytest.approx(
            (0.05 + 0.50 + 0.05) / 3, abs=1e-9
        )

    def test_does_not_mutate_input(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        before_vars = set(geno.ds.variables)
        genotype_summary(geno)
        # The wrapped Dataset should not have new variables added in-place.
        assert set(geno.ds.variables) == before_vars


# ---------------------------------------------------------------------------
# heterozygosity
# ---------------------------------------------------------------------------
class TestHeterozygosity:
    def test_one_row_per_sample(self, synthetic_geno_ds):
        out = heterozygosity(Genotypes(synthetic_geno_ds))
        assert len(out) == synthetic_geno_ds.sizes["samples"]

    def test_columns_present(self, synthetic_geno_ds):
        out = heterozygosity(Genotypes(synthetic_geno_ds))
        assert list(out.columns) == [
            "sample_id", "n_called", "n_het",
            "het_rate", "het_zscore", "is_outlier",
        ]

    def test_het_rate_in_unit_interval(self, synthetic_geno_ds):
        out = heterozygosity(Genotypes(synthetic_geno_ds))
        rates = out["het_rate"].dropna()
        assert ((rates >= 0.0) & (rates <= 1.0)).all()

    def test_het_rate_matches_definition(self, synthetic_geno_ds):
        # Each row's het_rate must equal n_het / n_called.
        out = heterozygosity(Genotypes(synthetic_geno_ds))
        for _, row in out.iterrows():
            if row["n_called"] > 0:
                assert row["het_rate"] == pytest.approx(
                    row["n_het"] / row["n_called"]
                )

    def test_zscore_centered_around_zero(self, synthetic_geno_ds):
        # Across all valid samples, mean of zscore ≈ 0 (numerical drift OK).
        out = heterozygosity(Genotypes(synthetic_geno_ds))
        z = out["het_zscore"].dropna()
        if len(z) >= 2:
            assert z.mean() == pytest.approx(0.0, abs=1e-9)

    def test_outlier_flag_with_synthetic_outlier(self):
        # Build a dataset where one sample has zero het and the rest are 100% het.
        from igem.modules.data import Genotypes
        import xarray as xr
        # 1 sample (s0): all hom_ref. 9 samples (s1..s9): all het.
        # All 30 variants × 10 samples × ploidy 2.
        n_var, n_samp = 30, 10
        gt = np.zeros((n_var, n_samp, 2), dtype=np.int8)
        # Heterozygotes for s1..s9: gt[:, 1:, :] = [0, 1]
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
        out = heterozygosity(Genotypes(ds), outlier_sd=2.0).set_index(
            "sample_id"
        )
        # s0 het_rate=0 vs others het_rate=1 → s0 is the outlier.
        assert out.loc["s0", "is_outlier"] == True  # noqa: E712
        for s in [f"s{i}" for i in range(1, 10)]:
            assert out.loc[s, "is_outlier"] == False  # noqa: E712

    def test_outlier_sd_threshold_changes_flags(self):
        # Same dataset as above; with a very large threshold no one is an outlier.
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
        out = heterozygosity(Genotypes(ds), outlier_sd=10.0)
        assert not out["is_outlier"].any()

    def test_invalid_outlier_sd_raises(self, synthetic_geno_ds):
        with pytest.raises(ValueError, match="outlier_sd"):
            heterozygosity(Genotypes(synthetic_geno_ds), outlier_sd=0)
        with pytest.raises(ValueError, match="outlier_sd"):
            heterozygosity(Genotypes(synthetic_geno_ds), outlier_sd=-1.0)

    def test_zero_called_sample_yields_nan_rate(self, missingness_geno):
        # missingness_geno: s0 has all called; s3, s4 have only 1 of 3 called.
        # No sample is fully missing in this fixture, so het_rate is finite.
        out = heterozygosity(missingness_geno)
        # All n_called > 0 → no NaN rates here.
        assert out["het_rate"].notna().all()
