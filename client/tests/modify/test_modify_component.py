"""
Smoke tests for the IGEM facade exposure of the modify module.

Same pattern as ``tests/describe/test_describe_component.py`` and
documented in caderno
``docs/caderno/2026-05-08__003_Convenção de testes de fachada para componentes IGEM.md``:

  1. ``TestModifyFacadePhenotype`` — one test per phenotype method,
     comparing facade output to free-function output.
  2. ``TestModifyFacadeGenotype`` — same for genotype filters.
  3. ``TestFacadeCoverage`` — automatic guard ensuring every name in
     ``igem.modules.modify.__all__`` is reachable on
     ``igem.modify.<name>``.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem import IGEM
from igem.modules.data import Genotypes, Phenotypes
from igem.modules import modify as _free


# ---------------------------------------------------------------------------
# Phenotype methods
# ---------------------------------------------------------------------------
class TestModifyFacadePhenotype:
    def test_discretize(self, transform_phen):
        with IGEM() as igem:
            facade = igem.modify.discretize(
                transform_phen, "BMI", method="quantiles", n_bins=2,
            )
        free = _free.discretize(
            transform_phen, "BMI", method="quantiles", n_bins=2,
        )
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_recode(self, transform_phen):
        with IGEM() as igem:
            facade = igem.modify.recode(
                transform_phen, "SEX", {1: "M", 2: "F"},
                missing_values=[99],
            )
        free = _free.recode(
            transform_phen, "SEX", {1: "M", 2: "F"},
            missing_values=[99],
        )
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_drop_missing(self, transform_phen):
        with IGEM() as igem:
            facade = igem.modify.drop_missing(transform_phen)
        free = _free.drop_missing(transform_phen)
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_transform_method_string(self, transform_phen):
        with IGEM() as igem:
            facade = igem.modify.transform(
                transform_phen, "BMI", method="log",
            )
        free = _free.transform(transform_phen, "BMI", method="log")
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_transform_func_callable(self, transform_phen):
        with IGEM() as igem:
            facade = igem.modify.transform(
                transform_phen, "BMI", func=lambda s: s * 2,
            )
        free = _free.transform(
            transform_phen, "BMI", func=lambda s: s * 2,
        )
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_remove_outliers_propagates_method(self):
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(11)],
                "X": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
                      17.0, 18.0, 19.0, 1000.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        with IGEM() as igem:
            facade = igem.modify.remove_outliers(
                phen, cols=["X"], method="gaussian", cutoff=2.0,
            )
        free = _free.remove_outliers(
            phen, cols=["X"], method="gaussian", cutoff=2.0,
        )
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_auto_classify(self, transform_phen):
        with IGEM() as igem:
            facade = igem.modify.auto_classify(transform_phen)
        free = _free.auto_classify(transform_phen)
        pd.testing.assert_frame_equal(facade, free)

    def test_make_binary(self):
        df = pd.DataFrame(
            {"sample_id": ["A", "B", "C", "D"], "CASE": [0, 1, 0, 1]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        with IGEM() as igem:
            facade = igem.modify.make_binary(phen, only=["CASE"])
        free = _free.make_binary(phen, only=["CASE"])
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_make_categorical(self, transform_phen):
        with IGEM() as igem:
            facade = igem.modify.make_categorical(
                transform_phen, only=["SEX"],
            )
        free = _free.make_categorical(transform_phen, only=["SEX"])
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_make_continuous(self):
        df = pd.DataFrame(
            {"sample_id": ["A", "B"], "X": ["1.5", "2.5"]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        with IGEM() as igem:
            facade = igem.modify.make_continuous(phen, only=["X"])
        free = _free.make_continuous(phen, only=["X"])
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_colfilter(self, transform_phen):
        with IGEM() as igem:
            facade = igem.modify.colfilter(transform_phen, only=["BMI"])
        free = _free.colfilter(transform_phen, only=["BMI"])
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_colfilter_min_n(self):
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "dense": list(range(10)),
                "sparse": [1.0] + [np.nan] * 9,
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        with IGEM() as igem:
            facade = igem.modify.colfilter_min_n(phen, n=5)
        free = _free.colfilter_min_n(phen, n=5)
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_colfilter_min_cat_n(self):
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "balanced": [0, 1] * 5,
                "unbalanced": [0] * 9 + [1],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        with IGEM() as igem:
            facade = igem.modify.colfilter_min_cat_n(phen, n=3)
        free = _free.colfilter_min_cat_n(phen, n=3)
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_colfilter_percent_zero(self):
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "many_zero": [0.0] * 9 + [5.0],
                "no_zero": list(range(1, 11)),
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        with IGEM() as igem:
            facade = igem.modify.colfilter_percent_zero(
                phen, max_zero_pct=90.0,
            )
        free = _free.colfilter_percent_zero(phen, max_zero_pct=90.0)
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_merge_observations(self):
        top = Phenotypes(
            pd.DataFrame(
                {"sample_id": ["A", "B"], "BMI": [22.0, 28.0]}
            ),
            sample_id_col="sample_id",
        )
        bottom = Phenotypes(
            pd.DataFrame(
                {"sample_id": ["C", "D"], "BMI": [24.0, 30.0]}
            ),
            sample_id_col="sample_id",
        )
        with IGEM() as igem:
            facade = igem.modify.merge_observations(top, bottom)
        free = _free.merge_observations(top, bottom)
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_merge_variables(self):
        left = Phenotypes(
            pd.DataFrame(
                {"sample_id": ["A", "B"], "BMI": [22.0, 28.0]}
            ),
            sample_id_col="sample_id",
        )
        right = Phenotypes(
            pd.DataFrame(
                {"sample_id": ["B", "C"], "GLUCOSE": [95.0, 110.0]}
            ),
            sample_id_col="sample_id",
        )
        with IGEM() as igem:
            facade = igem.modify.merge_variables(left, right, how="inner")
        free = _free.merge_variables(left, right, how="inner")
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_rowfilter_incomplete_obs(self, transform_phen):
        with IGEM() as igem:
            facade = igem.modify.rowfilter_incomplete_obs(transform_phen)
        free = _free.rowfilter_incomplete_obs(transform_phen)
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_move_variables(self):
        from igem.modules.data import Phenotypes
        ids = [f"S{i}" for i in range(3)]
        src = Phenotypes(
            pd.DataFrame(
                {"sample_id": ids, "BMI": [22.0, 25.0, 28.0],
                 "EXTRA": [1, 2, 3]}
            ),
            sample_id_col="sample_id",
            outcomes=["BMI"],
        )
        dst = Phenotypes(
            pd.DataFrame({"sample_id": ids, "AGE": [40, 45, 50]}),
            sample_id_col="sample_id",
        )
        with IGEM() as igem:
            new_src_f, new_dst_f = igem.modify.move_variables(
                src, dst, only=["EXTRA"],
            )
        new_src_n, new_dst_n = _free.move_variables(
            src, dst, only=["EXTRA"],
        )
        pd.testing.assert_frame_equal(new_src_f.df, new_src_n.df)
        pd.testing.assert_frame_equal(new_dst_f.df, new_dst_n.df)


# ---------------------------------------------------------------------------
# Genotype methods
# ---------------------------------------------------------------------------
class TestModifyFacadeGenotype:
    def test_filter_biallelic(self, multiallelic_geno):
        with IGEM() as igem:
            facade = igem.modify.filter_biallelic(multiallelic_geno)
        free = _free.filter_biallelic(multiallelic_geno)
        assert facade.n_variants == free.n_variants

    def test_filter_maf(self, maf_geno):
        with IGEM() as igem:
            facade = igem.modify.filter_maf(maf_geno, min_maf=0.10)
        free = _free.filter_maf(maf_geno, min_maf=0.10)
        assert facade.n_variants == free.n_variants

    def test_filter_missingness_variants(self, missingness_geno):
        with IGEM() as igem:
            facade = igem.modify.filter_missingness_variants(
                missingness_geno, max_missing=0.20,
            )
        free = _free.filter_missingness_variants(
            missingness_geno, max_missing=0.20,
        )
        assert facade.n_variants == free.n_variants

    def test_filter_missingness_samples(self, missingness_geno):
        with IGEM() as igem:
            facade = igem.modify.filter_missingness_samples(
                missingness_geno, max_missing=0.50,
            )
        free = _free.filter_missingness_samples(
            missingness_geno, max_missing=0.50,
        )
        assert facade.n_samples == free.n_samples

    def test_filter_hwe(self, hwe_geno):
        with IGEM() as igem:
            facade = igem.modify.filter_hwe(hwe_geno, min_pvalue=1e-3)
        free = _free.filter_hwe(hwe_geno, min_pvalue=1e-3)
        assert facade.n_variants == free.n_variants

    def test_filter_heterozygosity_outliers(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        with IGEM() as igem:
            facade = igem.modify.filter_heterozygosity_outliers(
                geno, outlier_sd=2.0,
            )
        free = _free.filter_heterozygosity_outliers(geno, outlier_sd=2.0)
        assert facade.n_samples == free.n_samples

    def test_keep_snvs_only(self, indel_geno):
        with IGEM() as igem:
            facade = igem.modify.keep_snvs_only(indel_geno)
        free = _free.keep_snvs_only(indel_geno)
        assert facade.n_variants == free.n_variants

    def test_filter_variants(self, maf_geno):
        with IGEM() as igem:
            facade = igem.modify.filter_variants(
                maf_geno, ["v_common"], keep=True,
            )
        free = _free.filter_variants(maf_geno, ["v_common"], keep=True)
        assert facade.n_variants == free.n_variants

    def test_filter_by_region(self, indel_geno):
        with IGEM() as igem:
            facade = igem.modify.filter_by_region(
                indel_geno, "1", start=150, end=250,
            )
        free = _free.filter_by_region(
            indel_geno, "1", start=150, end=250,
        )
        assert facade.n_variants == free.n_variants

    def test_filter_samples_geno(self, missingness_geno):
        with IGEM() as igem:
            facade = igem.modify.filter_samples(
                missingness_geno, ["s0", "s1"],
            )
        free = _free.filter_samples(missingness_geno, ["s0", "s1"])
        assert facade.n_samples == free.n_samples

    def test_filter_samples_phen(self, transform_phen):
        with IGEM() as igem:
            facade = igem.modify.filter_samples(
                transform_phen, ["S0", "S2"],
            )
        free = _free.filter_samples(transform_phen, ["S0", "S2"])
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_prune_ld(self, ld_geno):
        with IGEM() as igem:
            facade = igem.modify.prune_ld(
                ld_geno, window=4, step=4, r2=0.5,
            )
        free = _free.prune_ld(ld_geno, window=4, step=4, r2=0.5)
        assert facade.n_variants == free.n_variants


# ---------------------------------------------------------------------------
# Coverage guard — every public function reachable via the facade
# ---------------------------------------------------------------------------
class TestFacadeCoverage:
    def test_every_public_function_is_on_the_facade(self):
        from igem.modules.modify import __all__ as public
        with IGEM() as igem:
            missing = [
                name for name in public
                if not hasattr(igem.modify, name)
            ]
        assert missing == [], (
            f"Functions exposed by igem.modules.modify but missing "
            f"from ModifyComponent: {missing}"
        )
