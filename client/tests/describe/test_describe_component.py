"""
Smoke tests for the IGEM facade exposure of the describe module.

These tests do **not** retest the logic of the underlying functions
(that is covered in test_phenotype_describe.py and
test_genotype_describe.py). They verify that:

  1. Every public function in ``igem.modules.describe`` is reachable
     via ``igem.describe.<method>``.
  2. Keyword arguments propagate correctly from the facade to the
     free function.
  3. The facade returns the same result as calling the free function
     directly with the same arguments.

If a future change drops a kwarg from the component or breaks the
delegation, these smoke tests catch it before the user does.
"""
from __future__ import annotations

import pandas as pd
import pytest

from igem import IGEM
from igem.modules.data import Genotypes, Phenotypes
from igem.modules import describe as _free


# ---------------------------------------------------------------------------
# Phenotype methods (silent — facade delegates without logging)
# ---------------------------------------------------------------------------
class TestPhenotypeFacade:
    def test_summarize_unweighted(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.summarize(describe_phen)
        free = _free.summarize(describe_phen)
        pd.testing.assert_frame_equal(facade, free)

    def test_summarize_weighted_propagates(self):
        # weighted=True requires weights_col on the wrapper.
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(5)],
                "X":  [1.0, 2.0, 3.0, 4.0, 5.0],
                "WT": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id", weights_col="WT")
        with IGEM() as igem:
            facade = igem.describe.summarize(phen, weighted=True)
        free = _free.summarize(phen, weighted=True)
        pd.testing.assert_frame_equal(facade, free)

    def test_summarize_cols_propagates(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.summarize(describe_phen, cols=["AGE", "BMI"])
        free = _free.summarize(describe_phen, cols=["AGE", "BMI"])
        pd.testing.assert_frame_equal(facade, free)

    def test_summarize_by(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.summarize_by(describe_phen, by="SEX")
        free = _free.summarize_by(describe_phen, by="SEX")
        pd.testing.assert_frame_equal(facade, free)

    def test_summarize_by_dropna_group_propagates(self):
        import numpy as np
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(6)],
                "GROUP": ["A", "A", "B", "B", np.nan, np.nan],
                "X": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        with IGEM() as igem:
            facade = igem.describe.summarize_by(
                phen, by="GROUP", dropna_group=False
            )
        free = _free.summarize_by(phen, by="GROUP", dropna_group=False)
        pd.testing.assert_frame_equal(facade, free)

    def test_dataset_summary(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.dataset_summary(describe_phen)
        free = _free.dataset_summary(describe_phen)
        assert facade == free

    def test_missing_report(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.missing_report(describe_phen)
        free = _free.missing_report(describe_phen)
        pd.testing.assert_frame_equal(facade, free)

    def test_correlation_matrix_method_propagates(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.correlation_matrix(
                describe_phen, method="spearman"
            )
        free = _free.correlation_matrix(describe_phen, method="spearman")
        pd.testing.assert_frame_equal(facade, free)

    def test_correlation_pairs_threshold_propagates(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.correlation_pairs(
                describe_phen, threshold=0.0, absolute=False
            )
        free = _free.correlation_pairs(
            describe_phen, threshold=0.0, absolute=False
        )
        pd.testing.assert_frame_equal(facade, free)

    def test_crosstab(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.crosstab(describe_phen, "SEX", "STUDY")
        free = _free.crosstab(describe_phen, "SEX", "STUDY")
        pd.testing.assert_frame_equal(facade, free)

    def test_crosstab_margins_propagates(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.crosstab(
                describe_phen, "SEX", "STUDY", margins=True
            )
        free = _free.crosstab(
            describe_phen, "SEX", "STUDY", margins=True
        )
        pd.testing.assert_frame_equal(facade, free)

    def test_value_counts(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.value_counts(
                describe_phen, cols=["SEX"], top=2
            )
        free = _free.value_counts(describe_phen, cols=["SEX"], top=2)
        # value_counts returns dict[str, DataFrame] — compare per key.
        assert set(facade.keys()) == set(free.keys())
        for key in facade:
            pd.testing.assert_frame_equal(facade[key], free[key])

    def test_skewness_dropna_propagates(self, describe_phen):
        with IGEM() as igem:
            facade = igem.describe.skewness(describe_phen, dropna=True)
        free = _free.skewness(describe_phen, dropna=True)
        pd.testing.assert_frame_equal(facade, free)


# ---------------------------------------------------------------------------
# Genotype methods (logged — facade emits header/footer)
# ---------------------------------------------------------------------------
class TestGenotypeFacade:
    def test_variant_stats(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        with IGEM() as igem:
            facade = igem.describe.variant_stats(geno)
        free = _free.variant_stats(geno)
        pd.testing.assert_frame_equal(facade, free)

    def test_sample_stats(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        with IGEM() as igem:
            facade = igem.describe.sample_stats(geno)
        free = _free.sample_stats(geno)
        pd.testing.assert_frame_equal(facade, free)

    def test_heterozygosity(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        with IGEM() as igem:
            facade = igem.describe.heterozygosity(geno)
        free = _free.heterozygosity(geno)
        pd.testing.assert_frame_equal(facade, free)

    def test_heterozygosity_outlier_sd_propagates(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        with IGEM() as igem:
            facade = igem.describe.heterozygosity(geno, outlier_sd=2.0)
        free = _free.heterozygosity(geno, outlier_sd=2.0)
        pd.testing.assert_frame_equal(facade, free)

    def test_genotype_summary(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        with IGEM() as igem:
            facade = igem.describe.genotype_summary(geno)
        free = _free.genotype_summary(geno)
        assert facade == free


# ---------------------------------------------------------------------------
# Coverage guard — every public function must be reachable via the facade
# ---------------------------------------------------------------------------
class TestFacadeCoverage:
    """
    If a new function is added to ``igem.modules.describe.__all__`` but
    not wired into ``DescribeComponent``, this test fails. Future-proofs
    the facade against silent omissions.
    """

    def test_every_public_function_is_on_the_facade(self):
        from igem.modules.describe import __all__ as public
        with IGEM() as igem:
            missing = [
                name for name in public
                if not hasattr(igem.describe, name)
            ]
        assert missing == [], (
            f"Functions exposed by igem.modules.describe but missing "
            f"from DescribeComponent: {missing}"
        )
