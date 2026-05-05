"""Tests for igem.modules.analyze.gwas (sgkit linear GWAS)."""
from __future__ import annotations

import numpy as np
import pytest

from igem.modules.analyze import RegressionResults, gwas
from igem.modules.data import Phenotypes


class TestGwasLinear:
    def test_returns_regression_results(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        res = gwas(geno, phen, "GLUCOSE")
        assert isinstance(res, RegressionResults)
        assert res.family == "linear"

    def test_one_row_per_variant(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        res = gwas(geno, phen, "GLUCOSE")
        assert res.n_tests == geno.n_variants
        assert set(res.df["variable"]) == set(
            np.asarray(geno.ds["variant_id"].values).astype(str)
        )

    def test_causal_variant_is_significant(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        res = gwas(geno, phen, "GLUCOSE")
        d = res.df.set_index("variable")
        # Variant rs00002 was injected with effect 4.0 → highly significant.
        assert d.loc["rs00002", "p_value"] < 1e-10
        # Other variants should not all be at < 0.01 (they're noise).
        non_causal = d.drop("rs00002")
        assert (non_causal["p_value"] >= 0.01).any()

    def test_beta_close_to_simulated(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        res = gwas(geno, phen, "GLUCOSE")
        d = res.df.set_index("variable")
        assert d.loc["rs00002", "beta"] == pytest.approx(4.0, abs=0.6)

    def test_ci_brackets_beta(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        res = gwas(geno, phen, "GLUCOSE")
        for _, row in res.df.iterrows():
            assert row["ci_low"] <= row["beta"] <= row["ci_high"]

    def test_alignment_drops_geno_only_samples(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        res = gwas(geno, phen, "GLUCOSE")
        # Phen has 200 samples; geno has 205 — overlap is 200.
        assert int(res.df["n"].iloc[0]) == 200
        assert res.metadata["n_samples_used"] == 200
        assert res.metadata["backend"] == "sgkit.gwas_linear_regression"

    def test_default_covariates_from_phen(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        res = gwas(geno, phen, "GLUCOSE")
        assert res.covariates == ["AGE"]

    def test_explicit_no_covariates(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        res = gwas(geno, phen, "GLUCOSE", covariates=[])
        assert res.covariates == []

    def test_chains_with_correction_and_top(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        out = (
            gwas(geno, phen, "GLUCOSE")
            .with_correction("bonferroni")
            .top(3)
        )
        assert "rs00002" in out.df["variable"].tolist()


class TestGwasValidation:
    def test_unknown_outcome_raises(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        with pytest.raises(ValueError, match="outcome"):
            gwas(geno, phen, "not_a_col")

    def test_unknown_covariate_raises(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        with pytest.raises(ValueError, match="covariates"):
            gwas(geno, phen, "GLUCOSE", covariates=["NOPE"])

    def test_logistic_not_implemented(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        with pytest.raises(NotImplementedError, match="logistic"):
            gwas(geno, phen, "GLUCOSE", family="logistic")

    def test_no_overlap_raises(self, gwas_geno_and_phen):
        import pandas as pd
        geno, phen = gwas_geno_and_phen
        bad_df = phen.df.copy()
        bad_df["sample_id"] = [f"X{i}" for i in range(len(bad_df))]
        bad_phen = Phenotypes(
            bad_df,
            sample_id_col="sample_id",
            outcomes=["GLUCOSE"], covariates=["AGE"],
        )
        # Wrap pd to silence flake (unused-import) without touching deps.
        assert isinstance(bad_phen.df, pd.DataFrame)
        with pytest.raises(ValueError, match="overlapping"):
            gwas(geno, bad_phen, "GLUCOSE")
