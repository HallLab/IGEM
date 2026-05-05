"""Tests for igem.modules.analyze.lrt."""
from __future__ import annotations

import pytest

from igem.modules.analyze import lrt


# ---------------------------------------------------------------------------
# Linear LRT
# ---------------------------------------------------------------------------
class TestLrtLinear:
    def test_returns_expected_keys(self, linear_phen):
        out = lrt(
            linear_phen, "GLUCOSE",
            full=["AGE", "SEX", "BMI"],
            nested=["AGE", "SEX"],
        )
        assert set(out.keys()) == {
            "chi2", "df", "p_value",
            "ll_full", "ll_nested", "n",
        }

    def test_significant_when_full_adds_real_predictor(self, linear_phen):
        # BMI was simulated with effect 2.0 → adding it should be highly
        # significant.
        out = lrt(
            linear_phen, "GLUCOSE",
            full=["AGE", "SEX", "BMI"],
            nested=["AGE", "SEX"],
        )
        assert out["chi2"] > 50
        assert out["df"] == 1
        assert out["p_value"] < 1e-10

    def test_non_significant_when_full_adds_noise(self, linear_phen):
        # Adding a null exposure should not improve fit substantially.
        out = lrt(
            linear_phen, "GLUCOSE",
            full=["AGE", "SEX", "EXP_NULL_A"],
            nested=["AGE", "SEX"],
        )
        assert out["p_value"] > 0.05


# ---------------------------------------------------------------------------
# Logistic LRT
# ---------------------------------------------------------------------------
class TestLrtLogistic:
    def test_logistic_family_inferred(self, logistic_phen):
        out = lrt(
            logistic_phen, "DIABETES",
            full=["AGE", "BMI"],
            nested=["AGE"],
        )
        assert out["df"] == 1
        # BMI was injected into the log-odds → significant at n=200.
        assert out["p_value"] < 0.05


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class TestLrtValidation:
    def test_unknown_outcome_raises(self, linear_phen):
        with pytest.raises(ValueError, match="outcome"):
            lrt(
                linear_phen, "not_a_col",
                full=["AGE"], nested=[],
            )

    def test_unknown_full_column_raises(self, linear_phen):
        with pytest.raises(ValueError, match="full"):
            lrt(
                linear_phen, "GLUCOSE",
                full=["AGE", "NOPE"], nested=["AGE"],
            )

    def test_unknown_nested_column_raises(self, linear_phen):
        with pytest.raises(ValueError, match="nested"):
            lrt(
                linear_phen, "GLUCOSE",
                full=["AGE", "BMI"], nested=["NOPE"],
            )

    def test_nested_must_subset_full(self, linear_phen):
        with pytest.raises(ValueError, match="subset"):
            lrt(
                linear_phen, "GLUCOSE",
                full=["AGE"], nested=["BMI"],
            )

    def test_same_models_raises(self, linear_phen):
        with pytest.raises(ValueError, match="extra"):
            lrt(
                linear_phen, "GLUCOSE",
                full=["AGE", "BMI"], nested=["AGE", "BMI"],
            )

    def test_two_extra_terms_gives_df_two(self, linear_phen):
        out = lrt(
            linear_phen, "GLUCOSE",
            full=["AGE", "SEX", "BMI"],
            nested=["AGE"],
        )
        assert out["df"] == 2
