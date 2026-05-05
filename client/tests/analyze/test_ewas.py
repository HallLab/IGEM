"""Tests for igem.modules.analyze.ewas."""
from __future__ import annotations

import pytest

from igem.modules.analyze import RegressionResults, ewas


# ---------------------------------------------------------------------------
# Linear EWAS
# ---------------------------------------------------------------------------
class TestEwasLinear:
    def test_returns_regression_results(self, linear_phen):
        res = ewas(linear_phen, "GLUCOSE", progress=False)
        assert isinstance(res, RegressionResults)
        assert res.family == "linear"

    def test_uses_phen_exposures_by_default(self, linear_phen):
        res = ewas(linear_phen, "GLUCOSE", progress=False)
        assert set(res.df["variable"]) == {
            "BMI", "EXP_NULL_A", "EXP_NULL_B"
        }

    def test_bmi_is_significant_null_are_not(self, linear_phen):
        res = ewas(linear_phen, "GLUCOSE", progress=False)
        d = res.df.set_index("variable")
        # BMI was simulated with effect 2.0 → strong signal at n=200.
        assert d.loc["BMI", "p_value"] < 1e-10
        # null exposures should not pass at any reasonable threshold.
        assert d.loc["EXP_NULL_A", "p_value"] > 0.01
        assert d.loc["EXP_NULL_B", "p_value"] > 0.01

    def test_beta_for_bmi_close_to_simulated(self, linear_phen):
        res = ewas(linear_phen, "GLUCOSE", progress=False)
        d = res.df.set_index("variable")
        # Note: marginal beta (no AGE/SEX in this row only adjustment).
        # With AGE+SEX as covariates, BMI's adjusted beta should be near 2.0.
        assert d.loc["BMI", "beta"] == pytest.approx(2.0, abs=0.5)

    def test_ci_brackets_beta(self, linear_phen):
        res = ewas(linear_phen, "GLUCOSE", progress=False)
        for _, row in res.df.iterrows():
            assert row["ci_low"] <= row["beta"] <= row["ci_high"]

    def test_explicit_exposures_overrides_phen(self, linear_phen):
        res = ewas(
            linear_phen, "GLUCOSE",
            exposures=["BMI"], progress=False,
        )
        assert list(res.df["variable"]) == ["BMI"]

    def test_no_covariates(self, linear_phen):
        res = ewas(
            linear_phen, "GLUCOSE",
            covariates=[], progress=False,
        )
        assert "AGE" not in res.formula_template


# ---------------------------------------------------------------------------
# Logistic EWAS
# ---------------------------------------------------------------------------
class TestEwasLogistic:
    def test_auto_detects_logistic(self, logistic_phen):
        res = ewas(logistic_phen, "DIABETES", progress=False)
        assert res.family == "logistic"

    def test_bmi_significant(self, logistic_phen):
        res = ewas(logistic_phen, "DIABETES", progress=False)
        d = res.df.set_index("variable")
        # BMI was injected into the log-odds → expect p<0.01 at n=200.
        assert d.loc["BMI", "p_value"] < 0.05
        assert d.loc["EXP_NULL_A", "p_value"] > 0.05

    def test_explicit_family_override(self, logistic_phen):
        # Overriding to linear on a binary outcome should still run
        # (statsmodels OLS doesn't refuse) — the family field reflects
        # the override.
        res = ewas(
            logistic_phen, "DIABETES",
            family="linear", progress=False,
        )
        assert res.family == "linear"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class TestEwasValidation:
    def test_unknown_outcome_raises(self, linear_phen):
        with pytest.raises(ValueError, match="not_a_col"):
            ewas(linear_phen, "not_a_col", progress=False)

    def test_no_exposures_raises(self, linear_phen):
        with pytest.raises(ValueError, match="no exposures"):
            ewas(
                linear_phen, "GLUCOSE",
                exposures=[], progress=False,
            )

    def test_unknown_exposure_raises(self, linear_phen):
        with pytest.raises(ValueError, match="missing"):
            ewas(
                linear_phen, "GLUCOSE",
                exposures=["not_real"], progress=False,
            )


# ---------------------------------------------------------------------------
# Error handling (constant / collinear exposure)
# ---------------------------------------------------------------------------
class TestEwasErrors:
    def test_unfittable_exposure_lands_in_errors(self, errors_phen):
        res = ewas(errors_phen, "GLUCOSE", progress=False)
        # BMI should run; ALL_NAN_EXP should fail (no rows after dropna).
        assert "BMI" in res.df["variable"].tolist()
        assert "ALL_NAN_EXP" in res.errors["variable"].tolist()
        assert res.n_tests == 1
        assert res.n_errors == 1

    def test_loop_does_not_crash_on_failed_regression(self, errors_phen):
        # Single failure does not derail the run for other variables.
        res = ewas(errors_phen, "GLUCOSE", progress=False)
        assert isinstance(res, RegressionResults)


# ---------------------------------------------------------------------------
# Result usage chain
# ---------------------------------------------------------------------------
class TestEwasChainable:
    def test_with_correction_passing_top(self, linear_phen):
        out = (
            ewas(linear_phen, "GLUCOSE", progress=False)
            .with_correction("bonferroni")
            .passing(p_corrected=0.05)
            .top(5)
        )
        # BMI should make it through Bonferroni at n=3 tests.
        assert "BMI" in out.df["variable"].tolist()
