"""Tests for igem.modules.analyze.results.RegressionResults."""
from __future__ import annotations

import pandas as pd
import pytest

from igem.modules.analyze import RegressionResults


def _make_result() -> RegressionResults:
    df = pd.DataFrame(
        {
            "variable": ["a", "b", "c", "d"],
            "n": [100, 100, 100, 100],
            "beta": [0.5, 0.1, 0.3, 0.0],
            "se": [0.1, 0.05, 0.05, 0.5],
            "ci_low": [0.3, 0.0, 0.2, -1.0],
            "ci_high": [0.7, 0.2, 0.4, 1.0],
            "p_value": [0.0001, 0.04, 0.001, 0.95],
        }
    )
    return RegressionResults(
        df=df,
        family="linear",
        outcome="y",
        covariates=["age"],
        formula_template="y ~ {exposure} + age",
        errors=pd.DataFrame(columns=["variable", "error"]),
        metadata={},
    )


class TestRegressionResultsBasics:
    def test_n_tests_and_n_errors(self):
        res = _make_result()
        assert res.n_tests == 4
        assert res.n_errors == 0

    def test_to_dataframe_returns_copy(self):
        res = _make_result()
        df = res.to_dataframe()
        df.loc[0, "beta"] = 9999
        assert res.df.loc[0, "beta"] == 0.5

    def test_to_csv(self, tmp_path):
        res = _make_result()
        out = res.to_csv(tmp_path / "out.csv")
        assert out.exists()
        loaded = pd.read_csv(out)
        assert len(loaded) == 4

    def test_repr_includes_family_and_n(self):
        text = repr(_make_result())
        assert "linear" in text
        assert "n_tests=4" in text


class TestWithCorrection:
    def test_adds_p_corrected_column(self):
        res = _make_result().with_correction("bonferroni")
        assert "p_corrected" in res.df.columns
        # Bonferroni: p * 4, capped at 1.0
        assert res.df.loc[0, "p_corrected"] == pytest.approx(0.0004)

    def test_method_stored_in_metadata(self):
        res = _make_result().with_correction("fdr_bh")
        assert res.correction_method == "fdr_bh"

    def test_overwrites_when_called_twice(self):
        res = _make_result().with_correction("bonferroni")
        res = res.with_correction("fdr_bh")
        assert res.correction_method == "fdr_bh"
        # Bonferroni column should not be present anymore (it was named
        # p_corrected and got overwritten by the new method).
        assert "p_corrected" in res.df.columns

    def test_raises_when_no_p_value(self):
        res = _make_result()
        bad = pd.DataFrame({"variable": ["a"]})
        from dataclasses import replace
        broken = replace(res, df=bad)
        with pytest.raises(ValueError, match="p_value"):
            broken.with_correction("bonferroni")


class TestPassing:
    def test_filter_by_p(self):
        res = _make_result().passing(p=0.01)
        assert set(res.df["variable"]) == {"a", "c"}

    def test_filter_by_p_corrected(self):
        res = _make_result().with_correction("bonferroni")
        out = res.passing(p_corrected=0.01)
        # After bonferroni: 0.0004, 0.16, 0.004, 1.0 → only a and c pass
        assert set(out.df["variable"]) == {"a", "c"}

    def test_combined_thresholds(self):
        res = _make_result().with_correction("bonferroni")
        out = res.passing(p=0.01, p_corrected=0.01)
        # both: variable's p<0.01 AND p_corrected<0.01
        assert set(out.df["variable"]) == {"a", "c"}

    def test_requires_at_least_one_threshold(self):
        with pytest.raises(ValueError, match="passing"):
            _make_result().passing()

    def test_p_corrected_without_correction_raises(self):
        with pytest.raises(ValueError, match="with_correction"):
            _make_result().passing(p_corrected=0.01)


class TestTop:
    def test_default_sorts_by_p_value_ascending(self):
        out = _make_result().top(n=2)
        assert list(out.df["variable"]) == ["a", "c"]

    def test_custom_sort_column(self):
        out = _make_result().top(n=1, by="beta", ascending=False)
        assert list(out.df["variable"]) == ["a"]

    def test_unknown_sort_column_raises(self):
        with pytest.raises(ValueError, match="sort column"):
            _make_result().top(by="not_a_col")

    def test_invalid_n_raises(self):
        with pytest.raises(ValueError, match="positive"):
            _make_result().top(n=0)


class TestSummary:
    def test_summary_keys(self):
        out = _make_result().summary()
        assert out["n_tests"] == 4
        assert out["family"] == "linear"
        assert out["covariates"] == ["age"]

    def test_summary_after_correction(self):
        out = _make_result().with_correction("bonferroni").summary()
        assert out["correction_method"] == "bonferroni"
        assert "n_passing_corrected_005" in out


# ---------------------------------------------------------------------------
# Block A — groupby correction + canonical schema auto-detect
# ---------------------------------------------------------------------------
def _make_phewas_result() -> RegressionResults:
    """Multi-outcome result with the new schema (uses ``beta_pvalue``)."""
    df = pd.DataFrame(
        {
            "outcome":   ["GLUCOSE", "GLUCOSE", "BMI", "BMI"],
            "variable":  ["a",       "b",       "a",   "b"  ],
            "beta":      [0.5,       0.1,       0.3,   0.05 ],
            "se":        [0.1,       0.05,      0.05,  0.5  ],
            "beta_pvalue": [0.0001,  0.04,      0.001, 0.95 ],
        }
    )
    return RegressionResults(
        df=df,
        family="linear",
        outcome="(multiple)",
        covariates=["age"],
        formula_template="{outcome} ~ {exposure} + age",
        errors=pd.DataFrame(columns=["variable", "error"]),
        metadata={},
    )


class TestWithCorrectionGroupby:
    def test_groupby_outcome_corrects_per_outcome(self):
        # Per-outcome Bonferroni: each outcome has 2 tests, so each
        # p-value is multiplied by 2 (capped at 1).
        res = _make_phewas_result().with_correction(
            "bonferroni", groupby="outcome",
        )
        # GLUCOSE p-values 0.0001, 0.04 → corrected 0.0002, 0.08.
        # BMI    p-values 0.001,  0.95 → corrected 0.002, 1.0.
        glucose = res.df[res.df["outcome"] == "GLUCOSE"]
        bmi = res.df[res.df["outcome"] == "BMI"]
        assert set(glucose["p_corrected"].round(4)) == {0.0002, 0.08}
        assert bmi["p_corrected"].round(4).tolist() == [0.002, 1.0]

    def test_groupby_global_corrects_across_all(self):
        # Default groupby=None: global Bonferroni on 4 tests.
        res = _make_phewas_result().with_correction("bonferroni")
        # Each p multiplied by 4 (capped at 1).
        expected = [0.0004, 0.16, 0.004, 1.0]
        assert res.df["p_corrected"].round(4).tolist() == expected

    def test_groupby_unknown_column_raises(self):
        with pytest.raises(ValueError, match="not in result columns"):
            _make_phewas_result().with_correction(
                "bonferroni", groupby="not_a_col",
            )

    def test_groupby_metadata_recorded(self):
        res = _make_phewas_result().with_correction(
            "bonferroni", groupby="outcome",
        )
        assert res.metadata["correction_method"] == "bonferroni"
        assert res.metadata["correction_groupby"] == "outcome"


class TestSchemaAutoDetect:
    def test_with_correction_uses_beta_pvalue_when_present(self):
        # New schema with beta_pvalue (no p_value column at all).
        res = _make_phewas_result().with_correction("bonferroni")
        assert "p_corrected" in res.df.columns

    def test_passing_uses_beta_pvalue_when_present(self):
        out = _make_phewas_result().passing(p=0.01)
        assert set(out.df["variable"]) == {"a"}    # only the two p<0.01 rows
        assert set(out.df["outcome"]) == {"GLUCOSE", "BMI"}

    def test_top_uses_beta_pvalue_when_present(self):
        out = _make_phewas_result().top(n=1)
        assert out.df.iloc[0]["beta_pvalue"] == 0.0001
