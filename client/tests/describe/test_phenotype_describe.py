"""Tests for igem.modules.describe.phenotypes."""
from __future__ import annotations

import numpy as np
import pytest

from igem.modules.describe import (
    correlation_matrix,
    missing_report,
    summarize,
    value_counts,
)


# ---------------------------------------------------------------------------
# summarize
# ---------------------------------------------------------------------------
class TestSummarize:
    def test_one_row_per_column(self, describe_phen):
        out = summarize(describe_phen)
        # sample_id is excluded by default → 5 cols (AGE, BMI, SEX, ETHNICITY, STUDY)
        assert set(out["column"]) == {"AGE", "BMI", "SEX", "ETHNICITY", "STUDY"}
        assert len(out) == 5

    def test_continuous_stats_correct(self, describe_phen):
        out = summarize(describe_phen).set_index("column")
        age = out.loc["AGE"]
        assert age["kind"] == "continuous"
        assert age["n"] == 10
        assert age["n_missing"] == 0
        assert age["mean"] == pytest.approx(4.5)
        assert age["min"] == 0
        assert age["max"] == 9
        assert age["median"] == pytest.approx(4.5)

    def test_missing_count_correct(self, describe_phen):
        out = summarize(describe_phen).set_index("column")
        bmi = out.loc["BMI"]
        assert bmi["kind"] == "continuous"
        assert bmi["n_missing"] == 2
        assert bmi["missing_pct"] == 20.0
        # mean ignores NaN: BMI non-NaN values
        non_na = [20.0, 22.5, 25.0, 30.0, 35.0, 40.0, 28.0, 24.0]
        assert bmi["mean"] == pytest.approx(sum(non_na) / len(non_na))

    def test_categorical_stats_correct(self, describe_phen):
        out = summarize(describe_phen).set_index("column")
        sex = out.loc["SEX"]
        assert sex["kind"] == "categorical"
        assert sex["n_unique"] == 2
        assert sex["mode"] == "M"
        assert sex["mode_count"] == 6
        # Continuous-only stats are NaN.
        assert np.isnan(sex["mean"])
        assert np.isnan(sex["std"])

    def test_constant_column(self, describe_phen):
        out = summarize(describe_phen).set_index("column")
        study = out.loc["STUDY"]
        assert study["n_unique"] == 1
        assert study["mode"] == "NHANES"
        assert study["mode_count"] == 10

    def test_explicit_cols(self, describe_phen):
        out = summarize(describe_phen, cols=["AGE", "SEX"])
        assert list(out["column"]) == ["AGE", "SEX"]

    def test_unknown_column_raises(self, describe_phen):
        with pytest.raises(ValueError, match="not_a_col"):
            summarize(describe_phen, cols=["not_a_col"])


# ---------------------------------------------------------------------------
# missing_report
# ---------------------------------------------------------------------------
class TestMissingReport:
    def test_only_bmi_has_missing(self, describe_phen):
        out = missing_report(describe_phen)
        bmi_row = out[out["column"] == "BMI"].iloc[0]
        assert bmi_row["n_missing"] == 2
        assert bmi_row["missing_pct"] == 20.0
        # Other columns have no missing.
        for col in ("AGE", "SEX", "ETHNICITY", "STUDY"):
            row = out[out["column"] == col].iloc[0]
            assert row["n_missing"] == 0

    def test_sorted_by_missing_pct_desc(self, describe_phen):
        out = missing_report(describe_phen)
        # BMI has the only missing values → ranks first.
        assert out.iloc[0]["column"] == "BMI"
        # Tied zeros at the bottom — accept any order, just ensure pct sorted.
        assert list(out["missing_pct"]) == sorted(
            out["missing_pct"], reverse=True
        )

    def test_includes_sample_id_column(self, describe_phen):
        out = missing_report(describe_phen)
        assert "sample_id" in out["column"].values


# ---------------------------------------------------------------------------
# correlation_matrix
# ---------------------------------------------------------------------------
class TestCorrelationMatrix:
    def test_default_picks_numeric_only(self, describe_phen):
        out = correlation_matrix(describe_phen)
        assert set(out.columns) == {"AGE", "BMI"}
        assert set(out.index) == {"AGE", "BMI"}

    def test_diagonal_is_one(self, describe_phen):
        out = correlation_matrix(describe_phen)
        for col in out.columns:
            assert out.loc[col, col] == pytest.approx(1.0)

    def test_method_propagates(self, describe_phen):
        pearson = correlation_matrix(describe_phen, method="pearson")
        spearman = correlation_matrix(describe_phen, method="spearman")
        # Same shape, same labels, but possibly different values.
        assert pearson.shape == spearman.shape
        assert list(pearson.columns) == list(spearman.columns)

    def test_explicit_non_numeric_col_raises(self, describe_phen):
        with pytest.raises(ValueError, match="not numeric"):
            correlation_matrix(describe_phen, cols=["AGE", "SEX"])

    def test_invalid_method_raises(self, describe_phen):
        with pytest.raises(ValueError, match="method"):
            correlation_matrix(describe_phen, method="bogus")

    def test_no_numeric_columns_returns_empty(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {"sample_id": ["A", "B"], "label": ["x", "y"]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = correlation_matrix(phen)
        assert out.empty


# ---------------------------------------------------------------------------
# value_counts
# ---------------------------------------------------------------------------
class TestValueCounts:
    def test_returns_dict_per_column(self, describe_phen):
        out = value_counts(describe_phen, cols=["SEX"])
        assert set(out.keys()) == {"SEX"}
        sex = out["SEX"]
        assert list(sex.columns) == ["value", "count", "pct"]

    def test_counts_correct_for_binary(self, describe_phen):
        out = value_counts(describe_phen, cols=["SEX"])["SEX"]
        m_row = out[out["value"] == "M"].iloc[0]
        f_row = out[out["value"] == "F"].iloc[0]
        assert m_row["count"] == 6
        assert f_row["count"] == 4
        # 10 samples, no NaN → pct = count / 10 * 100
        assert m_row["pct"] == 60.0
        assert f_row["pct"] == 40.0

    def test_top_limits_rows(self, describe_phen):
        out = value_counts(describe_phen, cols=["ETHNICITY"], top=3)
        assert len(out["ETHNICITY"]) == 3

    def test_dropna_includes_or_excludes_nan(self, describe_phen):
        # BMI has 2 NaN. dropna=False → NaN appears as a row.
        out_with_na = value_counts(
            describe_phen, cols=["BMI"], dropna=False
        )["BMI"]
        nan_rows = out_with_na[out_with_na["value"].isna()]
        assert len(nan_rows) == 1
        assert int(nan_rows.iloc[0]["count"]) == 2

        out_without_na = value_counts(
            describe_phen, cols=["BMI"], dropna=True
        )["BMI"]
        assert out_without_na["value"].isna().sum() == 0

    def test_invalid_top_raises(self, describe_phen):
        with pytest.raises(ValueError, match="top"):
            value_counts(describe_phen, top=0)

    def test_unknown_column_raises(self, describe_phen):
        with pytest.raises(ValueError, match="not_a_col"):
            value_counts(describe_phen, cols=["not_a_col"])
