"""Tests for igem.modules.describe.phenotypes."""
from __future__ import annotations

import numpy as np
import pytest

from igem.modules.describe import (
    correlation_matrix,
    correlation_pairs,
    crosstab,
    dataset_summary,
    missing_report,
    skewness,
    summarize,
    summarize_by,
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

    def test_binary_stats_correct(self, describe_phen):
        out = summarize(describe_phen).set_index("column")
        sex = out.loc["SEX"]
        # SEX has exactly 2 unique values → kind is "binary".
        assert sex["kind"] == "binary"
        assert sex["n_unique"] == 2
        assert sex["mode"] == "M"
        assert sex["mode_count"] == 6
        # Continuous-only stats are NaN.
        assert np.isnan(sex["mean"])
        assert np.isnan(sex["std"])

    def test_categorical_kind_for_high_cardinality(self, describe_phen):
        # ETHNICITY has 10 distinct values → categorical, not binary.
        out = summarize(describe_phen).set_index("column")
        assert out.loc["ETHNICITY", "kind"] == "categorical"

    def test_numeric_binary_classified_as_binary(self):
        # Numeric column with only {0, 1} should be binary, not continuous.
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "CASE": [0, 1, 0, 1, 1, 0, 1, 0, 0, 1],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = summarize(phen).set_index("column")
        assert out.loc["CASE", "kind"] == "binary"

    def test_bool_classified_as_binary(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "FLAG": [True, False] * 5,
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = summarize(phen).set_index("column")
        assert out.loc["FLAG", "kind"] == "binary"

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

    def test_weighted_requires_weights_col(self, describe_phen):
        # describe_phen has no weights_col → weighted=True must fail.
        with pytest.raises(ValueError, match="weights_col"):
            summarize(describe_phen, weighted=True)

    def test_weighted_uniform_weights_match_unweighted(self):
        # With constant weights, weighted stats should match unweighted ones.
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "AGE": list(range(10)),
                "SEX": ["M", "F"] * 5,
                "WT": [1.0] * 10,
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id", weights_col="WT")
        unw = summarize(phen).set_index("column")
        wt = summarize(phen, weighted=True).set_index("column")
        # AGE mean should match.
        assert wt.loc["AGE", "mean"] == pytest.approx(unw.loc["AGE", "mean"])
        # SEX mode should match (more frequent value).
        assert wt.loc["SEX", "mode"] == unw.loc["SEX", "mode"]

    def test_weighted_skews_mean_toward_heavy_rows(self):
        # Heavy weight on row with X=100 should pull weighted mean toward 100.
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(5)],
                "X":  [0.0, 0.0, 0.0, 0.0, 100.0],
                "WT": [1.0, 1.0, 1.0, 1.0, 1000.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id", weights_col="WT")
        unw_mean = summarize(phen).set_index("column").loc["X", "mean"]
        wt_mean = summarize(phen, weighted=True).set_index("column").loc["X", "mean"]
        # Unweighted: 20.0; weighted: ~99.6.
        assert unw_mean == pytest.approx(20.0)
        assert wt_mean > 90.0

    def test_weighted_categorical_mode_uses_weight_sum(self):
        # "B" has fewer rows but heavier weights → weighted mode should be "B".
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(5)],
                "GROUP": ["A", "A", "A", "B", "B"],
                "WT":    [1.0, 1.0, 1.0, 100.0, 100.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id", weights_col="WT")
        unw = summarize(phen).set_index("column").loc["GROUP", "mode"]
        wt = summarize(phen, weighted=True).set_index("column").loc["GROUP", "mode"]
        # Unweighted picks "A" (more rows); weighted picks "B" (heavier total weight).
        assert unw == "A"
        assert wt == "B"

    def test_near_zero_var_constant_column(self, describe_phen):
        # STUDY is constant ("NHANES" × 10) → near_zero_var=True regardless of kind.
        out = summarize(describe_phen).set_index("column")
        assert out.loc["STUDY", "near_zero_var"] == True  # noqa: E712

    def test_near_zero_var_normal_continuous(self, describe_phen):
        # AGE = 0..9: clearly variable → not near-zero.
        out = summarize(describe_phen).set_index("column")
        assert out.loc["AGE", "near_zero_var"] == False  # noqa: E712

    def test_near_zero_var_low_cv_continuous(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                # Mean ~100, std tiny → CV << 1e-3.
                "X": [100.0, 100.0001, 100.00001, 100.0, 100.0,
                      100.0, 100.00002, 100.0, 100.0001, 100.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = summarize(phen).set_index("column")
        assert out.loc["X", "near_zero_var"] == True  # noqa: E712

    def test_near_zero_var_categorical_not_constant(self, describe_phen):
        # ETHNICITY has 10 unique values → not near-zero.
        out = summarize(describe_phen).set_index("column")
        assert out.loc["ETHNICITY", "near_zero_var"] == False  # noqa: E712

    def test_n_outliers_continuous_with_outlier(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(11)],
                # 10 normal points + 1 extreme outlier at the end.
                "X": [10.0, 11.0, 12.0, 13.0, 14.0,
                      15.0, 16.0, 17.0, 18.0, 19.0, 1000.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = summarize(phen).set_index("column")
        assert out.loc["X", "n_outliers"] >= 1

    def test_n_outliers_zero_for_uniform(self, describe_phen):
        # AGE = 0..9 is symmetric, no Tukey outliers.
        out = summarize(describe_phen).set_index("column")
        assert out.loc["AGE", "n_outliers"] == 0

    def test_n_outliers_nan_for_categorical(self, describe_phen):
        # SEX (binary) and ETHNICITY (categorical) → n_outliers stays NaN.
        out = summarize(describe_phen).set_index("column")
        assert np.isnan(out.loc["SEX", "n_outliers"])
        assert np.isnan(out.loc["ETHNICITY", "n_outliers"])

    def test_weighted_drops_missing_in_value_or_weight(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        import numpy as np
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(5)],
                "X":  [1.0, 2.0, np.nan, 4.0, 5.0],
                "WT": [1.0, 1.0, 1.0, np.nan, 1.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id", weights_col="WT")
        wt = summarize(phen, weighted=True).set_index("column")
        # Only S0, S1, S4 contribute to weighted mean → mean = (1+2+5)/3 = 2.667.
        assert wt.loc["X", "mean"] == pytest.approx(8.0 / 3, abs=1e-9)
        # n stays raw (all 5 rows; n_missing only for X = 1).
        assert wt.loc["X", "n"] == 5
        assert wt.loc["X", "n_missing"] == 1


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
# correlation_pairs
# ---------------------------------------------------------------------------
class TestCorrelationPairs:
    def _phen_with_correlated_cols(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        n = 50
        x = list(range(n))
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(n)],
                "x1": x,
                "x2": [v + 0.01 for v in x],          # ~+1 with x1
                "x3": [-v for v in x],                 # ~-1 with x1
                "x4": [(v * 7) % 13 for v in x],       # weak/no corr
            }
        )
        return Phenotypes(df, sample_id_col="sample_id")

    def test_columns_present(self, describe_phen):
        out = correlation_pairs(describe_phen, threshold=0.0)
        assert list(out.columns) == ["var1", "var2", "r"]

    def test_threshold_filters_pairs(self):
        phen = self._phen_with_correlated_cols()
        out = correlation_pairs(phen, threshold=0.99)
        # x1↔x2 (r≈1) and x1↔x3 (r≈-1) survive at |r|>=0.99; x4 pairs do not.
        pairs = {tuple(sorted([row.var1, row.var2])) for row in out.itertuples()}
        assert ("x1", "x2") in pairs
        assert ("x1", "x3") in pairs
        # x4 pairs have low |r|.
        for var1, var2 in pairs:
            assert "x4" not in (var1, var2)

    def test_absolute_default_keeps_anticorrelation(self):
        phen = self._phen_with_correlated_cols()
        out = correlation_pairs(phen, threshold=0.99, absolute=True)
        # x1↔x3 has r ≈ -1 → kept under absolute mode.
        rows = out[
            (out["var1"].isin(["x1", "x3"])) & (out["var2"].isin(["x1", "x3"]))
        ]
        assert len(rows) == 1
        assert rows.iloc[0]["r"] < 0

    def test_absolute_false_drops_anticorrelation(self):
        phen = self._phen_with_correlated_cols()
        out = correlation_pairs(phen, threshold=0.99, absolute=False)
        # With absolute=False, only r >= 0.99 survive → x1↔x3 (r≈-1) dropped.
        for row in out.itertuples():
            assert row.r >= 0.99

    def test_sorted_by_abs_r_desc(self):
        phen = self._phen_with_correlated_cols()
        out = correlation_pairs(phen, threshold=0.0)
        abs_r = out["r"].abs().tolist()
        assert abs_r == sorted(abs_r, reverse=True)

    def test_no_pairs_above_threshold_returns_empty(self, describe_phen):
        # Only AGE, BMI numeric → 1 candidate pair; high threshold drops it.
        out = correlation_pairs(describe_phen, threshold=0.999)
        assert out.empty

    def test_invalid_threshold_raises(self, describe_phen):
        with pytest.raises(ValueError, match="threshold"):
            correlation_pairs(describe_phen, threshold=1.5)
        with pytest.raises(ValueError, match="threshold"):
            correlation_pairs(describe_phen, threshold=-0.1)

    def test_no_numeric_columns_returns_empty(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame({"sample_id": ["A", "B"], "label": ["x", "y"]})
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = correlation_pairs(phen)
        assert out.empty
        assert list(out.columns) == ["var1", "var2", "r"]


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


# ---------------------------------------------------------------------------
# skewness
# ---------------------------------------------------------------------------
class TestSkewness:
    def test_default_picks_numeric_only(self, describe_phen):
        out = skewness(describe_phen)
        # AGE and BMI are numeric; sample_id excluded; SEX/ETHNICITY/STUDY skipped.
        assert set(out["column"]) == {"AGE", "BMI"}

    def test_columns_present(self, describe_phen):
        out = skewness(describe_phen)
        assert list(out.columns) == ["column", "n", "skew", "zscore", "pvalue"]

    def test_symmetric_column_has_zero_skew(self, describe_phen):
        # AGE = 0..9 → perfectly symmetric → skew == 0.
        out = skewness(describe_phen).set_index("column")
        age = out.loc["AGE"]
        assert age["skew"] == pytest.approx(0.0, abs=1e-9)
        assert age["n"] == 10
        # n=10 ≥ 8, so zscore/pvalue are defined.
        assert not np.isnan(age["zscore"])
        assert not np.isnan(age["pvalue"])

    def test_skewed_column_detected(self):
        # Right-skewed data → positive skew, p < 0.05.
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(20)],
                "x": list(range(19)) + [1000],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = skewness(phen).set_index("column")
        assert out.loc["x", "skew"] > 1.0
        assert out.loc["x", "pvalue"] < 0.05

    def test_dropna_false_propagates_nan(self, describe_phen):
        # BMI has 2 NaN; dropna=False → stats are NaN.
        out = skewness(describe_phen, dropna=False).set_index("column")
        bmi = out.loc["BMI"]
        assert np.isnan(bmi["skew"])
        assert np.isnan(bmi["zscore"])
        assert np.isnan(bmi["pvalue"])

    def test_dropna_true_drops_nan(self, describe_phen):
        # BMI has 8 valid values after dropna → skew defined, zscore/pvalue too.
        out = skewness(describe_phen, dropna=True).set_index("column")
        bmi = out.loc["BMI"]
        assert bmi["n"] == 8
        assert not np.isnan(bmi["skew"])
        assert not np.isnan(bmi["zscore"])

    def test_small_n_skewtest_omits_pvalue(self):
        # n=5 < 8 → skew computed, zscore/pvalue NaN.
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(5)],
                "x": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = skewness(phen).set_index("column")
        assert not np.isnan(out.loc["x", "skew"])
        assert np.isnan(out.loc["x", "zscore"])
        assert np.isnan(out.loc["x", "pvalue"])

    def test_explicit_non_numeric_col_raises(self, describe_phen):
        with pytest.raises(ValueError, match="not numeric"):
            skewness(describe_phen, cols=["AGE", "SEX"])

    def test_unknown_column_raises(self, describe_phen):
        with pytest.raises(ValueError, match="not_a_col"):
            skewness(describe_phen, cols=["not_a_col"])

    def test_bool_column_excluded_by_default(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "AGE": list(range(10)),
                "FLAG": [True, False] * 5,
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = skewness(phen)
        assert set(out["column"]) == {"AGE"}


# ---------------------------------------------------------------------------
# dataset_summary
# ---------------------------------------------------------------------------
class TestDatasetSummary:
    def test_keys_present(self, describe_phen):
        out = dataset_summary(describe_phen)
        expected = {
            "n_samples", "n_columns",
            "n_continuous", "n_binary", "n_categorical",
            "n_with_missing", "total_missing_pct",
            "n_outcomes", "n_covariates", "n_exposures",
            "has_survey_design",
        }
        assert set(out.keys()) == expected

    def test_counts_correct(self, describe_phen):
        # describe_phen: 10 samples; non-sample_id cols = 5
        # AGE (continuous), BMI (continuous), SEX (binary, M/F),
        # ETHNICITY (categorical, 10 unique), STUDY (categorical, 1 unique).
        out = dataset_summary(describe_phen)
        assert out["n_samples"] == 10
        assert out["n_columns"] == 5
        assert out["n_continuous"] == 2  # AGE, BMI
        assert out["n_binary"] == 1      # SEX
        assert out["n_categorical"] == 2  # ETHNICITY, STUDY

    def test_missing_aggregation(self, describe_phen):
        # Only BMI has missing → 1 column with missing.
        out = dataset_summary(describe_phen)
        assert out["n_with_missing"] == 1
        # 2 missing out of 50 cells (5 cols × 10 rows) = 4.0%.
        assert out["total_missing_pct"] == pytest.approx(4.0)

    def test_role_counts(self, describe_phen):
        # Fixture sets outcomes=["BMI"], covariates=["AGE", "SEX"].
        out = dataset_summary(describe_phen)
        assert out["n_outcomes"] == 1
        assert out["n_covariates"] == 2
        assert out["n_exposures"] == 0

    def test_no_survey_design_by_default(self, describe_phen):
        out = dataset_summary(describe_phen)
        assert out["has_survey_design"] is False

    def test_survey_design_detected(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "AGE": list(range(10)),
                "WT": [1.0] * 10,
            }
        )
        phen = Phenotypes(
            df, sample_id_col="sample_id", weights_col="WT"
        )
        out = dataset_summary(phen)
        assert out["has_survey_design"] is True


# ---------------------------------------------------------------------------
# summarize_by
# ---------------------------------------------------------------------------
class TestSummarizeBy:
    def test_one_row_per_group_and_column(self, describe_phen):
        # by=SEX has 2 groups (M, F); 4 non-by columns (AGE, BMI, ETHNICITY, STUDY)
        # → 8 rows. sample_id excluded by default; SEX itself excluded as `by`.
        out = summarize_by(describe_phen, by="SEX")
        assert len(out) == 8
        assert set(out["SEX"]) == {"M", "F"}
        assert set(out["column"]) == {"AGE", "BMI", "ETHNICITY", "STUDY"}

    def test_columns_layout(self, describe_phen):
        out = summarize_by(describe_phen, by="SEX")
        # `by` column comes first, then standard summary columns.
        assert out.columns[0] == "SEX"
        assert "column" in out.columns
        assert "mean" in out.columns

    def test_per_group_stats_correct(self, describe_phen):
        # M group has AGE values: indices 0,2,3,6,7,9 → AGE = 0,2,3,6,7,9
        # mean = 27/6 = 4.5; F group: indices 1,4,5,8 → AGE = 1,4,5,8 → mean = 4.5
        out = summarize_by(describe_phen, by="SEX")
        m_age = out[(out["SEX"] == "M") & (out["column"] == "AGE")].iloc[0]
        f_age = out[(out["SEX"] == "F") & (out["column"] == "AGE")].iloc[0]
        assert m_age["n"] == 6
        assert f_age["n"] == 4
        assert m_age["mean"] == pytest.approx(4.5)
        assert f_age["mean"] == pytest.approx(4.5)

    def test_by_column_excluded_from_target(self, describe_phen):
        # Even with explicit cols including `by`, by is dropped.
        out = summarize_by(
            describe_phen, by="SEX", cols=["AGE", "SEX", "BMI"]
        )
        assert "SEX" not in set(out["column"])
        assert set(out["column"]) == {"AGE", "BMI"}

    def test_dropna_group_default_drops_nan(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        import numpy as np
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(6)],
                "GROUP": ["A", "A", "B", "B", np.nan, np.nan],
                "X": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        # Default dropna_group=True: NaN group dropped.
        out = summarize_by(phen, by="GROUP")
        assert set(out["GROUP"].dropna()) == {"A", "B"}
        # NaN should not appear as a group key.
        assert not out["GROUP"].isna().any()

    def test_dropna_group_false_keeps_nan(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        import numpy as np
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(6)],
                "GROUP": ["A", "A", "B", "B", np.nan, np.nan],
                "X": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = summarize_by(phen, by="GROUP", dropna_group=False)
        # NaN group included → 3 group values × 1 col = 3 rows.
        assert len(out) == 3
        assert out["GROUP"].isna().any()

    def test_unknown_by_raises(self, describe_phen):
        with pytest.raises(ValueError, match="not_a_col"):
            summarize_by(describe_phen, by="not_a_col")

    def test_unknown_target_col_raises(self, describe_phen):
        with pytest.raises(ValueError, match="bogus"):
            summarize_by(describe_phen, by="SEX", cols=["bogus"])


# ---------------------------------------------------------------------------
# crosstab
# ---------------------------------------------------------------------------
class TestCrosstab:
    def _phen_with_genotype_exposure(self):
        from igem.modules.data import Phenotypes
        import pandas as pd
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(12)],
                "GENO":     [0, 0, 0, 1, 1, 1, 2, 2, 2, 0, 1, 2],
                "EXPOSED":  [0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 0, 0],
            }
        )
        return Phenotypes(df, sample_id_col="sample_id")

    def test_basic_counts(self, describe_phen):
        # SEX × STUDY: STUDY constant ("NHANES") for all 10; SEX = 6M, 4F.
        out = crosstab(describe_phen, "SEX", "STUDY")
        assert out.loc["M", "NHANES"] == 6
        assert out.loc["F", "NHANES"] == 4

    def test_genotype_exposure_cells(self):
        phen = self._phen_with_genotype_exposure()
        out = crosstab(phen, "GENO", "EXPOSED")
        # Verify shape: 3 genotypes × 2 exposure levels.
        assert out.shape == (3, 2)
        # Spot-check known cells.
        assert out.loc[0, 0] == 2  # GENO=0 & EXPOSED=0 → S0, S1
        assert out.loc[2, 1] == 2  # GENO=2 & EXPOSED=1 → S7, S8

    def test_normalize_index_rows_sum_to_one(self):
        phen = self._phen_with_genotype_exposure()
        out = crosstab(phen, "GENO", "EXPOSED", normalize="index")
        for row_total in out.sum(axis=1):
            assert row_total == pytest.approx(1.0)

    def test_normalize_all_grand_total_one(self):
        phen = self._phen_with_genotype_exposure()
        out = crosstab(phen, "GENO", "EXPOSED", normalize="all")
        assert out.values.sum() == pytest.approx(1.0)

    def test_margins_adds_totals(self):
        phen = self._phen_with_genotype_exposure()
        out = crosstab(phen, "GENO", "EXPOSED", margins=True)
        # Margins add an "All" row and column.
        assert "All" in out.index
        assert "All" in out.columns
        # Grand total in bottom-right matches sample count.
        assert out.loc["All", "All"] == 12

    def test_unknown_var_raises(self, describe_phen):
        with pytest.raises(ValueError, match="bogus"):
            crosstab(describe_phen, "bogus", "SEX")
        with pytest.raises(ValueError, match="bogus"):
            crosstab(describe_phen, "SEX", "bogus")

    def test_rare_cell_detection_via_threshold(self):
        # User-side rare cell flagging: just compare output to threshold.
        phen = self._phen_with_genotype_exposure()
        out = crosstab(phen, "GENO", "EXPOSED")
        rare = out < 3
        # All cells in our small fixture are below 3 → rare matrix is all True.
        assert rare.values.all()
