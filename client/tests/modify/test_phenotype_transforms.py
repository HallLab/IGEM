"""Tests for igem.modules.modify.phenotypes."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem.modules.modify import (
    auto_classify,
    colfilter,
    colfilter_min_cat_n,
    colfilter_min_n,
    colfilter_percent_zero,
    discretize,
    drop_missing,
    make_binary,
    make_categorical,
    make_continuous,
    merge_observations,
    merge_variables,
    move_variables,
    recode,
    remove_outliers,
    rowfilter_incomplete_obs,
    transform,
)


# ---------------------------------------------------------------------------
# discretize
# ---------------------------------------------------------------------------
class TestDiscretize:
    def test_quantile_bins_create_new_column(self, transform_phen):
        result = discretize(transform_phen, "BMI", method="quantiles", n_bins=2)
        assert "BMI_cat" in result.df.columns
        assert "BMI" in result.df.columns  # original kept
        assert result.df["BMI_cat"].nunique(dropna=True) == 2

    def test_quantile_bins_with_labels(self, transform_phen):
        result = discretize(
            transform_phen, "BMI",
            method="quantiles", n_bins=2,
            labels=["low", "high"],
        )
        cats = set(result.df["BMI_cat"].dropna().astype(str).unique())
        assert cats == {"low", "high"}

    def test_explicit_bin_edges(self, transform_phen):
        # BMI values: 18, 21, 24, 27, 30, 33, 36, 39
        result = discretize(
            transform_phen, "BMI",
            method="bins", bin_edges=[0, 25, 30, 100],
            labels=["normal", "overweight", "obese"],
        )
        cats = result.df["BMI_cat"].astype(str).tolist()
        # 18,21,24 → normal; 27,30 → overweight; 33,36,39 → obese
        assert cats.count("normal") == 3
        assert cats.count("overweight") == 2
        assert cats.count("obese") == 3

    def test_replace_in_place(self, transform_phen):
        result = discretize(
            transform_phen, "BMI", method="quantiles", n_bins=2,
            replace=True,
        )
        # Replaced: "BMI" now holds categories, no "BMI_cat" column.
        assert "BMI_cat" not in result.df.columns
        assert result.df["BMI"].nunique(dropna=True) == 2

    def test_preserves_role_metadata(self, transform_phen):
        result = discretize(transform_phen, "BMI", method="quantiles", n_bins=2)
        assert result.outcomes == transform_phen.outcomes
        assert result.covariates == transform_phen.covariates
        assert result.exposures == transform_phen.exposures
        assert result.sample_id_col == transform_phen.sample_id_col

    def test_unknown_column_raises(self, transform_phen):
        with pytest.raises(ValueError, match="not_a_col"):
            discretize(transform_phen, "not_a_col", n_bins=2)

    def test_unknown_method_raises(self, transform_phen):
        with pytest.raises(ValueError, match="method"):
            discretize(transform_phen, "BMI", method="bogus")


# ---------------------------------------------------------------------------
# recode
# ---------------------------------------------------------------------------
class TestRecode:
    def test_maps_values_in_place(self, transform_phen):
        result = recode(transform_phen, "SEX", {1: "M", 2: "F"})
        # Value 99 left untouched (not in mapping, not in missing_values).
        values = result.df["SEX"].tolist()
        assert values.count("M") == 3
        assert values.count("F") == 4
        assert 99 in values or "99" in [str(v) for v in values]

    def test_missing_values_become_nan(self, transform_phen):
        result = recode(
            transform_phen, "SEX",
            {1: "M", 2: "F"},
            missing_values=[99],
        )
        assert result.df["SEX"].isna().sum() == 1

    def test_writes_to_new_column(self, transform_phen):
        result = recode(
            transform_phen, "SEX",
            {1: "M", 2: "F"},
            new_col="SEX_label",
            replace=False,
        )
        assert "SEX_label" in result.df.columns
        # Original column unchanged.
        assert result.df["SEX"].tolist() == transform_phen.df["SEX"].tolist()

    def test_preserves_role_metadata(self, transform_phen):
        result = recode(transform_phen, "SEX", {1: "M", 2: "F"})
        assert result.outcomes == transform_phen.outcomes
        assert result.covariates == transform_phen.covariates

    def test_unknown_column_raises(self, transform_phen):
        with pytest.raises(ValueError, match="not_a_col"):
            recode(transform_phen, "not_a_col", {1: 0})


# ---------------------------------------------------------------------------
# drop_missing
# ---------------------------------------------------------------------------
class TestDropMissing:
    def test_default_uses_role_columns(self, transform_phen):
        # AGE has a NaN → row with NaN in AGE should be dropped.
        result = drop_missing(transform_phen)
        assert result.n_samples == 7
        assert result.df["AGE"].isna().sum() == 0

    def test_explicit_cols_restrict_scope(self, transform_phen):
        # Restrict to BMI only (no missing there) → no rows dropped.
        result = drop_missing(transform_phen, cols=["BMI"])
        assert result.n_samples == transform_phen.n_samples

    def test_preserves_role_metadata(self, transform_phen):
        result = drop_missing(transform_phen)
        assert result.outcomes == transform_phen.outcomes
        assert result.weights_col == transform_phen.weights_col

    def test_unknown_column_raises(self, transform_phen):
        with pytest.raises(ValueError, match="not_a_col"):
            drop_missing(transform_phen, cols=["not_a_col"])

    def test_no_active_columns_returns_copy(self):
        """When no role columns exist and none are given, return as-is."""
        from igem.modules.data import Phenotypes

        df = pd.DataFrame(
            {"sample_id": ["A", "B"], "val": [1.0, np.nan]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        # Default cols = [sample_id] → no NaN there, row stays.
        result = drop_missing(phen)
        assert result.n_samples == 2


# ---------------------------------------------------------------------------
# transform
# ---------------------------------------------------------------------------
class TestTransform:
    def test_log_creates_new_column(self, transform_phen):
        result = transform(transform_phen, "BMI", method="log")
        assert "BMI_log" in result.df.columns
        assert "BMI" in result.df.columns
        # log(18) ≈ 2.89; log(39) ≈ 3.66
        assert result.df["BMI_log"].iloc[0] == pytest.approx(np.log(18.0))

    def test_log1p_handles_zero(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {"sample_id": ["A", "B", "C"], "X": [0.0, 1.0, 2.0]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        result = transform(phen, "X", method="log1p")
        # log1p(0) = 0; log1p(1) = ln(2); log1p(2) = ln(3)
        assert result.df["X_log1p"].iloc[0] == pytest.approx(0.0)
        assert result.df["X_log1p"].iloc[1] == pytest.approx(np.log(2))

    def test_log_negative_becomes_nan(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {"sample_id": ["A", "B"], "X": [-1.0, 2.0]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        result = transform(phen, "X", method="log")
        assert np.isnan(result.df["X_log"].iloc[0])
        assert result.df["X_log"].iloc[1] == pytest.approx(np.log(2.0))

    def test_sqrt_negative_becomes_nan(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {"sample_id": ["A", "B"], "X": [-4.0, 9.0]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        result = transform(phen, "X", method="sqrt")
        assert np.isnan(result.df["X_sqrt"].iloc[0])
        assert result.df["X_sqrt"].iloc[1] == pytest.approx(3.0)

    def test_zscore_centers_and_scales(self, transform_phen):
        result = transform(transform_phen, "BMI", method="zscore")
        z = result.df["BMI_zscore"]
        assert z.mean() == pytest.approx(0.0, abs=1e-10)
        assert z.std(ddof=1) == pytest.approx(1.0, abs=1e-10)

    def test_rank_int_produces_normal_like(self, transform_phen):
        # 8 distinct values → ranks 1..8 → quantiles (0.5..7.5)/8 → norm.ppf
        result = transform(transform_phen, "BMI", method="rank_int")
        rint = result.df["BMI_rank_int"]
        # Symmetric around 0, monotonic in input.
        assert rint.iloc[0] < 0  # smallest BMI → most negative
        assert rint.iloc[-1] > 0  # largest BMI → most positive
        assert rint.mean() == pytest.approx(0.0, abs=1e-10)

    def test_rank_int_preserves_ranks_with_ties(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {"sample_id": [f"S{i}" for i in range(4)],
             "X": [1.0, 1.0, 2.0, 3.0]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        result = transform(phen, "X", method="rank_int")
        rint = result.df["X_rank_int"]
        # The two 1.0s get the same average rank → same RINT value.
        assert rint.iloc[0] == pytest.approx(rint.iloc[1])

    def test_boxcox_positive_only(self, transform_phen):
        result = transform(transform_phen, "BMI", method="boxcox")
        assert "BMI_boxcox" in result.df.columns
        # Box-Cox on monotonic input stays monotonic.
        bc = result.df["BMI_boxcox"]
        assert bc.is_monotonic_increasing

    def test_boxcox_rejects_non_positive(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {"sample_id": ["A", "B"], "X": [0.0, 1.0]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        with pytest.raises(ValueError, match="positive"):
            transform(phen, "X", method="boxcox")

    def test_func_callable_path(self, transform_phen):
        # Custom transformation: multiply by 10.
        result = transform(transform_phen, "BMI", func=lambda s: s * 10)
        assert "BMI_transformed" in result.df.columns
        assert result.df["BMI_transformed"].iloc[0] == 180.0

    def test_method_and_func_mutually_exclusive(self, transform_phen):
        with pytest.raises(ValueError, match="exactly one"):
            transform(transform_phen, "BMI", method="log", func=np.exp)
        with pytest.raises(ValueError, match="exactly one"):
            transform(transform_phen, "BMI")

    def test_replace_in_place(self, transform_phen):
        result = transform(transform_phen, "BMI", method="zscore", replace=True)
        assert "BMI_zscore" not in result.df.columns
        assert result.df["BMI"].mean() == pytest.approx(0.0, abs=1e-10)

    def test_unknown_method_raises(self, transform_phen):
        with pytest.raises(ValueError, match="method must be"):
            transform(transform_phen, "BMI", method="bogus")

    def test_unknown_column_raises(self, transform_phen):
        with pytest.raises(ValueError, match="not_a_col"):
            transform(transform_phen, "not_a_col", method="log")

    def test_non_numeric_column_raises(self, transform_phen):
        # SEX is int but recoded to string would fail. Use sample_id (object).
        with pytest.raises(ValueError, match="numeric"):
            transform(transform_phen, "sample_id", method="log")

    def test_preserves_role_metadata(self, transform_phen):
        result = transform(transform_phen, "BMI", method="log")
        assert result.outcomes == transform_phen.outcomes
        assert result.covariates == transform_phen.covariates
        assert result.exposures == transform_phen.exposures


# ---------------------------------------------------------------------------
# remove_outliers
# ---------------------------------------------------------------------------
class TestRemoveOutliers:
    def _phen_with_outlier(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(11)],
                # 10 points 10..19 + one extreme outlier at 1000
                "X": [10.0, 11.0, 12.0, 13.0, 14.0,
                      15.0, 16.0, 17.0, 18.0, 19.0, 1000.0],
                "Y": [1.0] * 11,  # constant — no outlier possible
            }
        )
        return Phenotypes(df, sample_id_col="sample_id")

    def test_iqr_replaces_outlier_with_nan(self):
        phen = self._phen_with_outlier()
        result = remove_outliers(phen, cols=["X"], method="iqr")
        # The 1000.0 should become NaN; original 10..19 untouched.
        x = result.df["X"]
        assert np.isnan(x.iloc[-1])
        assert (x.iloc[:-1] == phen.df["X"].iloc[:-1]).all()

    def test_gaussian_replaces_outlier(self):
        phen = self._phen_with_outlier()
        result = remove_outliers(phen, cols=["X"], method="gaussian", cutoff=2.0)
        assert np.isnan(result.df["X"].iloc[-1])

    def test_default_picks_numeric_only(self, transform_phen):
        # Should not error on the SEX column even though it's int.
        result = remove_outliers(transform_phen)
        # All non-numeric columns untouched.
        assert result.df["sample_id"].equals(transform_phen.df["sample_id"])

    def test_non_numeric_explicit_raises(self, transform_phen):
        with pytest.raises(ValueError, match="not numeric"):
            remove_outliers(transform_phen, cols=["sample_id"])

    def test_unknown_method_raises(self, transform_phen):
        with pytest.raises(ValueError, match="method must be"):
            remove_outliers(transform_phen, method="bogus")

    def test_constant_column_unchanged(self):
        phen = self._phen_with_outlier()
        result = remove_outliers(phen, cols=["Y"])
        # Y is constant → IQR=0 → no change.
        assert result.df["Y"].equals(phen.df["Y"])

    def test_preserves_role_metadata(self, transform_phen):
        result = remove_outliers(transform_phen)
        assert result.outcomes == transform_phen.outcomes
        assert result.covariates == transform_phen.covariates

    def test_invalid_k_raises(self, transform_phen):
        with pytest.raises(ValueError, match="k must"):
            remove_outliers(transform_phen, k=0)

    def test_invalid_cutoff_raises(self, transform_phen):
        with pytest.raises(ValueError, match="cutoff must"):
            remove_outliers(transform_phen, cutoff=-1.0)


# ---------------------------------------------------------------------------
# auto_classify
# ---------------------------------------------------------------------------
class TestAutoClassify:
    def test_columns_returned(self, transform_phen):
        out = auto_classify(transform_phen)
        assert list(out.columns) == ["column", "n_unique", "dtype", "kind"]

    def test_excludes_sample_id(self, transform_phen):
        out = auto_classify(transform_phen)
        assert transform_phen.sample_id_col not in set(out["column"])

    def test_classification_logic(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(20)],
                "constant_col": [5] * 20,
                "binary_col": [0, 1] * 10,
                "small_cat":   list(range(4)) * 5,                 # 4 levels
                "many_unique": list(range(20)),                    # numeric, n=20
                "object_high": [f"x{i}" for i in range(20)],       # non-numeric
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = auto_classify(phen).set_index("column")
        assert out.loc["constant_col", "kind"] == "constant"
        assert out.loc["binary_col", "kind"] == "binary"
        assert out.loc["small_cat", "kind"] == "categorical"
        assert out.loc["many_unique", "kind"] == "continuous"
        # 20 unique non-numeric values → not numeric → unknown.
        assert out.loc["object_high", "kind"] == "unknown"

    def test_thresholds_propagate(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "X": list(range(10)),    # 10 unique numeric
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        # cont_min=10 → "X" qualifies as continuous.
        out_low = auto_classify(phen, cont_min=10).set_index("column")
        assert out_low.loc["X", "kind"] == "continuous"
        # cont_min=15, cat_max=6 → "X" falls into unknown gap.
        out_high = auto_classify(phen, cont_min=15).set_index("column")
        assert out_high.loc["X", "kind"] == "unknown"


# ---------------------------------------------------------------------------
# make_binary
# ---------------------------------------------------------------------------
class TestMakeBinary:
    def test_zero_one_int_coerces_to_int64(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {"sample_id": ["A", "B", "C", "D"],
             "CASE": [0, 1, 0, 1]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = make_binary(phen, only=["CASE"])
        assert str(out.df["CASE"].dtype) == "Int64"

    def test_string_binary_coerces_to_categorical(self, transform_phen):
        # First recode SEX to drop sentinel 99, then make a binary column.
        recoded = recode(transform_phen, "SEX",
                         {1: "M", 2: "F"}, missing_values=[99])
        # Drop the row with NaN before make_binary (unique non-NaN must be 2).
        cleaned = drop_missing(recoded, cols=["SEX"])
        out = make_binary(cleaned, only=["SEX"])
        assert isinstance(out.df["SEX"].dtype, pd.CategoricalDtype)
        assert sorted(out.df["SEX"].cat.categories.tolist()) == ["F", "M"]

    def test_more_than_two_unique_raises(self, transform_phen):
        # SEX has values {1, 2, 99} → 3 unique → fail.
        with pytest.raises(ValueError, match="3 distinct"):
            make_binary(transform_phen, only=["SEX"])

    def test_skip_excludes_columns(self, transform_phen):
        # only=None and skip=everything → no columns target → no error.
        out = make_binary(
            transform_phen,
            skip=["BMI", "SEX", "GLUCOSE", "AGE"],
        )
        # Nothing changed.
        assert out.df.equals(transform_phen.df)


# ---------------------------------------------------------------------------
# make_categorical
# ---------------------------------------------------------------------------
class TestMakeCategorical:
    def test_coerces_to_category_dtype(self, transform_phen):
        out = make_categorical(transform_phen, only=["SEX"])
        assert isinstance(out.df["SEX"].dtype, pd.CategoricalDtype)

    def test_only_targets_specified(self, transform_phen):
        out = make_categorical(transform_phen, only=["SEX"])
        # BMI should remain numeric.
        assert pd.api.types.is_numeric_dtype(out.df["BMI"])


# ---------------------------------------------------------------------------
# make_continuous
# ---------------------------------------------------------------------------
class TestMakeContinuous:
    def test_string_numeric_coerced(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {"sample_id": ["A", "B"], "X": ["1.5", "2.5"]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        out = make_continuous(phen, only=["X"])
        assert pd.api.types.is_numeric_dtype(out.df["X"])
        assert out.df["X"].iloc[0] == 1.5

    def test_non_numeric_string_raises(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {"sample_id": ["A", "B"], "X": ["foo", "bar"]}
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        with pytest.raises(ValueError):
            make_continuous(phen, only=["X"])


# ---------------------------------------------------------------------------
# colfilter (base)
# ---------------------------------------------------------------------------
class TestColfilter:
    def test_only_keeps_listed_plus_sample_id(self, transform_phen):
        result = colfilter(transform_phen, only=["BMI", "AGE"])
        assert set(result.df.columns) == {
            transform_phen.sample_id_col, "BMI", "AGE"
        }

    def test_skip_drops_listed(self, transform_phen):
        result = colfilter(transform_phen, skip=["BMI"])
        assert "BMI" not in result.df.columns
        assert "AGE" in result.df.columns
        assert transform_phen.sample_id_col in result.df.columns

    def test_skip_never_drops_sample_id(self, transform_phen):
        # sample_id passed to skip → no-op for sample_id.
        result = colfilter(transform_phen, skip=[transform_phen.sample_id_col])
        assert transform_phen.sample_id_col in result.df.columns

    def test_role_metadata_filtered(self, transform_phen):
        # Drop GLUCOSE (an outcome) → outcomes should no longer reference it.
        result = colfilter(transform_phen, skip=["GLUCOSE"])
        assert "GLUCOSE" not in result.outcomes
        # AGE / SEX still in covariates.
        assert "AGE" in result.covariates


# ---------------------------------------------------------------------------
# colfilter_min_n
# ---------------------------------------------------------------------------
class TestColfilterMinN:
    def test_drops_columns_below_threshold(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "dense": list(range(10)),                       # 10 non-NaN
                "sparse": [1.0, np.nan, np.nan, np.nan, np.nan,
                           np.nan, np.nan, np.nan, np.nan, np.nan],  # 1 non-NaN
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        result = colfilter_min_n(phen, n=5)
        assert "dense" in result.df.columns
        assert "sparse" not in result.df.columns

    def test_keeps_at_threshold(self, transform_phen):
        # All cols have 7 or 8 non-NaN → n=7 keeps all.
        result = colfilter_min_n(transform_phen, n=7)
        assert "BMI" in result.df.columns
        assert "AGE" in result.df.columns

    def test_invalid_n_raises(self, transform_phen):
        with pytest.raises(ValueError, match="n must"):
            colfilter_min_n(transform_phen, n=-1)


# ---------------------------------------------------------------------------
# colfilter_min_cat_n
# ---------------------------------------------------------------------------
class TestColfilterMinCatN:
    def test_drops_unbalanced_binary(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "balanced": [0, 1] * 5,           # 5 of each
                "unbalanced": [0] * 9 + [1],      # 9 zeros, 1 one
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        result = colfilter_min_cat_n(phen, n=3)
        assert "balanced" in result.df.columns
        assert "unbalanced" not in result.df.columns

    def test_skips_continuous(self, transform_phen):
        # BMI has 8 unique values (continuous) → not filtered.
        result = colfilter_min_cat_n(transform_phen, n=200)
        assert "BMI" in result.df.columns


# ---------------------------------------------------------------------------
# colfilter_percent_zero
# ---------------------------------------------------------------------------
class TestColfilterPercentZero:
    def test_drops_high_zero_columns(self):
        from igem.modules.data import Phenotypes
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(10)],
                "many_zero": [0.0] * 9 + [5.0],   # 90% zero
                "no_zero":   list(range(1, 11)),  # 0% zero
            }
        )
        phen = Phenotypes(df, sample_id_col="sample_id")
        result = colfilter_percent_zero(phen, max_zero_pct=90.0)
        assert "many_zero" not in result.df.columns
        assert "no_zero" in result.df.columns

    def test_skips_non_numeric(self, transform_phen):
        result = colfilter_percent_zero(transform_phen, max_zero_pct=50.0)
        assert transform_phen.sample_id_col in result.df.columns

    def test_invalid_threshold_raises(self, transform_phen):
        with pytest.raises(ValueError, match="max_zero_pct"):
            colfilter_percent_zero(transform_phen, max_zero_pct=-1.0)
        with pytest.raises(ValueError, match="max_zero_pct"):
            colfilter_percent_zero(transform_phen, max_zero_pct=101.0)


# ---------------------------------------------------------------------------
# merge_observations
# ---------------------------------------------------------------------------
class TestMergeObservations:
    def _two_phens(self, n_left=3, n_right=3, sample_id_offset=10):
        from igem.modules.data import Phenotypes
        df_top = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(n_left)],
                "BMI": [22.0 + i for i in range(n_left)],
                "AGE": [40 + i for i in range(n_left)],
            }
        )
        df_bottom = pd.DataFrame(
            {
                "sample_id": [
                    f"S{i + sample_id_offset}" for i in range(n_right)
                ],
                "BMI": [25.0 + i for i in range(n_right)],
                "AGE": [50 + i for i in range(n_right)],
            }
        )
        top = Phenotypes(
            df_top, sample_id_col="sample_id",
            outcomes=["BMI"], covariates=["AGE"],
        )
        bottom = Phenotypes(
            df_bottom, sample_id_col="sample_id",
            outcomes=["BMI"], covariates=["AGE"],
        )
        return top, bottom

    def test_concatenates_rows(self):
        top, bottom = self._two_phens()
        merged = merge_observations(top, bottom)
        assert merged.n_samples == top.n_samples + bottom.n_samples

    def test_keeps_only_common_columns(self):
        from igem.modules.data import Phenotypes
        df_top = pd.DataFrame(
            {"sample_id": ["A", "B"], "X": [1, 2], "ONLY_TOP": [9, 9]}
        )
        df_bottom = pd.DataFrame(
            {"sample_id": ["C", "D"], "X": [3, 4], "ONLY_BOTTOM": [7, 7]}
        )
        top = Phenotypes(df_top, sample_id_col="sample_id")
        bottom = Phenotypes(df_bottom, sample_id_col="sample_id")
        merged = merge_observations(top, bottom)
        assert "X" in merged.df.columns
        assert "ONLY_TOP" not in merged.df.columns
        assert "ONLY_BOTTOM" not in merged.df.columns

    def test_unions_roles(self):
        from igem.modules.data import Phenotypes
        df_top = pd.DataFrame(
            {"sample_id": ["A"], "BMI": [22.0], "GLUCOSE": [88.0]}
        )
        df_bottom = pd.DataFrame(
            {"sample_id": ["B"], "BMI": [28.0], "GLUCOSE": [95.0]}
        )
        top = Phenotypes(
            df_top, sample_id_col="sample_id", outcomes=["BMI"]
        )
        bottom = Phenotypes(
            df_bottom, sample_id_col="sample_id", outcomes=["GLUCOSE"]
        )
        merged = merge_observations(top, bottom)
        # Outcomes of both are unioned.
        assert set(merged.outcomes) == {"BMI", "GLUCOSE"}

    def test_overlapping_sample_ids_raises(self):
        top, _ = self._two_phens()
        # Bottom uses same IDs as top → should raise.
        bottom_overlap, _ = self._two_phens()
        with pytest.raises(ValueError, match="appear in both"):
            merge_observations(top, bottom_overlap)

    def test_sample_id_mismatch_raises(self):
        from igem.modules.data import Phenotypes
        top = Phenotypes(
            pd.DataFrame({"id_a": ["X"], "Y": [1]}), sample_id_col="id_a"
        )
        bottom = Phenotypes(
            pd.DataFrame({"id_b": ["W"], "Y": [2]}), sample_id_col="id_b"
        )
        with pytest.raises(ValueError, match="sample_id_col mismatch"):
            merge_observations(top, bottom)


# ---------------------------------------------------------------------------
# merge_variables
# ---------------------------------------------------------------------------
class TestMergeVariables:
    def test_outer_merge_default(self):
        from igem.modules.data import Phenotypes
        left = Phenotypes(
            pd.DataFrame(
                {"sample_id": ["A", "B"], "BMI": [22.0, 28.0]}
            ),
            sample_id_col="sample_id",
            outcomes=["BMI"],
        )
        right = Phenotypes(
            pd.DataFrame(
                {"sample_id": ["B", "C"], "GLUCOSE": [95.0, 110.0]}
            ),
            sample_id_col="sample_id",
            outcomes=["GLUCOSE"],
        )
        merged = merge_variables(left, right)
        # Outer merge → 3 unique sample_ids: A, B, C.
        assert merged.n_samples == 3
        assert set(merged.outcomes) == {"BMI", "GLUCOSE"}
        # B has both BMI and GLUCOSE, A only BMI, C only GLUCOSE.
        row_b = merged.df[merged.df["sample_id"] == "B"].iloc[0]
        assert row_b["BMI"] == 28.0
        assert row_b["GLUCOSE"] == 95.0

    def test_inner_merge_intersects_samples(self):
        from igem.modules.data import Phenotypes
        left = Phenotypes(
            pd.DataFrame({"sample_id": ["A", "B"], "X": [1, 2]}),
            sample_id_col="sample_id",
        )
        right = Phenotypes(
            pd.DataFrame({"sample_id": ["B", "C"], "Y": [3, 4]}),
            sample_id_col="sample_id",
        )
        merged = merge_variables(left, right, how="inner")
        assert merged.n_samples == 1
        assert merged.df["sample_id"].iloc[0] == "B"

    def test_invalid_how_raises(self):
        from igem.modules.data import Phenotypes
        left = Phenotypes(
            pd.DataFrame({"sample_id": ["A"], "X": [1]}),
            sample_id_col="sample_id",
        )
        right = Phenotypes(
            pd.DataFrame({"sample_id": ["A"], "Y": [2]}),
            sample_id_col="sample_id",
        )
        with pytest.raises(ValueError, match="how must"):
            merge_variables(left, right, how="bogus")


# ---------------------------------------------------------------------------
# rowfilter_incomplete_obs
# ---------------------------------------------------------------------------
class TestRowfilterIncompleteObs:
    def test_drops_rows_with_any_nan(self, transform_phen):
        # transform_phen has 1 NaN in AGE → 1 row dropped.
        result = rowfilter_incomplete_obs(transform_phen)
        assert result.n_samples == 7
        # No NaN remains anywhere.
        assert not result.df.isna().any().any()

    def test_skip_excludes_columns_from_check(self, transform_phen):
        # Skip AGE → no NaN in remaining cols → no rows dropped.
        result = rowfilter_incomplete_obs(transform_phen, skip=["AGE"])
        assert result.n_samples == transform_phen.n_samples

    def test_only_restricts_scope(self, transform_phen):
        # Only BMI → no NaN there → no rows dropped.
        result = rowfilter_incomplete_obs(transform_phen, only=["BMI"])
        assert result.n_samples == transform_phen.n_samples


# ---------------------------------------------------------------------------
# move_variables
# ---------------------------------------------------------------------------
class TestMoveVariables:
    def _src_dst_pair(self):
        from igem.modules.data import Phenotypes
        ids = [f"S{i}" for i in range(4)]
        src = Phenotypes(
            pd.DataFrame(
                {
                    "sample_id": ids,
                    "BMI": [22.0, 25.0, 28.0, 31.0],
                    "QUESTIONNAIRE_X": [1, 2, 1, 2],
                    "QUESTIONNAIRE_Y": [3, 4, 3, 4],
                }
            ),
            sample_id_col="sample_id",
            outcomes=["BMI"],
        )
        dst = Phenotypes(
            pd.DataFrame({"sample_id": ids, "AGE": [40, 45, 50, 55]}),
            sample_id_col="sample_id",
            covariates=["AGE"],
        )
        return src, dst

    def test_only_moves_listed(self):
        src, dst = self._src_dst_pair()
        new_src, new_dst = move_variables(
            src, dst, only=["QUESTIONNAIRE_X"],
        )
        assert "QUESTIONNAIRE_X" not in new_src.df.columns
        assert "QUESTIONNAIRE_X" in new_dst.df.columns
        # Untouched column stays in src.
        assert "QUESTIONNAIRE_Y" in new_src.df.columns

    def test_skip_excludes_from_move(self):
        src, dst = self._src_dst_pair()
        new_src, new_dst = move_variables(
            src, dst, skip=["BMI"],
        )
        # BMI stays in src (not moved).
        assert "BMI" in new_src.df.columns
        # Other columns migrated.
        assert "QUESTIONNAIRE_X" in new_dst.df.columns
        assert "QUESTIONNAIRE_Y" in new_dst.df.columns

    def test_sample_id_mismatch_raises(self):
        from igem.modules.data import Phenotypes
        src = Phenotypes(
            pd.DataFrame({"id_a": ["A"], "X": [1]}), sample_id_col="id_a"
        )
        dst = Phenotypes(
            pd.DataFrame({"id_b": ["A"], "Y": [2]}), sample_id_col="id_b"
        )
        with pytest.raises(ValueError, match="sample_id_col mismatch"):
            move_variables(src, dst)

    def test_misaligned_samples_raises(self):
        from igem.modules.data import Phenotypes
        src = Phenotypes(
            pd.DataFrame({"sample_id": ["A", "B"], "X": [1, 2]}),
            sample_id_col="sample_id",
        )
        dst = Phenotypes(
            pd.DataFrame({"sample_id": ["B", "A"], "Y": [3, 4]}),
            sample_id_col="sample_id",
        )
        with pytest.raises(ValueError, match="matching sample IDs"):
            move_variables(src, dst)

    def test_role_metadata_filtered_on_src(self):
        # Move BMI (an outcome) → outcomes list on new_src drops "BMI".
        src, dst = self._src_dst_pair()
        new_src, new_dst = move_variables(src, dst, only=["BMI"])
        assert "BMI" not in new_src.outcomes
