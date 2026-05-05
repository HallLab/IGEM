"""Tests for igem.modules.modify.phenotypes."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem.modules.modify import categorize, drop_missing, recode


# ---------------------------------------------------------------------------
# categorize
# ---------------------------------------------------------------------------
class TestCategorize:
    def test_quantile_bins_create_new_column(self, transform_phen):
        result = categorize(transform_phen, "BMI", method="quantiles", n_bins=2)
        assert "BMI_cat" in result.df.columns
        assert "BMI" in result.df.columns  # original kept
        assert result.df["BMI_cat"].nunique(dropna=True) == 2

    def test_quantile_bins_with_labels(self, transform_phen):
        result = categorize(
            transform_phen, "BMI",
            method="quantiles", n_bins=2,
            labels=["low", "high"],
        )
        cats = set(result.df["BMI_cat"].dropna().astype(str).unique())
        assert cats == {"low", "high"}

    def test_explicit_bin_edges(self, transform_phen):
        # BMI values: 18, 21, 24, 27, 30, 33, 36, 39
        result = categorize(
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
        result = categorize(
            transform_phen, "BMI", method="quantiles", n_bins=2,
            replace=True,
        )
        # Replaced: "BMI" now holds categories, no "BMI_cat" column.
        assert "BMI_cat" not in result.df.columns
        assert result.df["BMI"].nunique(dropna=True) == 2

    def test_preserves_role_metadata(self, transform_phen):
        result = categorize(transform_phen, "BMI", method="quantiles", n_bins=2)
        assert result.outcomes == transform_phen.outcomes
        assert result.covariates == transform_phen.covariates
        assert result.exposures == transform_phen.exposures
        assert result.sample_id_col == transform_phen.sample_id_col

    def test_unknown_column_raises(self, transform_phen):
        with pytest.raises(ValueError, match="not_a_col"):
            categorize(transform_phen, "not_a_col", n_bins=2)

    def test_unknown_method_raises(self, transform_phen):
        with pytest.raises(ValueError, match="method"):
            categorize(transform_phen, "BMI", method="bogus")


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
