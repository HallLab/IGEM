"""Tests for igem.modules.data.phenotypes (Phenotypes class + readers)."""
from __future__ import annotations

import pandas as pd
import pytest

from igem.modules.data import Phenotypes, read_phenotypes


# ---------------------------------------------------------------------------
# Phenotypes class
# ---------------------------------------------------------------------------
class TestPhenotypesInit:
    def test_full_role_metadata(self, nhanes_phen_df):
        phen = Phenotypes(
            nhanes_phen_df,
            sample_id_col="SEQN",
            outcomes=["GLUCOSE"],
            covariates=["AGE", "SEX"],
            exposures=["BMI"],
            weights_col="WTMEC",
            strata_col="SDMVSTRA",
            cluster_col="SDMVPSU",
        )
        assert phen.n_samples == 4
        assert phen.outcomes == ["GLUCOSE"]
        assert phen.covariates == ["AGE", "SEX"]
        assert phen.exposures == ["BMI"]
        assert phen.weights_col == "WTMEC"
        assert phen.strata_col == "SDMVSTRA"
        assert phen.cluster_col == "SDMVPSU"

    def test_default_roles_are_empty(self, nhanes_phen_df):
        df = nhanes_phen_df.rename(columns={"SEQN": "sample_id"})
        phen = Phenotypes(df)
        assert phen.outcomes == []
        assert phen.covariates == []
        assert phen.exposures == []
        assert phen.weights_col is None
        assert phen.strata_col is None
        assert phen.cluster_col is None

    def test_input_dataframe_is_copied(self, nhanes_phen_df):
        phen = Phenotypes(nhanes_phen_df, sample_id_col="SEQN")
        # Mutating the wrapper's frame should not bleed back into the input.
        phen.df.loc[:, "AGE"] = 0
        assert nhanes_phen_df.loc[0, "AGE"] == 45


class TestPhenotypesValidation:
    def test_rejects_missing_sample_id_col(self, nhanes_phen_df):
        with pytest.raises(ValueError, match="sample_id_col"):
            Phenotypes(nhanes_phen_df, sample_id_col="NOT_A_COLUMN")

    def test_rejects_unknown_outcome_column(self, nhanes_phen_df):
        with pytest.raises(ValueError, match="does_not_exist"):
            Phenotypes(
                nhanes_phen_df,
                sample_id_col="SEQN",
                outcomes=["does_not_exist"],
            )

    def test_rejects_unknown_covariate_column(self, nhanes_phen_df):
        with pytest.raises(ValueError, match="missing_cov"):
            Phenotypes(
                nhanes_phen_df,
                sample_id_col="SEQN",
                covariates=["AGE", "missing_cov"],
            )

    def test_rejects_unknown_weights_col(self, nhanes_phen_df):
        with pytest.raises(ValueError, match="weights_col"):
            Phenotypes(
                nhanes_phen_df,
                sample_id_col="SEQN",
                weights_col="NOT_A_WEIGHT",
            )

    def test_rejects_unknown_strata_col(self, nhanes_phen_df):
        with pytest.raises(ValueError, match="strata_col"):
            Phenotypes(
                nhanes_phen_df,
                sample_id_col="SEQN",
                strata_col="NOPE",
            )


class TestPhenotypesAccessors:
    def test_samples_named_index(self, nhanes_phen_df):
        samples = Phenotypes(nhanes_phen_df, sample_id_col="SEQN").samples
        assert isinstance(samples, pd.Index)
        assert samples.name == "SEQN"
        assert list(samples) == ["S000", "S001", "S002", "S003"]

    def test_outcomes_df_includes_sample_id(self, nhanes_phen_df):
        phen = Phenotypes(
            nhanes_phen_df,
            sample_id_col="SEQN",
            outcomes=["GLUCOSE"],
        )
        out = phen.outcomes_df()
        assert list(out.columns) == ["SEQN", "GLUCOSE"]
        assert len(out) == 4

    def test_covariates_and_exposures_dfs(self, nhanes_phen_df):
        phen = Phenotypes(
            nhanes_phen_df,
            sample_id_col="SEQN",
            covariates=["AGE", "SEX"],
            exposures=["BMI"],
        )
        assert list(phen.covariates_df().columns) == ["SEQN", "AGE", "SEX"]
        assert list(phen.exposures_df().columns) == ["SEQN", "BMI"]

    def test_df_returns_underlying_frame(self, nhanes_phen_df):
        phen = Phenotypes(nhanes_phen_df, sample_id_col="SEQN")
        assert isinstance(phen.df, pd.DataFrame)
        assert len(phen.df) == 4

    def test_repr_includes_role_counts(self, nhanes_phen_df):
        phen = Phenotypes(
            nhanes_phen_df,
            sample_id_col="SEQN",
            outcomes=["GLUCOSE"],
            covariates=["AGE", "SEX"],
            weights_col="WTMEC",
        )
        text = repr(phen)
        assert "n_samples=4" in text
        assert "outcomes=1" in text
        assert "covariates=2" in text
        assert "weights" in text


class TestPhenotypesSelectSamples:
    def test_select_subset_preserves_metadata(self, nhanes_phen_df):
        phen = Phenotypes(
            nhanes_phen_df,
            sample_id_col="SEQN",
            outcomes=["GLUCOSE"],
            covariates=["AGE"],
            exposures=["BMI"],
            weights_col="WTMEC",
            strata_col="SDMVSTRA",
            cluster_col="SDMVPSU",
        )
        sub = phen.select_samples(["S001", "S003"])
        assert sub.n_samples == 2
        assert list(sub.samples) == ["S001", "S003"]
        assert sub.outcomes == ["GLUCOSE"]
        assert sub.covariates == ["AGE"]
        assert sub.exposures == ["BMI"]
        assert sub.weights_col == "WTMEC"
        assert sub.strata_col == "SDMVSTRA"
        assert sub.cluster_col == "SDMVPSU"

    def test_select_unknown_samples_returns_empty(self, nhanes_phen_df):
        phen = Phenotypes(nhanes_phen_df, sample_id_col="SEQN")
        sub = phen.select_samples(["NON_EXISTENT"])
        assert sub.n_samples == 0


# ---------------------------------------------------------------------------
# read_phenotypes
# ---------------------------------------------------------------------------
class TestReadPhenotypes:
    def test_from_dataframe(self, nhanes_phen_df):
        phen = read_phenotypes(
            nhanes_phen_df,
            sample_id_col="SEQN",
            outcomes=["GLUCOSE"],
        )
        assert isinstance(phen, Phenotypes)
        assert phen.n_samples == 4
        assert phen.outcomes == ["GLUCOSE"]

    def test_from_csv(self, tmp_path, nhanes_phen_df):
        path = tmp_path / "phen.csv"
        nhanes_phen_df.to_csv(path, index=False)
        phen = read_phenotypes(
            path,
            sample_id_col="SEQN",
            covariates=["AGE", "SEX"],
        )
        assert phen.n_samples == 4
        assert phen.covariates == ["AGE", "SEX"]

    def test_from_tsv_extension(self, tmp_path, nhanes_phen_df):
        path = tmp_path / "phen.tsv"
        nhanes_phen_df.to_csv(path, sep="\t", index=False)
        phen = read_phenotypes(path, sample_id_col="SEQN")
        assert phen.n_samples == 4
        assert "AGE" in phen.df.columns

    def test_unknown_outcome_raises(self, nhanes_phen_df):
        with pytest.raises(ValueError, match="does_not_exist"):
            read_phenotypes(
                nhanes_phen_df,
                sample_id_col="SEQN",
                outcomes=["does_not_exist"],
            )

    def test_accepts_string_path(self, tmp_path, nhanes_phen_df):
        path = tmp_path / "phen.csv"
        nhanes_phen_df.to_csv(path, index=False)
        phen = read_phenotypes(str(path), sample_id_col="SEQN")
        assert phen.n_samples == 4
