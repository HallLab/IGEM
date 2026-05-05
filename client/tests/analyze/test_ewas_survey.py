"""Tests for the survey-aware mode of igem.modules.analyze.ewas."""
from __future__ import annotations

import warnings

import pytest

from igem.modules.analyze import ewas
from igem.modules.data import Phenotypes


class TestSurveyEnabling:
    def test_metadata_records_survey_columns(self, survey_phen):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            res = ewas(
                survey_phen, "GLUCOSE", use_survey=True, progress=False,
            )
        assert "survey" in res.metadata
        sv = res.metadata["survey"]
        assert sv["weights_col"] == "WTMEC"
        assert sv["cluster_col"] == "SDMVPSU"
        assert sv["strata_col"] == "SDMVSTRA"
        assert sv["strata_used_for_variance"] is False

    def test_use_survey_without_weights_col_raises(self, linear_phen):
        # linear_phen has no weights_col set.
        with pytest.raises(ValueError, match="weights_col"):
            ewas(linear_phen, "GLUCOSE", use_survey=True, progress=False)

    def test_strata_set_emits_warning(self, survey_phen):
        with pytest.warns(UserWarning, match="strata"):
            ewas(survey_phen, "GLUCOSE", use_survey=True, progress=False)


class TestSurveyChangesEstimate:
    def _bmi_row(self, res):
        return res.df.set_index("variable").loc["BMI"]

    def test_survey_se_differs_from_unweighted(self, survey_phen):
        unweighted = ewas(
            survey_phen, "GLUCOSE", use_survey=False, progress=False,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            weighted = ewas(
                survey_phen, "GLUCOSE", use_survey=True, progress=False,
            )
        # Cluster-robust SEs from the weighted fit must not equal the
        # unweighted OLS SEs (the variance estimator is different).
        assert self._bmi_row(unweighted)["se"] != pytest.approx(
            self._bmi_row(weighted)["se"], rel=1e-3
        )

    def test_p_value_finite(self, survey_phen):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            res = ewas(
                survey_phen, "GLUCOSE", use_survey=True, progress=False,
            )
        bmi = self._bmi_row(res)
        assert bmi["p_value"] >= 0.0
        assert bmi["p_value"] < 1e-6  # BMI's effect was 2.0, n=150


class TestSurveyOnlyWeightsNoCluster:
    def test_runs_when_cluster_col_is_none(self, survey_phen):
        # Manually drop the cluster_col while keeping weights_col.
        df = survey_phen.df.copy()
        phen = Phenotypes(
            df,
            sample_id_col=survey_phen.sample_id_col,
            outcomes=survey_phen.outcomes,
            covariates=survey_phen.covariates,
            exposures=survey_phen.exposures,
            weights_col=survey_phen.weights_col,
            cluster_col=None,
            strata_col=None,
        )
        res = ewas(phen, "GLUCOSE", use_survey=True, progress=False)
        bmi = res.df.set_index("variable").loc["BMI"]
        assert bmi["p_value"] < 1e-6
