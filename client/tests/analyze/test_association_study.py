"""Tests for igem.modules.analyze.association_study (unified entrypoint)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem.modules.analyze import RegressionResults, association_study
from igem.modules.analyze.results import ASSOCIATION_RESULT_COLUMNS
from igem.modules.data import Phenotypes


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def assoc_phen() -> Phenotypes:
    """
    Phenotype frame with known structure:
      - GLUCOSE = 80 + 0.8*BMI + noise (linear, strong effect)
      - CASE   ~ Bernoulli(σ(BMI))     (logistic)
      - AGE confounder, SEX/RACE categorical
    """
    rng = np.random.default_rng(7)
    n = 400
    age = rng.integers(20, 80, n)
    bmi = rng.normal(25, 5, n)
    glucose = 80 + 0.8 * bmi + rng.normal(0, 5, n) + 0.1 * age
    logit = -10 + 0.4 * bmi
    case = (rng.uniform(size=n) < 1.0 / (1.0 + np.exp(-logit))).astype(int)
    sex = rng.choice(["M", "F"], n)
    race = rng.choice(["A", "B", "C"], n)

    df = pd.DataFrame(
        {
            "sample_id": [f"S{i:04d}" for i in range(n)],
            "BMI": bmi,
            "AGE": age,
            "GLUCOSE": glucose,
            "CASE": case,
            "SEX": sex,
            "RACE": race,
        }
    )
    return Phenotypes(
        df, sample_id_col="sample_id",
        outcomes=["GLUCOSE", "CASE"],
        covariates=["AGE"],
        exposures=["BMI"],
    )


# ---------------------------------------------------------------------------
# Single outcome / single regressor — basic sanity
# ---------------------------------------------------------------------------
class TestBasicAssociation:
    def test_returns_canonical_schema(self, assoc_phen):
        res = association_study(
            assoc_phen, "GLUCOSE", ["BMI"], min_n=10, progress=False,
        )
        assert isinstance(res, RegressionResults)
        for col in ASSOCIATION_RESULT_COLUMNS:
            assert col in res.df.columns, f"missing canonical col: {col}"

    def test_recovers_known_linear_effect(self, assoc_phen):
        res = association_study(
            assoc_phen, "GLUCOSE", ["BMI"], min_n=10, progress=False,
        )
        row = res.df.iloc[0]
        # True effect is 0.8 with σ≈5 noise on n=400 → ~95% CI is ±0.2.
        assert row["beta"] == pytest.approx(0.8, abs=0.2)
        assert row["beta_pvalue"] < 1e-10
        assert row["lrt_pvalue"] < 1e-10
        assert bool(row["converged"]) is True

    def test_logistic_auto_detected(self, assoc_phen):
        res = association_study(
            assoc_phen, "CASE", ["BMI"], min_n=10, progress=False,
        )
        assert res.family == "logistic"
        assert res.df.iloc[0]["beta_pvalue"] < 0.001

    def test_default_regressors_use_phen_exposures(self, assoc_phen):
        res = association_study(
            assoc_phen, "GLUCOSE", min_n=10, progress=False,
        )
        # phen.exposures = ["BMI"]
        assert set(res.df["variable"]) == {"BMI"}


# ---------------------------------------------------------------------------
# Multi-outcome and multi-regressor
# ---------------------------------------------------------------------------
class TestMultipleOutcomesAndRegressors:
    def test_multi_outcome(self, assoc_phen):
        res = association_study(
            assoc_phen, ["GLUCOSE", "CASE"], ["BMI"], min_n=10, progress=False,
        )
        # 2 outcomes × 1 regressor = 2 rows.
        assert len(res.df) == 2
        assert set(res.df["outcome"]) == {"GLUCOSE", "CASE"}

    def test_multi_regressor(self, assoc_phen):
        res = association_study(
            assoc_phen, "GLUCOSE", ["BMI", "AGE"],
            covariates=[],   # no covariates so AGE can be a regressor
            min_n=10, progress=False,
        )
        assert set(res.df["variable"]) == {"BMI", "AGE"}

    def test_multi_outcome_multi_regressor_grid(self, assoc_phen):
        res = association_study(
            assoc_phen, ["GLUCOSE", "CASE"], ["BMI", "AGE"],
            covariates=[],
            min_n=10, progress=False,
        )
        # 2 × 2 = 4 rows.
        assert len(res.df) == 4

    def test_per_outcome_correction(self, assoc_phen):
        res = association_study(
            assoc_phen, ["GLUCOSE", "CASE"], ["BMI"],
            min_n=10, progress=False,
        ).with_correction("bonferroni", groupby="outcome")
        # Each outcome has 1 test → corrected p == raw p.
        assert (res.df["p_corrected"] == res.df["beta_pvalue"]).all()


# ---------------------------------------------------------------------------
# Categorical regressor
# ---------------------------------------------------------------------------
class TestCategoricalRegressor:
    def test_categorical_emits_summary_row(self, assoc_phen):
        res = association_study(
            assoc_phen, "GLUCOSE", ["RACE"],
            covariates=[], min_n=10, progress=False,
        )
        # Default report_categorical_betas=False → 1 summary row.
        assert len(res.df) == 1
        row = res.df.iloc[0]
        assert row["variable"] == "RACE"
        assert row["variable_type"] == "categorical"
        # Summary row has NaN beta but a valid LRT p-value.
        assert np.isnan(row["beta"])
        assert not np.isnan(row["lrt_pvalue"])

    def test_report_categorical_betas_emits_dummies(self, assoc_phen):
        res = association_study(
            assoc_phen, "GLUCOSE", ["RACE"],
            covariates=[], min_n=10, progress=False,
            report_categorical_betas=True,
        )
        # Summary row + per-dummy rows (RACE has 3 levels → 2 dummies).
        assert len(res.df) >= 3
        types = set(res.df["variable_type"])
        assert "categorical" in types
        assert "categorical_dummy" in types


# ---------------------------------------------------------------------------
# min_n filter
# ---------------------------------------------------------------------------
class TestMinN:
    def test_min_n_drops_small_subsets(self, assoc_phen):
        # min_n very high (above n_samples) → all regressions land in errors.
        res = association_study(
            assoc_phen, "GLUCOSE", ["BMI"],
            min_n=10_000, progress=False,
        )
        assert res.n_tests == 0
        assert res.n_errors == 1
        assert "insufficient" in res.errors.iloc[0]["error"].lower()

    def test_invalid_min_n_raises(self, assoc_phen):
        with pytest.raises(ValueError, match="min_n"):
            association_study(
                assoc_phen, "GLUCOSE", ["BMI"], min_n=0, progress=False,
            )


# ---------------------------------------------------------------------------
# standardize_data
# ---------------------------------------------------------------------------
class TestStandardize:
    def test_standardize_changes_beta_magnitude(self, assoc_phen):
        plain = association_study(
            assoc_phen, "GLUCOSE", ["BMI"], min_n=10, progress=False,
        )
        std = association_study(
            assoc_phen, "GLUCOSE", ["BMI"],
            min_n=10, progress=False, standardize_data=True,
        )
        # Original BMI (std≈5) → standardized BMI (std=1).
        # Standardized beta ≈ raw_beta * raw_std → ~5x larger.
        assert std.df.iloc[0]["beta"] > plain.df.iloc[0]["beta"] * 3.5
        # P-values should be virtually identical (same signal).
        assert std.df.iloc[0]["beta_pvalue"] == pytest.approx(
            plain.df.iloc[0]["beta_pvalue"], rel=1e-6,
        )


# ---------------------------------------------------------------------------
# Parallelism
# ---------------------------------------------------------------------------
class TestParallel:
    def test_n_jobs_minus_one_matches_sequential(self, assoc_phen):
        # Many tasks for joblib to actually parallelise.
        seq = association_study(
            assoc_phen, ["GLUCOSE", "CASE"], ["BMI", "AGE"],
            covariates=[], min_n=10, progress=False, n_jobs=1,
        )
        par = association_study(
            assoc_phen, ["GLUCOSE", "CASE"], ["BMI", "AGE"],
            covariates=[], min_n=10, progress=False, n_jobs=-1,
        )
        # Same shape.
        assert len(seq.df) == len(par.df)
        # Same variables / outcomes (order may differ).
        assert set(zip(seq.df["outcome"], seq.df["variable"])) == set(
            zip(par.df["outcome"], par.df["variable"])
        )

    def test_n_jobs_zero_raises(self, assoc_phen):
        with pytest.raises(ValueError, match="n_jobs"):
            association_study(
                assoc_phen, "GLUCOSE", ["BMI"], min_n=10, n_jobs=0,
            )


# ---------------------------------------------------------------------------
# Survey-aware
# ---------------------------------------------------------------------------
class TestSurveyAware:
    def test_use_survey_requires_weights_col(self, assoc_phen):
        with pytest.raises(ValueError, match="weights_col"):
            association_study(
                assoc_phen, "GLUCOSE", ["BMI"],
                use_survey=True, min_n=10, progress=False,
            )

    def test_survey_path_runs_with_weights(self):
        rng = np.random.default_rng(4)
        n = 300
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(n)],
                "BMI": rng.normal(25, 5, n),
                "GLUCOSE": rng.normal(100, 15, n),
                "WT": rng.uniform(0.5, 2.0, n),
            }
        )
        phen = Phenotypes(
            df, sample_id_col="sample_id",
            outcomes=["GLUCOSE"], exposures=["BMI"],
            weights_col="WT",
        )
        res = association_study(
            phen, "GLUCOSE", ["BMI"],
            use_survey=True, min_n=10, progress=False,
        )
        assert res.n_tests == 1
        # Survey config is flattened into metadata via make_metadata's extras.
        assert res.metadata["survey"]["weights_col"] == "WT"


# ---------------------------------------------------------------------------
# Genotype regressors with encoding
# ---------------------------------------------------------------------------
class TestGenotypeRegressors:
    def _phen_geno_pair(self):
        from igem.modules.data import Genotypes
        import xarray as xr

        rng = np.random.default_rng(1)
        n_samples, n_variants = 200, 3
        sample_ids = [f"S{i:03d}" for i in range(n_samples)]
        variant_ids = [f"v{i}" for i in range(n_variants)]

        # Random dosages 0/1/2 with no missing.
        gt = rng.integers(0, 2, size=(n_variants, n_samples, 2)).astype(np.int8)
        # Build a known effect: phenotype Y depends on v0 dosage (additive).
        dosage = gt.sum(axis=-1)   # (variants, samples)
        y = 50 + 2.0 * dosage[0, :] + rng.normal(0, 1, n_samples)

        alleles = np.array([[b"A", b"C"]] * n_variants, dtype="|S1")
        ds = xr.Dataset(
            {
                "call_genotype": (
                    ("variants", "samples", "ploidy"), gt,
                ),
                "variant_allele": (("variants", "alleles"), alleles),
                "variant_id": (
                    "variants",
                    np.array(variant_ids, dtype=object),
                ),
                "sample_id": (
                    "samples",
                    np.array(sample_ids, dtype=object),
                ),
                "variant_contig": (
                    "variants", np.zeros(n_variants, dtype=np.int32),
                ),
                "variant_position": (
                    "variants", np.arange(n_variants, dtype=np.int32),
                ),
            },
            attrs={"contigs": ["1"]},
        )
        geno = Genotypes(ds)
        phen_df = pd.DataFrame(
            {"sample_id": sample_ids, "Y": y}
        )
        phen = Phenotypes(
            phen_df, sample_id_col="sample_id", outcomes=["Y"],
        )
        return phen, geno

    def test_additive_recovers_known_effect(self):
        phen, geno = self._phen_geno_pair()
        res = association_study(
            phen, "Y", regression_variables=["v0"], geno=geno,
            covariates=[], min_n=10, progress=False,
        )
        row = res.df.iloc[0]
        # True effect: 2.0 per dosage unit.
        assert row["beta"] == pytest.approx(2.0, abs=0.3)
        assert row["beta_pvalue"] < 1e-10
        assert row["variable_type"] == "genotype"

    def test_dominant_encoding_runs(self):
        phen, geno = self._phen_geno_pair()
        res = association_study(
            phen, "Y", regression_variables=["v0", "v1"], geno=geno,
            encoding="dominant",
            covariates=[], min_n=10, progress=False,
        )
        assert len(res.df) == 2
        assert all(res.df["variable_type"] == "genotype")

    def test_recessive_encoding_runs(self):
        phen, geno = self._phen_geno_pair()
        res = association_study(
            phen, "Y", regression_variables=["v0"], geno=geno,
            encoding="recessive",
            covariates=[], min_n=10, progress=False,
        )
        assert len(res.df) == 1

    def test_codominant_emits_summary_row(self):
        phen, geno = self._phen_geno_pair()
        res = association_study(
            phen, "Y", regression_variables=["v0"], geno=geno,
            encoding="codominant",
            covariates=[], min_n=10, progress=False,
        )
        # Summary row only (report_categorical_betas=False default).
        summary = res.df[res.df["variable"] == "v0"]
        assert len(summary) == 1
        assert np.isnan(summary.iloc[0]["beta"])     # multi-term has no single beta
        assert not np.isnan(summary.iloc[0]["lrt_pvalue"])

    def test_codominant_with_report_betas_emits_dummies(self):
        phen, geno = self._phen_geno_pair()
        res = association_study(
            phen, "Y", regression_variables=["v0"], geno=geno,
            encoding="codominant",
            covariates=[], min_n=10, progress=False,
            report_categorical_betas=True,
        )
        # Summary + 2 dummy rows (het, hom_alt).
        assert len(res.df) == 3

    def test_edge_encoding_requires_info(self):
        phen, geno = self._phen_geno_pair()
        with pytest.raises(ValueError, match="edge_encoding_info"):
            association_study(
                phen, "Y", regression_variables=["v0"], geno=geno,
                encoding="edge",
                covariates=[], min_n=10, progress=False,
            )

    def test_edge_encoding_with_info_runs(self):
        phen, geno = self._phen_geno_pair()
        info = pd.DataFrame(
            {"score_0": [0.0, 0.0, 0.0],
             "score_1": [1.0, 0.5, 0.5],
             "score_2": [2.0, 1.0, 1.0]},
            index=["v0", "v1", "v2"],
        )
        res = association_study(
            phen, "Y", regression_variables=["v0"], geno=geno,
            encoding="edge", edge_encoding_info=info,
            covariates=[], min_n=10, progress=False,
        )
        assert res.n_tests == 1


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class TestValidation:
    def test_unknown_outcome_raises(self, assoc_phen):
        with pytest.raises(ValueError, match="outcomes"):
            association_study(
                assoc_phen, "not_a_col", ["BMI"], progress=False,
            )

    def test_unknown_regressor_raises(self, assoc_phen):
        with pytest.raises(ValueError, match="regression_variables"):
            association_study(
                assoc_phen, "GLUCOSE", ["not_a_col"], progress=False,
            )

    def test_unknown_covariate_raises(self, assoc_phen):
        with pytest.raises(ValueError, match="covariates"):
            association_study(
                assoc_phen, "GLUCOSE", ["BMI"],
                covariates=["not_a_col"], progress=False,
            )

    def test_unknown_regression_kind_raises(self, assoc_phen):
        with pytest.raises(ValueError, match="regression_kind"):
            association_study(
                assoc_phen, "GLUCOSE", ["BMI"],
                regression_kind="bogus", progress=False,
            )
