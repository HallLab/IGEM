"""Tests for igem.modules.analyze.interaction_study (pairwise LRT)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem.modules.analyze import RegressionResults, interaction_study
from igem.modules.data import Phenotypes


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def interaction_phen() -> Phenotypes:
    """
    500 samples with a strong V1×V2 interaction injected and one null
    third variable.

        Y = V1 + V2 + 0.8 * V1*V2 + noise
    """
    rng = np.random.default_rng(11)
    n = 500
    v1 = rng.normal(0, 1, n)
    v2 = rng.normal(0, 1, n)
    null_x = rng.normal(0, 1, n)
    y = v1 + v2 + 0.8 * v1 * v2 + rng.normal(0, 0.5, n)

    df = pd.DataFrame(
        {
            "sample_id": [f"S{i:04d}" for i in range(n)],
            "V1": v1, "V2": v2, "NULL_X": null_x,
            "AGE": rng.integers(20, 80, n),
            "Y": y,
        }
    )
    return Phenotypes(
        df, sample_id_col="sample_id",
        outcomes=["Y"],
        covariates=["AGE"],
        exposures=["V1", "V2", "NULL_X"],
    )


# ---------------------------------------------------------------------------
# Basic interaction detection
# ---------------------------------------------------------------------------
class TestInteractionDetection:
    def test_returns_regression_results(self, interaction_phen):
        res = interaction_study(
            interaction_phen, "Y",
            covariates=[], min_n=10, progress=False,
        )
        assert isinstance(res, RegressionResults)

    def test_default_pairs_use_all_exposures(self, interaction_phen):
        # phen.exposures = [V1, V2, NULL_X] → 3 unordered pairs.
        res = interaction_study(
            interaction_phen, "Y",
            covariates=[], min_n=10, progress=False,
        )
        assert res.n_tests == 3

    def test_true_interaction_detected(self, interaction_phen):
        res = interaction_study(
            interaction_phen, "Y",
            covariates=[], min_n=10, progress=False,
        )
        d = res.df.set_index(["term1", "term2"])
        # V1×V2 was injected with effect 0.8 → highly significant.
        v1v2 = d.loc[("V1", "V2")]
        assert v1v2["lrt_pvalue"] < 1e-10
        # V1×NULL_X and V2×NULL_X are noise.
        assert d.loc[("V1", "NULL_X"), "lrt_pvalue"] > 0.05
        assert d.loc[("V2", "NULL_X"), "lrt_pvalue"] > 0.05

    def test_lrt_df_is_one_for_continuous_pair(self, interaction_phen):
        res = interaction_study(
            interaction_phen, "Y",
            covariates=[], min_n=10, progress=False,
        )
        # All exposures are continuous → df should be 1 for every pair.
        assert (res.df["lrt_df"] == 1).all()


# ---------------------------------------------------------------------------
# Interaction input forms
# ---------------------------------------------------------------------------
class TestInteractionsArg:
    def test_anchor_string_pairs_with_others(self, interaction_phen):
        # interactions="V1" → (V1, V2), (V1, NULL_X) — 2 pairs.
        res = interaction_study(
            interaction_phen, "Y", interactions="V1",
            covariates=[], min_n=10, progress=False,
        )
        assert res.n_tests == 2
        terms = set(zip(res.df["term1"], res.df["term2"]))
        assert ("V1", "V2") in terms
        assert ("V1", "NULL_X") in terms

    def test_explicit_tuples(self, interaction_phen):
        res = interaction_study(
            interaction_phen, "Y",
            interactions=[("V1", "V2")],
            covariates=[], min_n=10, progress=False,
        )
        assert res.n_tests == 1
        row = res.df.iloc[0]
        assert (row["term1"], row["term2"]) == ("V1", "V2")

    def test_invalid_tuple_raises(self, interaction_phen):
        with pytest.raises(ValueError, match="tuple"):
            interaction_study(
                interaction_phen, "Y",
                interactions=["not_a_tuple"],
                progress=False,
            )

    def test_max_pairs_cap(self, interaction_phen):
        with pytest.raises(ValueError, match="max_pairs"):
            interaction_study(
                interaction_phen, "Y",
                covariates=[], min_n=10, max_pairs=2, progress=False,
            )


# ---------------------------------------------------------------------------
# Multi-outcome
# ---------------------------------------------------------------------------
class TestMultiOutcome:
    def test_pairs_grow_per_outcome(self, interaction_phen):
        # Add a second outcome.
        df = interaction_phen.df.copy()
        df["Y2"] = df["Y"] + np.random.default_rng(0).normal(0, 2, len(df))
        from igem.modules.data import Phenotypes
        phen = Phenotypes(
            df, sample_id_col=interaction_phen.sample_id_col,
            outcomes=["Y", "Y2"],
            covariates=interaction_phen.covariates,
            exposures=interaction_phen.exposures,
        )
        res = interaction_study(
            phen, ["Y", "Y2"], interactions=[("V1", "V2")],
            covariates=[], min_n=10, progress=False,
        )
        # 2 outcomes × 1 pair = 2 rows.
        assert res.n_tests == 2
        assert set(res.df["outcome"]) == {"Y", "Y2"}


# ---------------------------------------------------------------------------
# report_betas
# ---------------------------------------------------------------------------
class TestReportBetas:
    def test_default_only_summary(self, interaction_phen):
        res = interaction_study(
            interaction_phen, "Y",
            interactions=[("V1", "V2")],
            covariates=[], min_n=10, progress=False,
        )
        assert res.n_tests == 1
        assert "term_beta" not in res.df.columns

    def test_report_betas_emits_extra_rows(self, interaction_phen):
        res = interaction_study(
            interaction_phen, "Y",
            interactions=[("V1", "V2")],
            covariates=[], min_n=10, progress=False,
            report_betas=True,
        )
        # Summary row + 1 dummy row (V1×V2 continuous → 1 product term).
        assert res.n_tests == 2
        # Beta row populated for the interaction term.
        beta_rows = res.df[res.df.get("term_beta", pd.Series()).notna()]
        assert len(beta_rows) == 1


# ---------------------------------------------------------------------------
# Categorical interaction
# ---------------------------------------------------------------------------
class TestCategoricalInteraction:
    def test_categorical_pair_emits_correct_df(self):
        from igem.modules.data import Phenotypes
        rng = np.random.default_rng(17)
        n = 400
        sex = rng.choice(["M", "F"], n)
        smoke = rng.choice(["yes", "no"], n)
        # No interaction signal; just structural test.
        y = rng.normal(0, 1, n)
        df = pd.DataFrame(
            {
                "sample_id": [f"S{i}" for i in range(n)],
                "SEX": sex, "SMOKE": smoke,
                "Y": y,
            }
        )
        phen = Phenotypes(
            df, sample_id_col="sample_id",
            outcomes=["Y"], exposures=["SEX", "SMOKE"],
        )
        res = interaction_study(
            phen, "Y", covariates=[], min_n=10, progress=False,
        )
        # 1 pair (SEX, SMOKE), each binary → df = (2-1)*(2-1) = 1.
        assert res.n_tests == 1
        assert int(res.df.iloc[0]["lrt_df"]) == 1


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class TestValidation:
    def test_unknown_outcome_raises(self, interaction_phen):
        with pytest.raises(ValueError, match="outcomes"):
            interaction_study(
                interaction_phen, "not_a_col",
                covariates=[], progress=False,
            )

    def test_unknown_term_raises(self, interaction_phen):
        with pytest.raises(ValueError, match="terms"):
            interaction_study(
                interaction_phen, "Y",
                interactions=[("V1", "missing_var")],
                covariates=[], progress=False,
            )

    def test_min_n_filter(self, interaction_phen):
        # min_n above n_samples → all pairs in errors.
        res = interaction_study(
            interaction_phen, "Y",
            covariates=[], min_n=10_000, progress=False,
        )
        assert res.n_tests == 0
        assert res.n_errors == 3

    def test_invalid_min_n_raises(self, interaction_phen):
        with pytest.raises(ValueError, match="min_n"):
            interaction_study(
                interaction_phen, "Y",
                covariates=[], min_n=0, progress=False,
            )

    def test_anchor_alone_in_exposures_raises(self):
        # phen with single exposure can't form pairs.
        df = pd.DataFrame(
            {"sample_id": ["A", "B"], "X": [1, 2], "Y": [3, 4]}
        )
        phen = Phenotypes(
            df, sample_id_col="sample_id",
            outcomes=["Y"], exposures=["X"],
        )
        with pytest.raises(ValueError, match="at least"):
            interaction_study(phen, "Y", progress=False)


# ---------------------------------------------------------------------------
# Parallel execution
# ---------------------------------------------------------------------------
class TestParallel:
    def test_n_jobs_minus_one_matches_sequential(self, interaction_phen):
        seq = interaction_study(
            interaction_phen, "Y",
            covariates=[], min_n=10, progress=False, n_jobs=1,
        )
        par = interaction_study(
            interaction_phen, "Y",
            covariates=[], min_n=10, progress=False, n_jobs=-1,
        )
        assert len(seq.df) == len(par.df)
        seq_pairs = set(zip(seq.df["term1"], seq.df["term2"]))
        par_pairs = set(zip(par.df["term1"], par.df["term2"]))
        assert seq_pairs == par_pairs
