"""Shared fixtures for plot tests.

Uses the matplotlib ``Agg`` backend so figure creation does not require
a display.
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import numpy as np
import pandas as pd
import pytest

from igem.modules.analyze.results import RegressionResults
from igem.modules.data import Phenotypes


def _build_results_df(n_tests: int, *, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    pvals = rng.uniform(low=1e-10, high=1.0, size=n_tests)
    pvals[0] = 1e-9  # one strong hit so labelling has something to show
    beta = rng.normal(0.0, 0.3, size=n_tests)
    se = np.abs(rng.normal(0.05, 0.02, size=n_tests))
    return pd.DataFrame(
        {
            "outcome": ["BMI"] * n_tests,
            "variable": [f"var_{i:04d}" for i in range(n_tests)],
            "variable_type": ["continuous"] * n_tests,
            "n": [1000] * n_tests,
            "beta": beta,
            "se": se,
            "ci_low": beta - 1.96 * se,
            "ci_high": beta + 1.96 * se,
            "beta_pvalue": pvals,
            "lrt_pvalue": pvals,
            "diff_aic": rng.normal(0.0, 1.0, size=n_tests),
            "converged": [True] * n_tests,
            "error": [None] * n_tests,
        }
    )


@pytest.fixture
def small_results() -> RegressionResults:
    """``n_tests=10`` — auto-mode should pick ``top``."""
    df = _build_results_df(10, seed=1)
    return RegressionResults(
        df=df,
        family="linear",
        outcome="BMI",
        covariates=["age", "sex"],
        formula_template="BMI ~ {var} + age + sex",
        errors=pd.DataFrame(),
    )


@pytest.fixture
def large_results() -> RegressionResults:
    """``n_tests=200`` — auto-mode should pick ``manhattan``."""
    df = _build_results_df(200, seed=2)
    return RegressionResults(
        df=df,
        family="linear",
        outcome="BMI",
        covariates=["age", "sex"],
        formula_template="BMI ~ {var} + age + sex",
        errors=pd.DataFrame(),
    )


@pytest.fixture
def annotated_results(large_results: RegressionResults) -> RegressionResults:
    """``large_results`` enriched with chromosome / position columns."""
    rng = np.random.default_rng(42)
    df = large_results.df.copy()
    df["chromosome"] = (rng.integers(1, 23, size=len(df))).astype(str)
    df["start_position"] = rng.integers(1, 250_000_000, size=len(df))
    df["gene_symbol"] = [f"GENE{i}" for i in range(len(df))]
    return RegressionResults(
        df=df,
        family=large_results.family,
        outcome=large_results.outcome,
        covariates=large_results.covariates,
        formula_template=large_results.formula_template,
        errors=large_results.errors,
    )


@pytest.fixture
def corrected_results(large_results: RegressionResults) -> RegressionResults:
    """``large_results`` with ``p_corrected`` populated (FDR-BH)."""
    return large_results.with_correction(method="fdr_bh")


@pytest.fixture
def plot_phen() -> Phenotypes:
    """
    Phenotypes with one column per kind (binary, continuous, categorical)
    so distribution primitives can be exercised end-to-end.
    """
    rng = np.random.default_rng(7)
    n = 200
    df = pd.DataFrame(
        {
            "sample_id": [f"S{i:03d}" for i in range(n)],
            "BMI": rng.normal(27.0, 4.0, n),
            "AGE": rng.integers(20, 80, n).astype(float),
            "SEX": rng.choice(["M", "F"], n),
            "RACE": rng.choice(["white", "black", "asian", "other"], n),
        }
    )
    return Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=["BMI"],
        covariates=["AGE", "SEX"],
    )


@pytest.fixture
def plot_phen_modified(plot_phen: Phenotypes) -> Phenotypes:
    """
    ``plot_phen`` with a log-transformed BMI and the rows where BMI > 35
    dropped — simulates a typical modify pipeline (transform + filter).
    """
    df = plot_phen.df.copy()
    df = df.loc[df["BMI"] <= 35].reset_index(drop=True)
    df["BMI"] = np.log(df["BMI"])
    return Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=plot_phen.outcomes,
        covariates=plot_phen.covariates,
    )


@pytest.fixture
def interaction_results_small() -> RegressionResults:
    """6 terms × all pairs → 15 rows; small enough for heatmap auto."""
    rng = np.random.default_rng(11)
    terms = [f"v{i}" for i in range(6)]
    pairs = [(a, b) for i, a in enumerate(terms) for b in terms[i + 1:]]
    df = pd.DataFrame(
        {
            "outcome": ["BMI"] * len(pairs),
            "term1": [a for a, _ in pairs],
            "term2": [b for _, b in pairs],
            "n": [1000] * len(pairs),
            "lrt_chi2": rng.uniform(0.5, 20, len(pairs)),
            "lrt_df": [1] * len(pairs),
            "lrt_pvalue": rng.uniform(1e-6, 1.0, len(pairs)),
            "diff_aic": rng.normal(0, 5, len(pairs)),
            "converged": [True] * len(pairs),
            "error": [None] * len(pairs),
        }
    )
    return RegressionResults(
        df=df, family="linear", outcome="BMI", covariates=["age"],
        formula_template="BMI ~ {term1} + {term2} + {term1}:{term2} + age",
        errors=pd.DataFrame(),
    )


@pytest.fixture
def interaction_results_large() -> RegressionResults:
    """40 terms → 780 pairs; auto kind should fall back to top_pairs."""
    rng = np.random.default_rng(12)
    terms = [f"v{i:02d}" for i in range(40)]
    pairs = [(a, b) for i, a in enumerate(terms) for b in terms[i + 1:]]
    df = pd.DataFrame(
        {
            "outcome": ["BMI"] * len(pairs),
            "term1": [a for a, _ in pairs],
            "term2": [b for _, b in pairs],
            "n": [1000] * len(pairs),
            "lrt_chi2": rng.uniform(0.5, 20, len(pairs)),
            "lrt_df": [1] * len(pairs),
            "lrt_pvalue": rng.uniform(1e-6, 1.0, len(pairs)),
            "diff_aic": rng.normal(0, 5, len(pairs)),
            "converged": [True] * len(pairs),
            "error": [None] * len(pairs),
        }
    )
    return RegressionResults(
        df=df, family="linear", outcome="BMI", covariates=[],
        formula_template="BMI ~ {term1} + {term2} + {term1}:{term2}",
        errors=pd.DataFrame(),
    )


@pytest.fixture
def legacy_results() -> RegressionResults:
    """Result frame with the legacy ``p_value`` column instead of ``beta_pvalue``."""
    rng = np.random.default_rng(3)
    n = 60
    pvals = rng.uniform(low=1e-8, high=1.0, size=n)
    df = pd.DataFrame(
        {
            "variable": [f"snp_{i}" for i in range(n)],
            "n": [800] * n,
            "beta": rng.normal(0.0, 0.2, n),
            "se": np.abs(rng.normal(0.04, 0.01, n)),
            "ci_low": rng.normal(-0.5, 0.1, n),
            "ci_high": rng.normal(0.5, 0.1, n),
            "p_value": pvals,
        }
    )
    return RegressionResults(
        df=df,
        family="linear",
        outcome="height",
        covariates=[],
        formula_template="height ~ {var}",
        errors=pd.DataFrame(),
    )
