"""Tests for igem.modules.analyze._engines."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem.modules.analyze._engines import (
    EngineResult,
    glm_engine,
    r_survey_engine,
    resolve_engine,
)


def _linear_data(n: int = 200, seed: int = 42):
    rng = np.random.default_rng(seed)
    x = rng.normal(size=n)
    cov = rng.normal(size=n)
    # True effect: y = 0.5 * x + 0.2 * cov + noise.
    y = pd.Series(0.5 * x + 0.2 * cov + rng.normal(scale=0.5, size=n))
    X = pd.DataFrame({
        "const": np.ones(n),
        "x": x,
        "cov": cov,
    })
    return y, X


def _logistic_data(n: int = 400, seed: int = 7):
    rng = np.random.default_rng(seed)
    x = rng.normal(size=n)
    logit = -0.3 + 1.0 * x
    p = 1.0 / (1.0 + np.exp(-logit))
    y = pd.Series((rng.uniform(size=n) < p).astype(int))
    X = pd.DataFrame({"const": np.ones(n), "x": x})
    return y, X


# ---------------------------------------------------------------------------
# glm_engine
# ---------------------------------------------------------------------------
class TestGlmEngineLinear:
    def test_recovers_known_effect(self):
        y, X = _linear_data()
        res = glm_engine(y, X, family="linear")
        # True beta of x is 0.5; with n=200 we expect close.
        assert res.params["x"] == pytest.approx(0.5, abs=0.1)
        assert res.pvalues["x"] < 0.001

    def test_returns_engine_result_shape(self):
        y, X = _linear_data()
        res = glm_engine(y, X, family="linear")
        assert isinstance(res, EngineResult)
        assert set(res.params.index) == set(X.columns)
        assert set(res.bse.index) == set(X.columns)
        assert set(res.conf_int.columns) == {"lo", "hi"}
        assert res.n == len(y)
        assert res.converged is True

    def test_weights_change_estimate(self):
        y, X = _linear_data(seed=1)
        unweighted = glm_engine(y, X, family="linear")
        # Heavy weight on first half of samples.
        weights = np.concatenate([
            np.ones(len(y) // 2) * 100, np.ones(len(y) - len(y) // 2)
        ])
        weighted = glm_engine(y, X, family="linear", weights=weights)
        # Weighted vs unweighted should differ (heavy weights on half
        # change the estimate beyond random noise).
        assert weighted.params["x"] != unweighted.params["x"]

    def test_cluster_changes_se(self):
        y, X = _linear_data(seed=2)
        # Build artificial clusters of size 5.
        cluster = np.repeat(np.arange(len(y) // 5), 5)
        plain = glm_engine(y, X, family="linear")
        clustered = glm_engine(y, X, family="linear", cluster=cluster)
        # Cluster-robust SE typically differs from naive SE.
        assert clustered.bse["x"] != plain.bse["x"]


class TestGlmEngineLogistic:
    def test_recovers_known_effect(self):
        y, X = _logistic_data()
        res = glm_engine(y, X, family="logistic")
        # True beta of x is 1.0.
        assert res.params["x"] == pytest.approx(1.0, abs=0.3)
        assert res.pvalues["x"] < 0.001

    def test_returns_engine_result_shape(self):
        y, X = _logistic_data()
        res = glm_engine(y, X, family="logistic")
        assert res.converged is True
        assert res.n == len(y)


class TestGlmEngineValidation:
    def test_unknown_family_raises(self):
        y, X = _linear_data(n=20)
        with pytest.raises(ValueError, match="family must be"):
            glm_engine(y, X, family="bogus")


# ---------------------------------------------------------------------------
# r_survey_engine
# ---------------------------------------------------------------------------
class TestRSurveyEngine:
    def test_raises_import_or_not_implemented(self):
        # Either ImportError (rpy2 absent) or NotImplementedError
        # (rpy2 present but binding pending) — both are acceptable
        # signals of "this is an opt-in surface".
        y, X = _linear_data(n=20)
        weights = np.ones(len(y))
        with pytest.raises((ImportError, NotImplementedError)):
            r_survey_engine(y, X, family="linear", weights=weights)


# ---------------------------------------------------------------------------
# resolve_engine
# ---------------------------------------------------------------------------
class TestResolveEngine:
    def test_auto_returns_glm(self):
        engine = resolve_engine("auto")
        assert engine is glm_engine

    def test_glm_alias(self):
        assert resolve_engine("glm") is glm_engine

    def test_weighted_glm_alias(self):
        assert resolve_engine("weighted_glm") is glm_engine

    def test_r_survey_alias(self):
        assert resolve_engine("r_survey") is r_survey_engine

    def test_callable_passed_through(self):
        def custom(y, X, family, **kwargs):
            return EngineResult(
                params=pd.Series(dtype=float),
                bse=pd.Series(dtype=float),
                pvalues=pd.Series(dtype=float),
                conf_int=pd.DataFrame(columns=["lo", "hi"]),
                log_likelihood=0.0, aic=0.0, n=0, converged=True,
            )
        assert resolve_engine(custom) is custom

    def test_unknown_string_raises(self):
        with pytest.raises(ValueError, match="regression_kind"):
            resolve_engine("not_an_engine")
