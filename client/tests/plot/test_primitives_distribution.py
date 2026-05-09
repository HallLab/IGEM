"""Smoke tests for the distribution primitive."""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from igem.modules.plot import distribution
from igem.modules.plot.primitives.distribution import detect_kind


class TestDetectKind:
    def test_binary_two_levels(self):
        assert detect_kind(pd.Series(["yes", "no", "yes"])) == "binary"

    def test_continuous_numeric(self):
        assert detect_kind(pd.Series([1.0, 2.5, 3.14, 0.5])) == "continuous"

    def test_categorical_string_many_levels(self):
        assert detect_kind(pd.Series(["a", "b", "c", "d", "e"])) == "categorical"

    def test_binary_takes_precedence_over_numeric(self):
        # 0/1 ints — should still be binary, not continuous
        assert detect_kind(pd.Series([0, 1, 1, 0, 0])) == "binary"


class TestDistributionContinuous:
    @pytest.fixture
    def s(self):
        rng = np.random.default_rng(0)
        return pd.Series(rng.normal(0, 1, 200), name="x")

    @pytest.mark.parametrize("kind", ["hist", "box", "violin", "qq"])
    def test_each_continuous_kind(self, s, kind):
        fig = distribution(s, continuous_kind=kind)
        assert len(fig.axes) >= 1
        plt.close(fig)

    def test_invalid_continuous_kind_raises(self, s):
        with pytest.raises(ValueError, match="continuous_kind"):
            distribution(s, continuous_kind="rugplot")

    def test_writes_output_path(self, s, tmp_path):
        out = tmp_path / "dist.png"
        fig = distribution(s, output_path=out)
        assert out.exists()
        plt.close(fig)


class TestDistributionCategorical:
    def test_categorical_renders_bars(self):
        s = pd.Series(["a", "b", "a", "c", "b", "a"], name="cat")
        fig = distribution(s)
        ax = fig.axes[0]
        # 3 unique levels = 3 bars
        bars = [p for p in ax.patches if p.get_height() > 0]
        assert len(bars) == 3
        plt.close(fig)

    def test_binary_classified_and_plotted_as_bars(self):
        s = pd.Series([0, 1, 1, 0, 1, 0, 1], name="b")
        fig = distribution(s)
        ax = fig.axes[0]
        bars = [p for p in ax.patches if p.get_height() > 0]
        assert len(bars) == 2
        plt.close(fig)


class TestExplicitKindOverride:
    def test_force_categorical_on_numeric(self):
        # Discrete numeric (1..5) which would auto-detect as continuous;
        # the user can force categorical to render bars instead.
        s = pd.Series(np.random.default_rng(0).integers(1, 6, 50), name="discrete")
        fig = distribution(s, kind="categorical")
        ax = fig.axes[0]
        bars = [p for p in ax.patches if p.get_height() > 0]
        assert 1 < len(bars) <= 5
        plt.close(fig)

    def test_invalid_kind_raises(self):
        s = pd.Series([1.0, 2.0])
        with pytest.raises(ValueError, match="kind="):
            distribution(s, kind="not-a-kind")


class TestEmptySeries:
    def test_no_data_message_on_all_nan(self):
        s = pd.Series([np.nan, np.nan, np.nan], name="x")
        fig = distribution(s, kind="continuous")
        # Should not raise — primitive renders a "no data" message
        assert fig is not None
        plt.close(fig)
