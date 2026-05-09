"""Smoke tests for the dotplot primitive."""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from igem.modules.plot import dotplot


@pytest.fixture
def df_for_dot() -> pd.DataFrame:
    rng = np.random.default_rng(0)
    n = 30
    beta = rng.normal(0, 0.3, n)
    se = np.abs(rng.normal(0.05, 0.02, n))
    return pd.DataFrame(
        {
            "variable": [f"v{i}" for i in range(n)],
            "beta_pvalue": rng.uniform(1e-6, 1.0, n),
            "beta": beta,
            "ci_low": beta - 1.96 * se,
            "ci_high": beta + 1.96 * se,
        }
    )


class TestDotplot:
    def test_returns_two_panel_figure(self, df_for_dot):
        fig = dotplot(df_for_dot)
        assert len(fig.axes) == 2  # p-value panel + beta panel
        plt.close(fig)

    def test_n_top_truncates(self, df_for_dot):
        fig = dotplot(df_for_dot, n_top=5)
        # left panel should have 5 ticks on the y-axis
        labels = [t.get_text() for t in fig.axes[0].get_yticklabels()]
        assert len(labels) == 5
        plt.close(fig)

    def test_writes_output_path(self, df_for_dot, tmp_path):
        out = tmp_path / "dot.png"
        fig = dotplot(df_for_dot, output_path=out)
        assert out.exists()
        plt.close(fig)

    def test_works_without_ci_columns(self, df_for_dot):
        df = df_for_dot.drop(columns=["ci_low", "ci_high"])
        fig = dotplot(df, ci_low_col=None, ci_high_col=None)
        plt.close(fig)

    def test_raises_on_missing_required_col(self, df_for_dot):
        df = df_for_dot.drop(columns=["beta"])
        with pytest.raises(ValueError, match="missing"):
            dotplot(df)
