"""Smoke tests for the miami_plot primitive."""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from igem.modules.plot import miami_plot


@pytest.fixture
def two_studies():
    rng = np.random.default_rng(0)
    n = 100
    return (
        pd.DataFrame({
            "variable": [f"v{i}" for i in range(n)],
            "beta_pvalue": rng.uniform(1e-8, 1.0, n),
        }),
        pd.DataFrame({
            "variable": [f"v{i}" for i in range(n)],
            "beta_pvalue": rng.uniform(1e-8, 1.0, n),
        }),
    )


class TestMiamiPlot:
    def test_returns_figure_with_two_axes(self, two_studies):
        df_top, df_bottom = two_studies
        fig = miami_plot(df_top, df_bottom)
        assert len(fig.axes) == 2
        plt.close(fig)

    def test_bottom_axis_is_inverted(self, two_studies):
        df_top, df_bottom = two_studies
        fig = miami_plot(df_top, df_bottom)
        ax_top, ax_bottom = fig.axes
        # Inverted y means ylim[0] > ylim[1]
        ylim_top = ax_top.get_ylim()
        ylim_bottom = ax_bottom.get_ylim()
        assert ylim_top[0] < ylim_top[1]      # normal
        assert ylim_bottom[0] > ylim_bottom[1]  # inverted
        plt.close(fig)

    def test_labels_propagate(self, two_studies):
        df_top, df_bottom = two_studies
        fig = miami_plot(
            df_top, df_bottom, top_label="discovery", bottom_label="replication",
        )
        texts_top = [t.get_text() for t in fig.axes[0].texts]
        texts_bottom = [t.get_text() for t in fig.axes[1].texts]
        assert "discovery" in texts_top
        assert "replication" in texts_bottom
        plt.close(fig)

    def test_writes_output_path(self, two_studies, tmp_path):
        df_top, df_bottom = two_studies
        out = tmp_path / "miami.png"
        fig = miami_plot(df_top, df_bottom, output_path=out)
        assert out.exists()
        plt.close(fig)
