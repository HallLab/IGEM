"""Smoke tests for the heatmap primitive."""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from igem.modules.plot import heatmap


@pytest.fixture
def pairs_df() -> pd.DataFrame:
    rng = np.random.default_rng(0)
    terms = ["A", "B", "C", "D"]
    pairs = [(a, b) for i, a in enumerate(terms) for b in terms[i + 1:]]
    return pd.DataFrame(
        {
            "term1": [a for a, _ in pairs],
            "term2": [b for _, b in pairs],
            "lrt_pvalue": rng.uniform(1e-4, 1.0, len(pairs)),
            "beta": rng.normal(0, 1, len(pairs)),
        }
    )


class TestHeatmap:
    def test_returns_figure_with_colorbar(self, pairs_df):
        fig = heatmap(pairs_df)
        # imshow + colorbar => 2 axes
        assert len(fig.axes) >= 2
        plt.close(fig)

    def test_symmetric_fills_both_triangles(self, pairs_df):
        fig = heatmap(pairs_df, symmetric=True)
        ax = fig.axes[0]
        im = ax.images[0]
        data = im.get_array()
        # 4 unique terms → 4×4 matrix
        assert data.shape == (4, 4)
        # Symmetric across the diagonal (off-diagonals filled both sides)
        finite = np.isfinite(data)
        assert np.array_equal(finite, finite.T)
        plt.close(fig)

    def test_neglog10_transform(self, pairs_df):
        fig = heatmap(pairs_df, transform="neglog10")
        ax = fig.axes[0]
        im = ax.images[0]
        # All p-values are in (0, 1] → -log10 should be ≥ 0 wherever finite
        data = im.get_array()
        assert (data[np.isfinite(data)] >= 0).all()
        plt.close(fig)

    def test_raw_transform_preserves_values(self, pairs_df):
        fig = heatmap(pairs_df, transform="raw")
        ax = fig.axes[0]
        im = ax.images[0]
        data = im.get_array()
        finite = data[np.isfinite(data)]
        assert finite.min() >= 0
        assert finite.max() <= 1
        plt.close(fig)

    def test_signed_neglog10_requires_sign_col(self, pairs_df):
        with pytest.raises(ValueError, match="sign_col"):
            heatmap(pairs_df, transform="signed_neglog10")

    def test_signed_neglog10_with_sign_col(self, pairs_df):
        fig = heatmap(pairs_df, transform="signed_neglog10", sign_col="beta")
        plt.close(fig)

    def test_cluster_reorders_axis(self, pairs_df):
        fig = heatmap(pairs_df, cluster=True)
        # Just verify it didn't blow up and produced a valid axes labeling
        labels = [t.get_text() for t in fig.axes[0].get_xticklabels()]
        assert set(labels) == {"A", "B", "C", "D"}
        plt.close(fig)

    def test_annotate_writes_cell_values(self, pairs_df):
        fig = heatmap(pairs_df, annotate=True)
        ax = fig.axes[0]
        # Each finite cell should have a text annotation
        assert len(ax.texts) > 0
        plt.close(fig)

    def test_cutoff_outlines_significant_cells(self, pairs_df):
        # Force at least one cell below cutoff
        df = pairs_df.copy()
        df.loc[0, "lrt_pvalue"] = 1e-8
        fig = heatmap(df, cutoff=1e-5)
        ax = fig.axes[0]
        # cutoff outlines drawn as Rectangle patches
        rectangles = [p for p in ax.patches if p.fill is False]
        assert len(rectangles) > 0
        plt.close(fig)

    def test_writes_output_path(self, pairs_df, tmp_path):
        out = tmp_path / "heatmap.png"
        fig = heatmap(pairs_df, output_path=out)
        assert out.exists()
        plt.close(fig)

    def test_raises_on_missing_required_col(self, pairs_df):
        with pytest.raises(ValueError, match="missing"):
            heatmap(pairs_df, value_col="nope")

    def test_invalid_transform_raises(self, pairs_df):
        with pytest.raises(ValueError, match="transform"):
            heatmap(pairs_df, transform="not-a-transform")
