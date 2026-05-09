"""Smoke tests for the Manhattan primitives."""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from igem.modules.plot import manhattan, manhattan_bonferroni, manhattan_fdr


@pytest.fixture
def df_simple() -> pd.DataFrame:
    rng = np.random.default_rng(0)
    n = 100
    return pd.DataFrame(
        {
            "variable": [f"v{i}" for i in range(n)],
            "beta_pvalue": rng.uniform(1e-8, 1.0, n),
        }
    )


@pytest.fixture
def df_genomic() -> pd.DataFrame:
    rng = np.random.default_rng(1)
    n = 100
    return pd.DataFrame(
        {
            "variable": [f"rs{i}" for i in range(n)],
            "beta_pvalue": rng.uniform(1e-8, 1.0, n),
            "chromosome": (rng.integers(1, 5, n)).astype(str),
            "start_position": rng.integers(1, 1_000_000, n),
        }
    )


class TestManhattan:
    def test_returns_figure(self, df_simple):
        fig = manhattan(df_simple)
        assert fig.axes  # non-empty
        plt.close(fig)

    def test_genomic_axis_uses_chrom_centers(self, df_genomic):
        fig = manhattan(df_genomic, chrom_col="chromosome", pos_col="start_position")
        ax = fig.axes[0]
        labels = [t.get_text() for t in ax.get_xticklabels()]
        assert ax.get_xlabel() == "Chromosome"
        assert set(labels) <= set(df_genomic["chromosome"].unique())
        plt.close(fig)

    def test_writes_output_path(self, df_simple, tmp_path):
        out = tmp_path / "m.png"
        fig = manhattan(df_simple, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0
        plt.close(fig)

    def test_raises_on_missing_pvalue_col(self, df_simple):
        with pytest.raises(ValueError, match="pvalue_col"):
            manhattan(df_simple, pvalue_col="nope")

    def test_raises_on_empty_after_dropna(self):
        df = pd.DataFrame({"variable": ["a"], "beta_pvalue": [np.nan]})
        with pytest.raises(ValueError, match="empty"):
            manhattan(df)

    def test_cutoffs_drawn(self, df_simple):
        fig = manhattan(df_simple, cutoffs=[(5e-8, "red", "--")])
        ax = fig.axes[0]
        # axhline shows up as a Line2D among ax.lines
        ylines = [line.get_ydata()[0] for line in ax.lines if line.get_linestyle() == "--"]
        expected = -np.log10(5e-8)
        assert any(abs(y - expected) < 1e-6 for y in ylines)
        plt.close(fig)


class TestManhattanBonferroni:
    def test_threshold_is_alpha_over_n(self, df_simple):
        fig = manhattan_bonferroni(df_simple, alpha=0.05)
        ax = fig.axes[0]
        n = int(df_simple["beta_pvalue"].notna().sum())
        expected = -np.log10(0.05 / n)
        ylines = [line.get_ydata()[0] for line in ax.lines if line.get_linestyle() == "--"]
        assert any(abs(y - expected) < 1e-6 for y in ylines)
        plt.close(fig)

    def test_default_title_mentions_alpha(self, df_simple):
        fig = manhattan_bonferroni(df_simple, alpha=0.01)
        assert "0.01" in fig.axes[0].get_title()
        plt.close(fig)


class TestManhattanFdr:
    def test_uses_p_corrected_column(self, df_simple):
        df = df_simple.copy()
        df["p_corrected"] = df["beta_pvalue"].clip(upper=0.5)
        fig = manhattan_fdr(df, q_threshold=0.1)
        ax = fig.axes[0]
        ylines = [line.get_ydata()[0] for line in ax.lines if line.get_linestyle() == "--"]
        assert any(abs(y - -np.log10(0.1)) < 1e-6 for y in ylines)
        plt.close(fig)

    def test_raises_when_corrected_col_absent(self, df_simple):
        with pytest.raises(ValueError, match="not found"):
            manhattan_fdr(df_simple)
