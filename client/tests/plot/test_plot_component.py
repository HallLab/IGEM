"""Smoke tests for the IGEM facade exposure of the plot module.

Convention: caderno __003 — facade tests verify
  1. Every public function in ``igem.modules.plot.__all__`` is reachable
     via ``igem.plot``.
  2. Calls via the facade and via the free function produce equivalent
     figures (smoke-level: same primary axes count + same x/y labels).

Plot is stateless (no manager class), so the guard checks
``__all__`` directly — same shape as ``DescribeComponent`` /
``ModifyComponent``.
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from igem import IGEM
from igem.modules import plot as _plot


class TestPlotFacadeBridges:
    def test_from_results_facade_matches_free(self, large_results):
        with IGEM() as igem:
            fig_facade = igem.plot.from_results(large_results, kind="manhattan")
        fig_free = _plot.from_results(large_results, kind="manhattan")
        assert fig_facade.axes[0].get_ylabel() == fig_free.axes[0].get_ylabel()
        plt.close(fig_facade)
        plt.close(fig_free)

    def test_from_results_kind_propagates(self, small_results):
        with IGEM() as igem:
            fig = igem.plot.from_results(small_results, kind="qq")
        assert "Expected" in fig.axes[0].get_xlabel()
        plt.close(fig)

    def test_from_results_output_path(self, large_results, tmp_path):
        out = tmp_path / "from_results.png"
        with IGEM() as igem:
            fig = igem.plot.from_results(
                large_results, kind="manhattan", output_path=out,
            )
        assert out.exists()
        plt.close(fig)

    def test_from_describe_returns_list(self, plot_phen):
        with IGEM() as igem:
            figs = igem.plot.from_describe(plot_phen)
        assert isinstance(figs, list) and len(figs) >= 1
        for f in figs:
            plt.close(f)

    def test_from_modify_phenotypes(self, plot_phen, plot_phen_modified):
        with IGEM() as igem:
            fig = igem.plot.from_modify(
                plot_phen, plot_phen_modified, var="BMI",
            )
        assert fig.axes
        plt.close(fig)

    def test_from_modify_genotypes_maf(self, maf_geno):
        with IGEM() as igem:
            fig = igem.plot.from_modify(maf_geno, maf_geno, metric="maf")
        plt.close(fig)

    def test_from_interaction(self, interaction_results_small):
        with IGEM() as igem:
            fig = igem.plot.from_interaction(
                interaction_results_small, kind="heatmap",
            )
        plt.close(fig)

    def test_suggest_plots(self, large_results):
        with IGEM() as igem:
            sugg = igem.plot.suggest_plots(large_results)
        assert "manhattan" in sugg


class TestPlotFacadePrimitives:
    @pytest.fixture
    def df(self):
        rng = np.random.default_rng(0)
        n = 60
        beta = rng.normal(0, 0.2, n)
        se = np.abs(rng.normal(0.04, 0.01, n))
        return pd.DataFrame(
            {
                "variable": [f"v{i}" for i in range(n)],
                "beta_pvalue": rng.uniform(1e-6, 1.0, n),
                "beta": beta,
                "se": se,
                "ci_low": beta - 1.96 * se,
                "ci_high": beta + 1.96 * se,
            }
        )

    def test_manhattan(self, df):
        with IGEM() as igem:
            fig = igem.plot.manhattan(df)
        assert fig.axes[0].get_ylabel() == r"$-\log_{10}(p)$"
        plt.close(fig)

    def test_manhattan_bonferroni(self, df):
        with IGEM() as igem:
            fig = igem.plot.manhattan_bonferroni(df, alpha=0.01)
        assert "0.01" in fig.axes[0].get_title()
        plt.close(fig)

    def test_manhattan_fdr(self, df):
        df = df.copy()
        df["p_corrected"] = df["beta_pvalue"].clip(upper=0.5)
        with IGEM() as igem:
            fig = igem.plot.manhattan_fdr(df, q_threshold=0.1)
        assert "FDR" in fig.axes[0].get_title()
        plt.close(fig)

    def test_qq_plot(self, df):
        with IGEM() as igem:
            fig = igem.plot.qq_plot(df["beta_pvalue"].to_numpy())
        assert "Expected" in fig.axes[0].get_xlabel()
        plt.close(fig)

    def test_dotplot(self, df):
        with IGEM() as igem:
            fig = igem.plot.dotplot(df, n_top=10)
        assert len(fig.axes) == 2
        plt.close(fig)

    def test_distribution(self):
        s = pd.Series(np.random.default_rng(0).normal(0, 1, 100), name="x")
        with IGEM() as igem:
            fig = igem.plot.distribution(s, continuous_kind="hist")
        plt.close(fig)

    def test_heatmap(self, interaction_results_small):
        with IGEM() as igem:
            fig = igem.plot.heatmap(interaction_results_small.df)
        plt.close(fig)

    def test_miami_plot(self, df):
        with IGEM() as igem:
            fig = igem.plot.miami_plot(df, df)
        assert len(fig.axes) == 2
        plt.close(fig)

    def test_ax_kwarg_propagates_for_composition(self):
        """Regression: facade methods must forward ``ax=`` to the
        primitive so users can compose multiple panels in one figure
        (showcased in the notebook tour).
        """
        s = pd.Series(np.random.default_rng(0).normal(0, 1, 50), name="x")
        fig, axes = plt.subplots(1, 2, figsize=(8, 3))
        with IGEM() as igem:
            igem.plot.distribution(s, continuous_kind="hist", ax=axes[0])
            igem.plot.qq_plot(np.abs(s.to_numpy()) % 1, ax=axes[1])
        # Each axes should now have artists drawn on it
        assert len(axes[0].patches) > 0  # hist bars
        assert len(axes[1].lines) > 0    # qq diagonal + scatter
        plt.close(fig)


class TestFacadeCoverage:
    """If a new function lands in ``igem.modules.plot.__all__`` but is
    missing from :class:`PlotComponent`, this test fails — protecting
    the facade against silent omissions when new bridges/primitives
    are added in future phases.
    """

    def test_every_public_function_is_on_the_facade(self):
        with IGEM() as igem:
            missing = [
                name for name in _plot.__all__ if not hasattr(igem.plot, name)
            ]
        assert missing == [], (
            f"Functions exposed by igem.modules.plot but missing from "
            f"PlotComponent: {missing}"
        )
