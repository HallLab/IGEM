"""Tests for the ``suggest_plots`` engine and the ``.suggested_plots()`` method."""
from __future__ import annotations

import pandas as pd

from igem.modules.plot import suggest_plots


class TestSuggestRegressionResults:
    def test_regular_results_suggests_manhattan_qq_top(self, large_results):
        sugg = suggest_plots(large_results)
        assert "manhattan" in sugg
        assert "qq" in sugg
        assert "top" in sugg
        # Without correction the FDR variant should NOT be suggested
        assert "manhattan_fdr" not in sugg

    def test_corrected_results_adds_manhattan_fdr(self, corrected_results):
        sugg = suggest_plots(corrected_results)
        assert "manhattan_fdr" in sugg
        # base suggestions still present
        assert "manhattan" in sugg

    def test_interaction_results_suggests_heatmap_and_top_pairs(
        self, interaction_results_small,
    ):
        sugg = suggest_plots(interaction_results_small)
        assert sugg == ["heatmap", "top_pairs"]


class TestSuggestPhenotypesAndGenotypes:
    def test_phenotypes_suggests_distributions(self, plot_phen):
        sugg = suggest_plots(plot_phen)
        assert "distributions" in sugg

    def test_genotypes_suggests_maf_and_call_rate(self, maf_geno):
        sugg = suggest_plots(maf_geno)
        assert "maf_distribution" in sugg
        assert "call_rate_distribution" in sugg


class TestSuggestUnsupported:
    def test_returns_empty_for_unsupported(self):
        assert suggest_plots(pd.DataFrame()) == []
        assert suggest_plots("string") == []
        assert suggest_plots(None) == []


class TestRegressionResultsMethod:
    def test_method_delegates_to_free_function(self, large_results):
        assert large_results.suggested_plots() == suggest_plots(large_results)

    def test_method_for_interaction_results(self, interaction_results_small):
        assert interaction_results_small.suggested_plots() == [
            "heatmap", "top_pairs",
        ]
