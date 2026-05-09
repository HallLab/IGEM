"""Tests for the ``from_interaction`` bridge."""
from __future__ import annotations

import matplotlib.pyplot as plt
import pytest

from igem.modules.plot import from_interaction


class TestAutoKind:
    def test_small_picks_heatmap(self, interaction_results_small):
        fig = from_interaction(interaction_results_small, kind="auto")
        # heatmap = imshow + colorbar => >= 2 axes
        assert len(fig.axes) >= 2
        plt.close(fig)

    def test_large_picks_top_pairs(self, interaction_results_large):
        fig = from_interaction(interaction_results_large, kind="auto")
        # top_pairs = single-axis dotplot
        assert len(fig.axes) == 1
        plt.close(fig)


class TestExplicitKinds:
    def test_heatmap(self, interaction_results_small):
        fig = from_interaction(interaction_results_small, kind="heatmap")
        plt.close(fig)

    def test_top_pairs(self, interaction_results_small):
        fig = from_interaction(
            interaction_results_small, kind="top_pairs", n_top=5,
        )
        labels = [t.get_text() for t in fig.axes[0].get_yticklabels()]
        assert len(labels) == 5
        # Top pairs labels include both terms separated by " × "
        assert all("×" in label for label in labels)
        plt.close(fig)

    def test_unknown_kind_raises(self, interaction_results_small):
        with pytest.raises(ValueError, match="unknown kind"):
            from_interaction(interaction_results_small, kind="not-a-kind")


class TestValidation:
    def test_rejects_non_interaction_results(self, large_results):
        # large_results has variable, not term1/term2
        with pytest.raises(ValueError, match="term1"):
            from_interaction(large_results)

    def test_rejects_unknown_value_column(self, interaction_results_small):
        with pytest.raises(ValueError, match="value="):
            from_interaction(interaction_results_small, value="bogus")


class TestCustomization:
    def test_custom_value_column(self, interaction_results_small):
        # diff_aic exists in the schema and isn't a p-value, but the
        # bridge should still accept it
        fig = from_interaction(
            interaction_results_small, kind="top_pairs", value="diff_aic",
        )
        plt.close(fig)

    def test_cluster_option_propagates(self, interaction_results_small):
        fig = from_interaction(
            interaction_results_small, kind="heatmap", cluster=True,
        )
        plt.close(fig)

    def test_writes_output_path(self, interaction_results_small, tmp_path):
        out = tmp_path / "interaction.png"
        fig = from_interaction(
            interaction_results_small, kind="heatmap", output_path=out,
        )
        assert out.exists()
        plt.close(fig)
