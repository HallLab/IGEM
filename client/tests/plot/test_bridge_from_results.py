"""Tests for the ``from_results`` bridge."""
from __future__ import annotations

import matplotlib.pyplot as plt
import pytest

from igem.modules.plot import from_results


class TestAutoKind:
    def test_small_results_picks_top(self, small_results):
        fig = from_results(small_results, kind="auto")
        # ``top`` builds a two-panel dotplot
        assert len(fig.axes) == 2
        plt.close(fig)

    def test_large_results_picks_manhattan(self, large_results):
        fig = from_results(large_results, kind="auto")
        # manhattan has a single primary axes
        assert len(fig.axes) == 1
        ax = fig.axes[0]
        assert ax.get_ylabel() == r"$-\log_{10}(p)$"
        plt.close(fig)


class TestExplicitKinds:
    def test_manhattan(self, large_results):
        fig = from_results(large_results, kind="manhattan")
        assert fig.axes[0].get_ylabel() == r"$-\log_{10}(p)$"
        plt.close(fig)

    def test_qq(self, large_results):
        fig = from_results(large_results, kind="qq")
        ax = fig.axes[0]
        assert "Expected" in ax.get_xlabel()
        plt.close(fig)

    def test_top(self, small_results):
        fig = from_results(small_results, kind="top", n_top=5)
        # n_top=5 -> 5 y-ticks on the p-value panel
        labels = [t.get_text() for t in fig.axes[0].get_yticklabels()]
        assert len(labels) == 5
        plt.close(fig)

    def test_manhattan_bonferroni(self, large_results):
        fig = from_results(large_results, kind="manhattan_bonferroni")
        assert "Bonferroni" in fig.axes[0].get_title()
        plt.close(fig)

    def test_manhattan_fdr_requires_correction(self, large_results):
        with pytest.raises(ValueError, match="with_correction"):
            from_results(large_results, kind="manhattan_fdr")

    def test_manhattan_fdr_after_correction(self, corrected_results):
        fig = from_results(corrected_results, kind="manhattan_fdr")
        assert "FDR" in fig.axes[0].get_title()
        plt.close(fig)


class TestSchemaDetection:
    def test_legacy_p_value_column(self, legacy_results):
        # ``p_value`` legacy schema must still plot without the user
        # specifying the column name.
        fig = from_results(legacy_results, kind="manhattan")
        plt.close(fig)

    def test_chromosome_axis_when_annotated(self, annotated_results):
        fig = from_results(annotated_results, kind="manhattan")
        assert fig.axes[0].get_xlabel() == "Chromosome"
        plt.close(fig)

    def test_no_chromosome_axis_when_not_annotated(self, large_results):
        fig = from_results(large_results, kind="manhattan")
        assert fig.axes[0].get_xlabel() == "Variable index"
        plt.close(fig)


class TestUnknownKind:
    def test_raises_on_unknown_kind(self, small_results):
        with pytest.raises(ValueError, match="unknown kind"):
            from_results(small_results, kind="not-a-kind")


class TestOutputPath:
    def test_writes_via_bridge(self, large_results, tmp_path):
        out = tmp_path / "manhattan.png"
        fig = from_results(large_results, kind="manhattan", output_path=out)
        assert out.exists()
        plt.close(fig)
