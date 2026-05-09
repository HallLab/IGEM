"""Smoke tests for the QQ primitive."""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pytest

from igem.modules.plot import qq_plot
from igem.modules.plot.primitives.qq import genomic_inflation


class TestGenomicInflation:
    def test_uniform_pvalues_lambda_close_to_one(self):
        rng = np.random.default_rng(0)
        p = rng.uniform(0, 1, 5000)
        lam = genomic_inflation(p)
        assert 0.9 < lam < 1.1

    def test_inflated_pvalues_lambda_above_one(self):
        rng = np.random.default_rng(0)
        # Heavily skewed-small p-values mimic uncorrected confounding
        p = rng.beta(0.3, 1.0, 5000)
        lam = genomic_inflation(p)
        assert lam > 1.5

    def test_raises_when_no_finite_values(self):
        with pytest.raises(ValueError, match="no finite"):
            genomic_inflation([np.nan, 0.0, 2.0])


class TestQQPlot:
    def test_returns_figure_and_lambda_annotated(self):
        rng = np.random.default_rng(0)
        fig = qq_plot(rng.uniform(0, 1, 500))
        ax = fig.axes[0]
        texts = [t.get_text() for t in ax.texts]
        assert any("lambda" in t.lower() or r"\lambda" in t or "λ" in t for t in texts)
        plt.close(fig)

    def test_show_lambda_false_skips_annotation(self):
        rng = np.random.default_rng(0)
        fig = qq_plot(rng.uniform(0, 1, 200), show_lambda=False)
        assert len(fig.axes[0].texts) == 0
        plt.close(fig)

    def test_writes_output_path(self, tmp_path):
        rng = np.random.default_rng(0)
        out = tmp_path / "qq.png"
        fig = qq_plot(rng.uniform(0, 1, 200), output_path=out)
        assert out.exists()
        plt.close(fig)

    def test_raises_when_no_valid_pvalues(self):
        with pytest.raises(ValueError, match="no finite"):
            qq_plot([np.nan, 0.0, 2.0])
