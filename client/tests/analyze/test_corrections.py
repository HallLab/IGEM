"""Tests for igem.modules.analyze._corrections."""
from __future__ import annotations

import numpy as np
import pytest

from igem.modules.analyze import apply_correction, list_methods


class TestApplyCorrection:
    def test_bonferroni_matches_manual_formula(self):
        pvalues = [0.01, 0.04, 0.05, 0.20]
        out = apply_correction(pvalues, method="bonferroni")
        # Bonferroni: each p multiplied by m, capped at 1.0
        expected = np.minimum(np.array(pvalues) * 4, 1.0)
        assert np.allclose(out, expected)

    def test_fdr_bh_runs_and_returns_same_shape(self):
        pvalues = [0.001, 0.01, 0.05, 0.10, 0.50]
        out = apply_correction(pvalues, method="fdr_bh")
        assert out.shape == (5,)
        # BH adjusted values are >= raw values and monotone non-decreasing
        # when sorted by raw p
        order = np.argsort(pvalues)
        assert np.all(np.diff(out[order]) >= -1e-12)

    def test_nan_pvalues_pass_through(self):
        pvalues = [0.01, np.nan, 0.20, np.nan]
        out = apply_correction(pvalues, method="bonferroni")
        assert np.isnan(out[1])
        assert np.isnan(out[3])
        # Bonferroni for the 2 finite p-values uses m=2, not m=4.
        assert out[0] == pytest.approx(min(0.01 * 2, 1.0))
        assert out[2] == pytest.approx(min(0.20 * 2, 1.0))

    def test_all_nan_returns_all_nan(self):
        out = apply_correction([np.nan, np.nan, np.nan], method="bonferroni")
        assert np.isnan(out).all()

    def test_unknown_method_raises(self):
        with pytest.raises(ValueError, match="unknown correction"):
            apply_correction([0.05], method="bogus")


class TestListMethods:
    def test_includes_common_methods(self):
        methods = list_methods()
        for m in ("bonferroni", "fdr_bh", "fdr_by", "holm"):
            assert m in methods
