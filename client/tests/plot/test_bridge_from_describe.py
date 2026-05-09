"""Tests for the ``from_describe`` bridge."""
from __future__ import annotations

import matplotlib.pyplot as plt
import pytest

from igem.modules.plot import from_describe


class TestFromDescribe:
    def test_returns_list_of_figures(self, plot_phen):
        figs = from_describe(plot_phen)
        assert isinstance(figs, list)
        assert all(hasattr(f, "axes") for f in figs)
        for f in figs:
            plt.close(f)

    def test_default_grid_fits_columns_in_one_page(self, plot_phen):
        # plot_phen has 4 plotting cols (BMI, AGE, SEX, RACE) — sample_id excluded.
        # 4 cols ≤ 3*4=12 panels per page → 1 figure.
        figs = from_describe(plot_phen)
        assert len(figs) == 1
        for f in figs:
            plt.close(f)

    def test_small_grid_paginates(self, plot_phen):
        # grid=(1,2) → 2 panels per page; 4 cols → 2 figures
        figs = from_describe(plot_phen, grid=(1, 2))
        assert len(figs) == 2
        for f in figs:
            plt.close(f)

    def test_cols_filter_propagates(self, plot_phen):
        figs = from_describe(plot_phen, cols=["BMI", "SEX"])
        # 2 cols on default 3x4 grid → 1 figure with 2 used panels
        assert len(figs) == 1
        ax_visible = [a for a in figs[0].axes if a.get_visible()]
        assert len(ax_visible) == 2
        for f in figs:
            plt.close(f)

    def test_continuous_kind_propagates(self, plot_phen):
        figs = from_describe(plot_phen, cols=["BMI"], continuous_kind="box")
        # box renders patches on axes; just verify it doesn't blow up
        assert figs[0].axes
        plt.close(figs[0])

    def test_writes_pdf(self, plot_phen, tmp_path):
        out = tmp_path / "describe.pdf"
        figs = from_describe(plot_phen, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0
        for f in figs:
            plt.close(f)

    def test_rejects_non_pdf_output_path(self, plot_phen, tmp_path):
        with pytest.raises(ValueError, match=".pdf"):
            from_describe(plot_phen, output_path=tmp_path / "x.png")
