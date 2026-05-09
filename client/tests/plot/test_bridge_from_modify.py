"""Tests for the ``from_modify`` before/after bridge."""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from igem.modules.data import Phenotypes
from igem.modules.plot import from_modify


# ----- Phenotypes path --------------------------------------------------
class TestPhenotypesPath:
    def test_overlay_returns_single_figure(self, plot_phen, plot_phen_modified):
        fig = from_modify(plot_phen, plot_phen_modified, var="BMI")
        assert len(fig.axes) == 1
        plt.close(fig)

    def test_side_by_side_returns_two_axes(self, plot_phen, plot_phen_modified):
        fig = from_modify(
            plot_phen, plot_phen_modified, var="BMI", layout="side_by_side",
        )
        assert len(fig.axes) == 2
        plt.close(fig)

    def test_categorical_var_overlay(self, plot_phen, plot_phen_modified):
        fig = from_modify(plot_phen, plot_phen_modified, var="SEX")
        plt.close(fig)

    def test_var_required_for_phenotypes(self, plot_phen, plot_phen_modified):
        with pytest.raises(ValueError, match="var="):
            from_modify(plot_phen, plot_phen_modified)

    def test_var_must_be_present_in_both(self, plot_phen, plot_phen_modified):
        with pytest.raises(ValueError, match="not in"):
            from_modify(plot_phen, plot_phen_modified, var="DOES_NOT_EXIST")

    def test_writes_output_path(self, plot_phen, plot_phen_modified, tmp_path):
        out = tmp_path / "modify.png"
        fig = from_modify(
            plot_phen, plot_phen_modified, var="BMI", output_path=out,
        )
        assert out.exists()
        plt.close(fig)


# ----- Genotypes path ---------------------------------------------------
class TestGenotypesPath:
    def test_maf_overlay(self, maf_geno):
        fig = from_modify(maf_geno, maf_geno, metric="maf")
        assert len(fig.axes) == 1
        plt.close(fig)

    def test_call_rate_metric(self, maf_geno):
        fig = from_modify(maf_geno, maf_geno, metric="call_rate")
        plt.close(fig)

    def test_invalid_metric_raises(self, maf_geno):
        with pytest.raises(ValueError, match="metric="):
            from_modify(maf_geno, maf_geno, metric="not-a-metric")


# ----- Dispatch / validation -------------------------------------------
class TestDispatch:
    def test_mismatched_types_raises(self, plot_phen, maf_geno):
        with pytest.raises(TypeError, match="same type"):
            from_modify(plot_phen, maf_geno, var="BMI")

    def test_invalid_layout_raises(self, plot_phen, plot_phen_modified):
        with pytest.raises(ValueError, match="layout="):
            from_modify(
                plot_phen, plot_phen_modified, var="BMI", layout="nope",
            )

    def test_unsupported_input_type_raises(self):
        with pytest.raises(TypeError, match="expects Phenotypes or Genotypes"):
            from_modify(pd.DataFrame({"a": [1]}), pd.DataFrame({"a": [2]}), var="a")
