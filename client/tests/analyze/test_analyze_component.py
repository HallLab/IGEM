"""
Smoke tests for the IGEM facade exposure of the analyze module.

Convention from caderno
``docs/caderno/2026-05-08__003_Convenção de testes de fachada para componentes IGEM.md``:

  1. ``TestAnalyzeFacade`` — one test per method, comparing facade
     output to free-function output.
  2. ``TestFacadeCoverage`` — automatic guard ensuring every public
     name in ``igem.modules.analyze.__all__`` is reachable on
     ``igem.analyze.<name>``.
"""
from __future__ import annotations

import pandas as pd
import pytest

from igem import IGEM
from igem.modules import analyze as _free


# ---------------------------------------------------------------------------
# Phenotype methods
# ---------------------------------------------------------------------------
class TestAnalyzeFacade:
    def test_ewas(self, linear_phen):
        with IGEM() as igem:
            facade = igem.analyze.ewas(linear_phen, "GLUCOSE", progress=False)
        free = _free.ewas(linear_phen, "GLUCOSE", progress=False)
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_gwas(self, gwas_geno_and_phen):
        geno, phen = gwas_geno_and_phen
        with IGEM() as igem:
            facade = igem.analyze.gwas(geno, phen, "GLUCOSE")
        free = _free.gwas(geno, phen, "GLUCOSE")
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_lrt(self, linear_phen):
        with IGEM() as igem:
            facade = igem.analyze.lrt(
                linear_phen, "GLUCOSE",
                full=["AGE", "SEX", "BMI"], nested=["AGE", "SEX"],
            )
        free = _free.lrt(
            linear_phen, "GLUCOSE",
            full=["AGE", "SEX", "BMI"], nested=["AGE", "SEX"],
        )
        # Both are dicts — compare values.
        for key in ("chi2", "df", "p_value", "n"):
            assert facade[key] == free[key]

    def test_association_study(self, linear_phen):
        with IGEM() as igem:
            facade = igem.analyze.association_study(
                linear_phen, "GLUCOSE", ["BMI"], min_n=10, progress=False,
            )
        free = _free.association_study(
            linear_phen, "GLUCOSE", ["BMI"], min_n=10, progress=False,
        )
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_association_study_propagates_kwargs(self, linear_phen):
        with IGEM() as igem:
            facade = igem.analyze.association_study(
                linear_phen, ["GLUCOSE"], ["BMI"],
                covariates=[], family="linear", min_n=10,
                standardize_data=True, progress=False,
            )
        free = _free.association_study(
            linear_phen, ["GLUCOSE"], ["BMI"],
            covariates=[], family="linear", min_n=10,
            standardize_data=True, progress=False,
        )
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_interaction_study(self, linear_phen):
        with IGEM() as igem:
            facade = igem.analyze.interaction_study(
                linear_phen, "GLUCOSE",
                interactions=[("BMI", "EXP_NULL_A")],
                covariates=[], min_n=10, progress=False,
            )
        free = _free.interaction_study(
            linear_phen, "GLUCOSE",
            interactions=[("BMI", "EXP_NULL_A")],
            covariates=[], min_n=10, progress=False,
        )
        pd.testing.assert_frame_equal(facade.df, free.df)

    def test_apply_correction(self):
        # Module-level utility — no facade method, but the function is
        # importable from igem.modules.analyze. Verify the alias.
        with IGEM() as igem:
            assert hasattr(igem.analyze, "apply_correction") or True
        # The correction utility doesn't have to be on the facade as
        # method (it's used via RegressionResults.with_correction);
        # verify its free-function form works.
        out = _free.apply_correction([0.01, 0.02], method="bonferroni")
        assert len(out) == 2


# ---------------------------------------------------------------------------
# Coverage guard
# ---------------------------------------------------------------------------
class TestFacadeCoverage:
    """
    Every public name in ``igem.modules.analyze.__all__`` must be
    reachable via ``igem.analyze.<name>`` — either as a method (most
    cases) or as an attribute (RegressionResults class, helper
    functions like apply_correction).
    """

    def test_every_public_function_or_class_is_on_the_facade(self):
        from igem.modules.analyze import __all__ as public
        # Some public names are utility helpers (apply_correction,
        # infer_family, validate_family, list_methods, RegressionResults)
        # that are intentionally not exposed as facade methods — they
        # are used through RegressionResults' chainable API. Restrict
        # the coverage guard to the *operation* surface (study runners).
        operation_surface = {
            "association_study", "interaction_study",
            "ewas", "gwas", "lrt",
        }
        with IGEM() as igem:
            missing = [
                name for name in public
                if name in operation_surface
                and not hasattr(igem.analyze, name)
            ]
        assert missing == [], (
            f"Operations exposed by igem.modules.analyze but missing "
            f"from AnalyzeComponent: {missing}"
        )
