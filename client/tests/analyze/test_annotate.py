"""Tests for RegressionResults.annotate."""
from __future__ import annotations

import pandas as pd
import pytest

from igem.modules.analyze import RegressionResults


def _result_with(variables: list[str]) -> RegressionResults:
    df = pd.DataFrame(
        {
            "variable": variables,
            "n": [100] * len(variables),
            "beta": [0.1] * len(variables),
            "se": [0.05] * len(variables),
            "ci_low": [0.0] * len(variables),
            "ci_high": [0.2] * len(variables),
            "p_value": [0.01] * len(variables),
        }
    )
    return RegressionResults(
        df=df,
        family="linear",
        outcome="y",
        covariates=[],
        formula_template="y ~ {variant}",
        errors=pd.DataFrame(columns=["variable", "error"]),
        metadata={},
    )


class TestAnnotateBasics:
    def test_merges_default_annotation_columns(self, fake_report):
        res = _result_with(["rs00001", "TP53"])
        out = res.annotate(fake_report)
        assert "gene_symbol" in out.df.columns
        assert "hgnc_id" in out.df.columns
        d = out.df.set_index("variable")
        assert d.loc["rs00001", "gene_symbol"] == "APOE"
        assert d.loc["TP53", "gene_symbol"] == "TP53"

    def test_unmatched_input_gets_nan(self, fake_report):
        res = _result_with(["rs00001", "UNKNOWN_GENE"])
        out = res.annotate(fake_report)
        d = out.df.set_index("variable")
        assert d.loc["rs00001", "gene_symbol"] == "APOE"
        assert pd.isna(d.loc["UNKNOWN_GENE", "gene_symbol"])

    def test_keep_columns_restricts_join(self, fake_report):
        res = _result_with(["rs00001"])
        out = res.annotate(fake_report, keep_columns=["gene_symbol"])
        assert "gene_symbol" in out.df.columns
        assert "hgnc_id" not in out.df.columns

    def test_assembly_passed_through(self, fake_report):
        res = _result_with(["rs00001"])
        res.annotate(fake_report, assembly="GRCh38.p14")
        assert fake_report.last_call["assembly"] == "GRCh38.p14"

    def test_only_unique_inputs_sent_to_server(self, fake_report):
        res = _result_with(["rs00001", "rs00001", "TP53"])
        res.annotate(fake_report)
        assert sorted(fake_report.last_call["input_values"]) == [
            "TP53", "rs00001",
        ]


class TestAnnotateClientShapes:
    def test_accepts_object_with_report_attr(self, fake_report):
        # Simulate the IGEM facade exposing .report.
        class FakeIGEM:
            def __init__(self, r):
                self.report = r

        res = _result_with(["TP53"])
        out = res.annotate(FakeIGEM(fake_report))
        assert "gene_symbol" in out.df.columns

    def test_rejects_object_without_gene_annotations(self):
        class Bogus:
            pass
        res = _result_with(["TP53"])
        with pytest.raises(TypeError, match="gene_annotations"):
            res.annotate(Bogus())

    def test_unknown_input_col_raises(self, fake_report):
        res = _result_with(["TP53"])
        with pytest.raises(ValueError, match="not_a_col"):
            res.annotate(fake_report, input_col="not_a_col")


class TestAnnotateMetadata:
    def test_metadata_records_annotation(self, fake_report):
        res = _result_with(["TP53"])
        out = res.annotate(fake_report)
        assert out.metadata["annotated_with"] == "gene_annotations"
        assert out.metadata["annotation_assembly"] == "GRCh38.p14"


class TestAnnotateChainability:
    def test_chains_with_correction_and_top(self, fake_report):
        res = _result_with(["rs00001", "TP53", "rs00002", "rs00003"])
        chained = (
            res
            .with_correction("bonferroni")
            .annotate(fake_report)
            .top(2)
        )
        assert "gene_symbol" in chained.df.columns
        assert chained.n_tests == 2
