"""
Smoke tests for the IGEM facade exposure of the report module.

Convention: caderno __003 — facade tests verify
  1. Every public method on ReportManager is reachable via igem.report.
  2. Keyword arguments propagate from the facade through to the manager.
  3. The facade returns the same result type as the manager.

The report module differs from describe/modify/analyze in that it
exports classes (ReportManager, ReportResult, schemas) rather than
free functions, so the TestFacadeCoverage guard checks public methods
on ReportManager rather than ``igem.modules.report.__all__``.
"""
from __future__ import annotations

import inspect

from igem.core.components.report_component import ReportComponent
from igem.modules.report import ReportManager, ReportResult


# ---------------------------------------------------------------------------
# Smoke tests — every method via the facade
# ---------------------------------------------------------------------------
class TestReportFacade:
    def test_list(self, igem_with_fake_http, fake_http, list_response):
        fake_http.set_response("GET", "/api/v1/reports", 200, list_response)
        reports = igem_with_fake_http.report.list()
        assert {r.name for r in reports} == {
            "gene_annotations", "pathway_annotations",
        }

    def test_explain(self, igem_with_fake_http, fake_http, explain_response):
        fake_http.set_response(
            "GET", "/api/v1/reports/gene_annotations", 200, explain_response,
        )
        text = igem_with_fake_http.report.explain("gene_annotations")
        assert text.startswith("# gene_annotations")

    def test_run(self, igem_with_fake_http, fake_http, gene_run_response):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        result = igem_with_fake_http.report.run(
            "gene_annotations", params={"input_values": ["TP53"]},
        )
        assert isinstance(result, ReportResult)
        assert len(result.df) == 2

    def test_run_columns_propagates(
        self, igem_with_fake_http, fake_http, gene_run_response,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        igem_with_fake_http.report.run(
            "gene_annotations",
            params={"input_values": ["TP53"]},
            columns=["gene_symbol"],
        )
        _, body = fake_http.posts[0]
        assert body["columns"] == ["gene_symbol"]

    def test_gene_annotations(
        self, igem_with_fake_http, fake_http, gene_run_response,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        result = igem_with_fake_http.report.gene_annotations(
            input_values=["TP53"],
        )
        assert isinstance(result, ReportResult)

    def test_gene_annotations_assembly_propagates(
        self, igem_with_fake_http, fake_http, gene_run_response,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        igem_with_fake_http.report.gene_annotations(
            input_values=["TP53"], assembly="GRCh37",
        )
        _, body = fake_http.posts[0]
        assert body["params"]["assembly"] == "GRCh37"

    def test_gene_annotations_input_file_propagates(
        self, igem_with_fake_http, fake_http, gene_run_response, tmp_path,
    ):
        ids = tmp_path / "genes.txt"
        ids.write_text("BRCA1\nEGFR\n")
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        igem_with_fake_http.report.gene_annotations(
            input_values=["TP53"], input_file=ids,
        )
        _, body = fake_http.posts[0]
        assert body["params"]["input_values"] == ["TP53", "BRCA1", "EGFR"]

    def test_gene_annotations_columns_propagates(
        self, igem_with_fake_http, fake_http, gene_run_response,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        igem_with_fake_http.report.gene_annotations(
            input_values=["TP53"], columns=["gene_symbol", "hgnc_id"],
        )
        _, body = fake_http.posts[0]
        assert body["columns"] == ["gene_symbol", "hgnc_id"]

    def test_gene_annotations_output_path_writes_csv(
        self, igem_with_fake_http, fake_http, gene_run_response, tmp_path,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        out = tmp_path / "result.csv"
        igem_with_fake_http.report.gene_annotations(
            input_values=["TP53"], output_path=out,
        )
        assert out.exists()


    # ----- new typed helpers (Sprint 2) -----

    def test_disease_annotations(
        self, igem_with_fake_http, fake_http, gene_run_response,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/disease_annotations/run",
            200,
            gene_run_response,
        )
        result = igem_with_fake_http.report.disease_annotations(
            input_values=["MONDO:0005301"], group_filter="autoimmune",
        )
        assert isinstance(result, ReportResult)
        _, body = fake_http.posts[0]
        assert body["params"]["input_values"] == ["MONDO:0005301"]
        assert body["params"]["group_filter"] == "autoimmune"

    def test_go_annotations_namespace_propagates(
        self, igem_with_fake_http, fake_http, gene_run_response,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/go_annotations/run",
            200,
            gene_run_response,
        )
        igem_with_fake_http.report.go_annotations(
            input_values=["GO:0007049"], namespace="BP",
        )
        _, body = fake_http.posts[0]
        assert body["params"]["namespace"] == "BP"

    def test_pathway_annotations(
        self, igem_with_fake_http, fake_http, gene_run_response,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/pathway_annotations/run",
            200,
            gene_run_response,
        )
        result = igem_with_fake_http.report.pathway_annotations(
            input_values=["R-HSA-109581"],
        )
        assert isinstance(result, ReportResult)
        _, body = fake_http.posts[0]
        assert body["params"]["input_values"] == ["R-HSA-109581"]

    def test_protein_annotations_pfam_overrides_propagate(
        self, igem_with_fake_http, fake_http, gene_run_response,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/protein_annotations/run",
            200,
            gene_run_response,
        )
        igem_with_fake_http.report.protein_annotations(
            input_values=["P04637"],
            include_pfam_details=True,
            max_pfam_ids_per_type=5,
        )
        _, body = fake_http.posts[0]
        assert body["params"]["include_pfam_details"] is True
        assert body["params"]["max_pfam_ids_per_type"] == 5

    def test_disease_annotations_output_path_writes_csv(
        self, igem_with_fake_http, fake_http, gene_run_response, tmp_path,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/disease_annotations/run",
            200,
            gene_run_response,
        )
        out = tmp_path / "diseases.csv"
        igem_with_fake_http.report.disease_annotations(
            input_values=["MONDO:0005301"], output_path=out,
        )
        assert out.exists()


# ---------------------------------------------------------------------------
# Coverage guard — every public method on ReportManager must be reachable
# on ReportComponent.
# ---------------------------------------------------------------------------
class TestFacadeCoverage:
    """
    Adapted from caderno __003: the report module exports classes, not
    free functions, so the natural surface to guard is the manager class
    rather than ``igem.modules.report.__all__``.

    If a new public method is added to ReportManager but not wired into
    ReportComponent, this test fails. Future-proofs the facade against
    silent omissions when typed helpers for new reports land.
    """

    def test_every_public_manager_method_is_on_the_component(self):
        manager_methods = {
            name for name, _ in inspect.getmembers(
                ReportManager, predicate=inspect.isfunction,
            )
            if not name.startswith("_")
        }
        component_methods = {
            name for name, _ in inspect.getmembers(
                ReportComponent, predicate=inspect.isfunction,
            )
            if not name.startswith("_")
        }
        missing = manager_methods - component_methods
        assert missing == set(), (
            f"Methods on ReportManager but missing from ReportComponent: "
            f"{sorted(missing)}"
        )
