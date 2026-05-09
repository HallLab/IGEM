"""
Unit tests for ReportManager.

Mocks the httpx client so the tests exercise the manager's HTTP wiring
(URLs, body shape, error handling) without needing a server.
"""
from __future__ import annotations

import pytest

from igem.core.errors import IGEMAPIError
from igem.modules.report.manager import ReportManager, read_identifier_file
from igem.modules.report.result import ReportResult


# ---------------------------------------------------------------------------
# list / explain
# ---------------------------------------------------------------------------
class TestReportManagerList:
    def test_calls_correct_endpoint(self, fake_http, list_response):
        fake_http.set_response("GET", "/api/v1/reports", 200, list_response)
        mgr = ReportManager(fake_http)
        reports = mgr.list()
        assert fake_http.gets == ["/api/v1/reports"]
        assert {r.name for r in reports} == {
            "gene_annotations", "pathway_annotations",
        }

    def test_raises_on_5xx(self, fake_http):
        fake_http.set_response(
            "GET", "/api/v1/reports", 500, {"detail": "boom"}
        )
        mgr = ReportManager(fake_http)
        with pytest.raises(IGEMAPIError) as exc_info:
            mgr.list()
        assert exc_info.value.status_code == 500


class TestReportManagerExplain:
    def test_calls_correct_endpoint(self, fake_http, explain_response):
        fake_http.set_response(
            "GET", "/api/v1/reports/gene_annotations", 200, explain_response,
        )
        mgr = ReportManager(fake_http)
        text = mgr.explain("gene_annotations")
        assert text == "# gene_annotations\n\nDocs body."
        assert fake_http.gets == ["/api/v1/reports/gene_annotations"]

    def test_raises_on_404(self, fake_http):
        fake_http.set_response(
            "GET", "/api/v1/reports/missing", 404, {"detail": "not found"},
        )
        mgr = ReportManager(fake_http)
        with pytest.raises(IGEMAPIError):
            mgr.explain("missing")


# ---------------------------------------------------------------------------
# run (generic)
# ---------------------------------------------------------------------------
class TestReportManagerRun:
    def test_posts_correct_body(self, fake_http, gene_run_response):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        result = mgr.run(
            "gene_annotations",
            params={"input_values": ["TP53"]},
            columns=["gene_symbol"],
        )
        assert isinstance(result, ReportResult)
        url, body = fake_http.posts[0]
        assert url == "/api/v1/reports/gene_annotations/run"
        assert body == {
            "params": {"input_values": ["TP53"]},
            "columns": ["gene_symbol"],
        }

    def test_omits_columns_when_none(self, fake_http, gene_run_response):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        mgr.run("gene_annotations", params={"input_values": ["TP53"]})
        _, body = fake_http.posts[0]
        assert "columns" not in body

    def test_defaults_params_to_empty_dict(self, fake_http, gene_run_response):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        mgr.run("gene_annotations")
        _, body = fake_http.posts[0]
        assert body["params"] == {}


# ---------------------------------------------------------------------------
# gene_annotations typed helper
# ---------------------------------------------------------------------------
class TestReportManagerGeneAnnotations:
    def test_input_values_propagate(self, fake_http, gene_run_response):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        mgr.gene_annotations(input_values=["TP53", "BRCA1"])
        _, body = fake_http.posts[0]
        assert body["params"]["input_values"] == ["TP53", "BRCA1"]

    def test_assembly_propagates(self, fake_http, gene_run_response):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        mgr.gene_annotations(input_values=["TP53"], assembly="GRCh37")
        _, body = fake_http.posts[0]
        assert body["params"]["assembly"] == "GRCh37"

    def test_default_assembly_is_grch38(self, fake_http, gene_run_response):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        mgr.gene_annotations(input_values=["TP53"])
        _, body = fake_http.posts[0]
        assert body["params"]["assembly"] == "GRCh38.p14"

    def test_no_input_omits_input_values(self, fake_http, gene_run_response):
        """Empty input → request omits input_values so server returns all."""
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        mgr.gene_annotations()
        _, body = fake_http.posts[0]
        assert "input_values" not in body["params"]

    def test_input_file_merges_with_input_values(
        self, fake_http, gene_run_response, tmp_path,
    ):
        ids = tmp_path / "genes.txt"
        ids.write_text("BRCA1\n# comment\nEGFR\n\n")
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        mgr.gene_annotations(input_values=["TP53"], input_file=ids)
        _, body = fake_http.posts[0]
        assert body["params"]["input_values"] == ["TP53", "BRCA1", "EGFR"]

    def test_columns_propagates(self, fake_http, gene_run_response):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        mgr.gene_annotations(
            input_values=["TP53"], columns=["gene_symbol", "hgnc_id"],
        )
        _, body = fake_http.posts[0]
        assert body["columns"] == ["gene_symbol", "hgnc_id"]

    def test_output_path_writes_csv(
        self, fake_http, gene_run_response, tmp_path,
    ):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/gene_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        out = tmp_path / "result.csv"
        result = mgr.gene_annotations(input_values=["TP53"], output_path=out)
        assert out.exists()
        assert len(result.df) == 2


# ---------------------------------------------------------------------------
# disease_annotations / go_annotations / pathway_annotations / protein_annotations
# ---------------------------------------------------------------------------
class TestReportManagerDiseaseAnnotations:
    def _set_response(self, fake_http, body):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/disease_annotations/run",
            200,
            body,
        )

    def test_input_values_propagate(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.disease_annotations(input_values=["MONDO:0005301", "OMIM:104300"])
        url, body = fake_http.posts[0]
        assert url == "/api/v1/reports/disease_annotations/run"
        assert body["params"]["input_values"] == [
            "MONDO:0005301", "OMIM:104300",
        ]

    def test_group_filter_propagates(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.disease_annotations(
            input_values=["MONDO:0005301"], group_filter="autoimmune",
        )
        _, body = fake_http.posts[0]
        assert body["params"]["group_filter"] == "autoimmune"

    def test_group_filter_omitted_when_none(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.disease_annotations(input_values=["MONDO:0005301"])
        _, body = fake_http.posts[0]
        assert "group_filter" not in body["params"]

    def test_default_toggles_sent(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.disease_annotations(input_values=["MONDO:0005301"])
        _, body = fake_http.posts[0]
        assert body["params"]["emit_not_found_rows"] is True
        assert body["params"]["include_relationships"] is True
        assert body["params"]["include_aliases"] is True

    def test_toggles_off_propagate(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.disease_annotations(
            input_values=["MONDO:0005301"],
            emit_not_found_rows=False,
            include_relationships=False,
            include_aliases=False,
        )
        _, body = fake_http.posts[0]
        assert body["params"]["emit_not_found_rows"] is False
        assert body["params"]["include_relationships"] is False
        assert body["params"]["include_aliases"] is False


class TestReportManagerGoAnnotations:
    def _set_response(self, fake_http, body):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/go_annotations/run",
            200,
            body,
        )

    def test_input_values_propagate(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.go_annotations(input_values=["GO:0007049"])
        url, body = fake_http.posts[0]
        assert url == "/api/v1/reports/go_annotations/run"
        assert body["params"]["input_values"] == ["GO:0007049"]

    def test_namespace_propagates(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.go_annotations(input_values=["GO:0007049"], namespace="BP")
        _, body = fake_http.posts[0]
        assert body["params"]["namespace"] == "BP"

    def test_namespace_omitted_when_none(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.go_annotations(input_values=["GO:0007049"])
        _, body = fake_http.posts[0]
        assert "namespace" not in body["params"]


class TestReportManagerPathwayAnnotations:
    def test_calls_correct_endpoint(self, fake_http, gene_run_response):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/pathway_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        mgr.pathway_annotations(input_values=["R-HSA-109581"])
        url, body = fake_http.posts[0]
        assert url == "/api/v1/reports/pathway_annotations/run"
        assert body["params"]["input_values"] == ["R-HSA-109581"]

    def test_default_toggles_sent(self, fake_http, gene_run_response):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/pathway_annotations/run",
            200,
            gene_run_response,
        )
        mgr = ReportManager(fake_http)
        mgr.pathway_annotations()
        _, body = fake_http.posts[0]
        assert body["params"]["emit_not_found_rows"] is True
        assert body["params"]["include_relationships"] is True
        assert body["params"]["include_aliases"] is True


class TestReportManagerProteinAnnotations:
    def _set_response(self, fake_http, body):
        fake_http.set_response(
            "POST",
            "/api/v1/reports/protein_annotations/run",
            200,
            body,
        )

    def test_input_values_propagate(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.protein_annotations(input_values=["P04637"])
        url, body = fake_http.posts[0]
        assert url == "/api/v1/reports/protein_annotations/run"
        assert body["params"]["input_values"] == ["P04637"]

    def test_pfam_defaults(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.protein_annotations(input_values=["P04637"])
        _, body = fake_http.posts[0]
        assert body["params"]["include_pfam_summary"] is True
        assert body["params"]["include_pfam_details"] is False
        assert body["params"]["max_pfam_ids_per_type"] == 10

    def test_pfam_overrides_propagate(self, fake_http, gene_run_response):
        self._set_response(fake_http, gene_run_response)
        mgr = ReportManager(fake_http)
        mgr.protein_annotations(
            input_values=["P04637"],
            include_pfam_summary=False,
            include_pfam_details=True,
            max_pfam_ids_per_type=5,
        )
        _, body = fake_http.posts[0]
        assert body["params"]["include_pfam_summary"] is False
        assert body["params"]["include_pfam_details"] is True
        assert body["params"]["max_pfam_ids_per_type"] == 5


# ---------------------------------------------------------------------------
# read_identifier_file helper
# ---------------------------------------------------------------------------
class TestReadIdentifierFile:
    def test_strips_whitespace_and_skips_blanks_and_comments(self, tmp_path):
        f = tmp_path / "ids.txt"
        f.write_text("TP53\n\n  BRCA1  \n# a comment\nEGFR\n")
        ids = read_identifier_file(f)
        assert ids == ["TP53", "BRCA1", "EGFR"]

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("")
        assert read_identifier_file(f) == []

    def test_only_comments(self, tmp_path):
        f = tmp_path / "comments.txt"
        f.write_text("# header\n# more\n")
        assert read_identifier_file(f) == []
