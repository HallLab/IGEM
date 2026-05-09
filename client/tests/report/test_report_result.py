"""
Unit tests for ReportResult — wrapper around a server-side report run.

Covers:
  - DataFrame construction from response rows + columns
  - Property pass-through (message, elapsed_seconds, stats, report)
  - save_csv: parent-dir creation and CSV round-trip
  - __repr__ format
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from igem.modules.report.result import ReportResult
from igem.modules.report.schemas import ReportRunResponse


@pytest.fixture
def result(gene_run_response) -> ReportResult:
    return ReportResult(ReportRunResponse.model_validate(gene_run_response))


class TestReportResultConstruction:
    def test_df_built_from_rows(self, result):
        assert list(result.df.columns) == [
            "input_value", "gene_symbol", "status",
        ]
        assert len(result.df) == 2

    def test_empty_rows_yields_empty_df_with_columns(self, gene_run_response):
        gene_run_response["rows"] = []
        result = ReportResult(
            ReportRunResponse.model_validate(gene_run_response)
        )
        assert len(result.df) == 0
        assert list(result.df.columns) == [
            "input_value", "gene_symbol", "status",
        ]


class TestReportResultProperties:
    def test_message(self, result):
        assert result.message == "[report] Running 'gene_annotations'..."

    def test_elapsed_seconds(self, result):
        assert result.elapsed_seconds == 0.123

    def test_stats(self, result):
        assert result.stats == {"total_rows": 2, "found": 1, "not_found": 1}

    def test_report_metadata(self, result):
        assert result.report.name == "gene_annotations"
        assert result.report.version == "1.0.0"


class TestReportResultSaveCsv:
    def test_writes_file(self, result, tmp_path):
        out = tmp_path / "out.csv"
        path = result.save_csv(out)
        assert path == out
        assert out.exists()

    def test_round_trip_preserves_rows(self, result, tmp_path):
        out = tmp_path / "out.csv"
        result.save_csv(out)
        df_read = pd.read_csv(out)
        assert list(df_read["input_value"]) == ["TP53", "NOPE"]
        assert list(df_read["status"]) == ["found", "not_found"]

    def test_creates_parent_dirs(self, result, tmp_path):
        out = tmp_path / "deep" / "nested" / "out.csv"
        path = result.save_csv(out)
        assert path.exists()
        assert path.parent == tmp_path / "deep" / "nested"

    def test_accepts_str_path_and_returns_path(self, result, tmp_path):
        out_str = str(tmp_path / "out.csv")
        path = result.save_csv(out_str)
        assert isinstance(path, Path)
        assert path == Path(out_str)


class TestReportResultRepr:
    def test_includes_name_rows_and_elapsed(self, result):
        text = repr(result)
        assert "gene_annotations" in text
        assert "rows=2" in text
        assert "elapsed=" in text
