from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Optional

from igem.core.errors import IGEMAPIError
from igem.modules.report.result import ReportResult
from igem.modules.report.schemas import (
    ReportExplainResponse,
    ReportInfo,
    ReportListResponse,
    ReportRunResponse,
)

if TYPE_CHECKING:
    import httpx


class ReportManager:
    """
    HTTP-side manager for the server's report endpoints.

    Mirrors the role of ``ReportManager`` on the backend: discovers what
    is available, runs reports, and returns results in a typed wrapper
    (``ReportResult``). Stateless — depends only on the shared httpx
    client passed at construction time.
    """

    def __init__(self, http: "httpx.Client") -> None:
        self._http = http

    # ----- Generic operations --------------------------------------------

    def list(self) -> list[ReportInfo]:
        resp = self._http.get("/api/v1/reports")
        if resp.status_code >= 400:
            _raise_for_status(resp)
        return ReportListResponse.model_validate(resp.json()).reports

    def explain(self, name: str) -> str:
        resp = self._http.get(f"/api/v1/reports/{name}")
        if resp.status_code >= 400:
            _raise_for_status(resp)
        return ReportExplainResponse.model_validate(resp.json()).markdown

    def run(
        self,
        name: str,
        params: Optional[dict[str, Any]] = None,
        columns: Optional[list[str]] = None,
    ) -> ReportResult:
        body: dict[str, Any] = {"params": params or {}}
        if columns is not None:
            body["columns"] = columns
        resp = self._http.post(f"/api/v1/reports/{name}/run", json=body)
        if resp.status_code >= 400:
            _raise_for_status(resp)
        parsed = ReportRunResponse.model_validate(resp.json())
        return ReportResult(parsed)

    # ----- Typed helpers --------------------------------------------------

    def _run_with_inputs(
        self,
        name: str,
        *,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[str | Path] = None,
        extra_params: Optional[dict[str, Any]] = None,
        columns: Optional[list[str]] = None,
        output_path: Optional[str | Path] = None,
    ) -> ReportResult:
        """
        Shared body for typed report helpers.

        Merges ``input_values`` and ``input_file`` into a single list,
        attaches ``extra_params`` (report-specific kwargs), runs the
        report, and optionally writes the result to CSV.
        """
        merged: list[str] = []
        if input_values is not None:
            merged.extend(str(v) for v in input_values)
        if input_file is not None:
            merged.extend(read_identifier_file(input_file))

        params: dict[str, Any] = dict(extra_params or {})
        if merged:
            params["input_values"] = merged

        result = self.run(name, params=params, columns=columns)
        if output_path is not None:
            result.save_csv(output_path)
        return result

    def gene_annotations(
        self,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[str | Path] = None,
        assembly: str = "GRCh38.p14",
        columns: Optional[list[str]] = None,
        output_path: Optional[str | Path] = None,
    ) -> ReportResult:
        """
        Run the ``gene_annotations`` report.

        Provide either ``input_values`` (iterable of gene identifiers),
        ``input_file`` (text file with one identifier per line; blank lines
        and ``#`` comments are ignored), or both. If neither is provided,
        the server returns one row per registered gene.

        If ``output_path`` is given, the resulting DataFrame is also
        written to CSV.
        """
        return self._run_with_inputs(
            "gene_annotations",
            input_values=input_values,
            input_file=input_file,
            extra_params={"assembly": assembly},
            columns=columns,
            output_path=output_path,
        )

    def disease_annotations(
        self,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[str | Path] = None,
        group_filter: Optional[str] = None,
        emit_not_found_rows: bool = True,
        include_relationships: bool = True,
        include_aliases: bool = True,
        columns: Optional[list[str]] = None,
        output_path: Optional[str | Path] = None,
    ) -> ReportResult:
        """
        Run the ``disease_annotations`` report.

        Inputs accept MONDO IDs, OMIM IDs, MeSH IDs, ICD-10 codes,
        Orphanet IDs, disease names, or any registered alias. Pass
        ``group_filter`` to restrict the result to a named disease group
        (e.g. ``"autoimmune"``). The remaining toggles map directly to
        server-side behaviour: ``emit_not_found_rows`` keeps unmatched
        inputs in the output, and the ``include_*`` flags populate the
        relationship / alias columns.
        """
        extra: dict[str, Any] = {
            "emit_not_found_rows": emit_not_found_rows,
            "include_relationships": include_relationships,
            "include_aliases": include_aliases,
        }
        if group_filter is not None:
            extra["group_filter"] = group_filter
        return self._run_with_inputs(
            "disease_annotations",
            input_values=input_values,
            input_file=input_file,
            extra_params=extra,
            columns=columns,
            output_path=output_path,
        )

    def go_annotations(
        self,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[str | Path] = None,
        namespace: Optional[str] = None,
        emit_not_found_rows: bool = True,
        include_relationships: bool = True,
        include_aliases: bool = True,
        columns: Optional[list[str]] = None,
        output_path: Optional[str | Path] = None,
    ) -> ReportResult:
        """
        Run the ``go_annotations`` report.

        Inputs accept GO IDs (``GO:xxxxxxx``), term names, synonyms, or
        any registered alias. Pass ``namespace`` (``"BP"``, ``"MF"``, or
        ``"CC"``) to restrict to a single GO namespace.
        """
        extra: dict[str, Any] = {
            "emit_not_found_rows": emit_not_found_rows,
            "include_relationships": include_relationships,
            "include_aliases": include_aliases,
        }
        if namespace is not None:
            extra["namespace"] = namespace
        return self._run_with_inputs(
            "go_annotations",
            input_values=input_values,
            input_file=input_file,
            extra_params=extra,
            columns=columns,
            output_path=output_path,
        )

    def pathway_annotations(
        self,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[str | Path] = None,
        emit_not_found_rows: bool = True,
        include_relationships: bool = True,
        include_aliases: bool = True,
        columns: Optional[list[str]] = None,
        output_path: Optional[str | Path] = None,
    ) -> ReportResult:
        """
        Run the ``pathway_annotations`` report.

        Inputs accept Reactome IDs, KEGG IDs, pathway names, or any
        registered alias.
        """
        extra: dict[str, Any] = {
            "emit_not_found_rows": emit_not_found_rows,
            "include_relationships": include_relationships,
            "include_aliases": include_aliases,
        }
        return self._run_with_inputs(
            "pathway_annotations",
            input_values=input_values,
            input_file=input_file,
            extra_params=extra,
            columns=columns,
            output_path=output_path,
        )

    def protein_annotations(
        self,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[str | Path] = None,
        include_pfam_summary: bool = True,
        include_pfam_details: bool = False,
        max_pfam_ids_per_type: int = 10,
        emit_not_found_rows: bool = True,
        include_relationships: bool = True,
        include_aliases: bool = True,
        columns: Optional[list[str]] = None,
        output_path: Optional[str | Path] = None,
    ) -> ReportResult:
        """
        Run the ``protein_annotations`` report.

        Inputs accept UniProt accessions (canonical or isoform), protein
        names, gene symbols, or any registered alias. Pfam summary
        columns are populated by default; pass ``include_pfam_details=
        True`` to also list per-type accessions, capped at
        ``max_pfam_ids_per_type``.
        """
        extra: dict[str, Any] = {
            "include_pfam_summary": include_pfam_summary,
            "include_pfam_details": include_pfam_details,
            "max_pfam_ids_per_type": max_pfam_ids_per_type,
            "emit_not_found_rows": emit_not_found_rows,
            "include_relationships": include_relationships,
            "include_aliases": include_aliases,
        }
        return self._run_with_inputs(
            "protein_annotations",
            input_values=input_values,
            input_file=input_file,
            extra_params=extra,
            columns=columns,
            output_path=output_path,
        )


def read_identifier_file(path: str | Path) -> list[str]:
    """
    Read a list of identifiers from a text file (one per line).

    Blank lines and lines starting with ``#`` are ignored. Whitespace is
    trimmed.
    """
    text = Path(path).read_text(encoding="utf-8")
    values: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        values.append(line)
    return values


def _raise_for_status(resp: "httpx.Response") -> None:
    try:
        body = resp.json()
        detail = body.get("detail", body)
    except Exception:
        detail = resp.text
    raise IGEMAPIError(resp.status_code, detail)
