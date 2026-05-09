from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Optional

from igem.core.errors import IGEMAPIError
from igem.modules.reports.result import ReportResult
from igem.modules.reports.schemas import (
    ReportExplainResponse,
    ReportInfo,
    ReportListResponse,
    ReportRunResponse,
)

if TYPE_CHECKING:
    import httpx


class ReportsManager:
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
        merged: list[str] = []
        if input_values is not None:
            merged.extend(str(v) for v in input_values)
        if input_file is not None:
            merged.extend(read_identifier_file(input_file))

        params: dict[str, Any] = {"assembly": assembly}
        if merged:
            params["input_values"] = merged

        result = self.run("gene_annotations", params=params, columns=columns)
        if output_path is not None:
            result.save_csv(output_path)
        return result


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
