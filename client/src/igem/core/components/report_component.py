from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional, Union

from igem.core.components.base_component import BaseComponent
from igem.modules.reports import (
    ReportInfo,
    ReportResult,
    ReportsManager,
)

PathLike = Union[str, Path]


class ReportsComponent(BaseComponent):
    """
    Server-side reports: list, explain, run.

    Mirrors the shape of ``igem_backend.core.components.report_component``
    on the backend side. Lazily creates a :class:`ReportsManager` against
    the shared HTTP client owned by :class:`IGEMCore`.
    """

    def _manager(self) -> ReportsManager:
        return ReportsManager(self.core.http)

    def list(self) -> list[ReportInfo]:
        """Return reports registered on the server."""
        return self._manager().list()

    def explain(self, name: str) -> str:
        """Return the markdown documentation for a report."""
        return self._manager().explain(name)

    def run(
        self,
        name: str,
        params: Optional[dict[str, Any]] = None,
        columns: Optional[list[str]] = None,
    ) -> ReportResult:
        """Run a report by name and return a typed result."""
        return self._manager().run(name, params=params, columns=columns)

    def gene_annotations(
        self,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[PathLike] = None,
        assembly: str = "GRCh38.p14",
        columns: Optional[list[str]] = None,
        output_path: Optional[PathLike] = None,
    ) -> ReportResult:
        """Typed helper for the ``gene_annotations`` report."""
        return self._manager().gene_annotations(
            input_values=input_values,
            input_file=input_file,
            assembly=assembly,
            columns=columns,
            output_path=output_path,
        )
