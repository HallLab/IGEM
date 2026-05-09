from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional, Union

from igem.core.components.base_component import BaseComponent
from igem.modules.report import (
    ReportInfo,
    ReportManager,
    ReportResult,
)

PathLike = Union[str, Path]


class ReportComponent(BaseComponent):
    """
    Server-side reports: list, explain, run.

    Mirrors the shape of ``igem_backend.core.components.report_component``
    on the backend side. Lazily creates a :class:`ReportManager` against
    the shared HTTP client owned by :class:`IGEMCore`.
    """

    def _manager(self) -> ReportManager:
        return ReportManager(self.core.http)

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

    def disease_annotations(
        self,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[PathLike] = None,
        group_filter: Optional[str] = None,
        emit_not_found_rows: bool = True,
        include_relationships: bool = True,
        include_aliases: bool = True,
        columns: Optional[list[str]] = None,
        output_path: Optional[PathLike] = None,
    ) -> ReportResult:
        """Typed helper for the ``disease_annotations`` report."""
        return self._manager().disease_annotations(
            input_values=input_values,
            input_file=input_file,
            group_filter=group_filter,
            emit_not_found_rows=emit_not_found_rows,
            include_relationships=include_relationships,
            include_aliases=include_aliases,
            columns=columns,
            output_path=output_path,
        )

    def go_annotations(
        self,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[PathLike] = None,
        namespace: Optional[str] = None,
        emit_not_found_rows: bool = True,
        include_relationships: bool = True,
        include_aliases: bool = True,
        columns: Optional[list[str]] = None,
        output_path: Optional[PathLike] = None,
    ) -> ReportResult:
        """Typed helper for the ``go_annotations`` report."""
        return self._manager().go_annotations(
            input_values=input_values,
            input_file=input_file,
            namespace=namespace,
            emit_not_found_rows=emit_not_found_rows,
            include_relationships=include_relationships,
            include_aliases=include_aliases,
            columns=columns,
            output_path=output_path,
        )

    def pathway_annotations(
        self,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[PathLike] = None,
        emit_not_found_rows: bool = True,
        include_relationships: bool = True,
        include_aliases: bool = True,
        columns: Optional[list[str]] = None,
        output_path: Optional[PathLike] = None,
    ) -> ReportResult:
        """Typed helper for the ``pathway_annotations`` report."""
        return self._manager().pathway_annotations(
            input_values=input_values,
            input_file=input_file,
            emit_not_found_rows=emit_not_found_rows,
            include_relationships=include_relationships,
            include_aliases=include_aliases,
            columns=columns,
            output_path=output_path,
        )

    def protein_annotations(
        self,
        input_values: Optional[Iterable[str]] = None,
        input_file: Optional[PathLike] = None,
        include_pfam_summary: bool = True,
        include_pfam_details: bool = False,
        max_pfam_ids_per_type: int = 10,
        emit_not_found_rows: bool = True,
        include_relationships: bool = True,
        include_aliases: bool = True,
        columns: Optional[list[str]] = None,
        output_path: Optional[PathLike] = None,
    ) -> ReportResult:
        """Typed helper for the ``protein_annotations`` report."""
        return self._manager().protein_annotations(
            input_values=input_values,
            input_file=input_file,
            include_pfam_summary=include_pfam_summary,
            include_pfam_details=include_pfam_details,
            max_pfam_ids_per_type=max_pfam_ids_per_type,
            emit_not_found_rows=emit_not_found_rows,
            include_relationships=include_relationships,
            include_aliases=include_aliases,
            columns=columns,
            output_path=output_path,
        )
