from __future__ import annotations

from igem_backend.core.components.base_component import BaseComponent


class ReportComponent(BaseComponent):
    """
    Report component for reusable OxO queries and analysis outputs.

    Planned capabilities:
    - Curated reports returning pandas DataFrames
    - OxO relationship queries (GxG, GxE, ExE filters)
    - ETL status and provenance reports

    Status: placeholder — to be implemented.
    """

    def list(self) -> list[str]:
        raise NotImplementedError("Report module not yet implemented.")

    def run(self, report_name: str, **kwargs):
        raise NotImplementedError("Report module not yet implemented.")
