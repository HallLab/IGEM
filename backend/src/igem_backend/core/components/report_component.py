from __future__ import annotations

from typing import Optional

import pandas as pd

from igem_backend.core.components.base_component import BaseComponent


class ReportComponent(BaseComponent):
    """
    Report component: runs curated IGEM analytical reports.

    Reports are auto-discovered from igem_backend.modules.report.reports
    and documented in reports_explain/*.md.
    """

    def _manager(self):
        from igem_backend.modules.report.report_manager import ReportManager

        return ReportManager(
            db=self.core.require_db(),
            logger=self.core.logger,
            debug_mode=self.core.debug_mode,
        )

    def list(self) -> list[dict]:
        """Return registered reports with name, version, description."""
        return self._manager().list()

    def explain(self, report_name: str) -> str:
        """Return markdown documentation for a report."""
        return self._manager().explain(report_name)

    def run(self, report_name: str, **kwargs) -> pd.DataFrame:
        """Run a report and return results as a DataFrame."""
        return self._manager().run(report_name, **kwargs)
