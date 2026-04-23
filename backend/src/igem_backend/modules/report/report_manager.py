from __future__ import annotations

import importlib
import pkgutil
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from igem_backend.modules.db.database import Database
    from igem_backend.utils.logger import Logger


class ReportManager:
    """
    Discovers and runs curated IGEM reports.

    Auto-discovers all ReportBase subclasses inside the reports/ subpackage
    by scanning with pkgutil.iter_modules(). Each report is registered by its
    REPORT_NAME attribute. Markdown documentation is loaded from reports_explain/.
    """

    def __init__(
        self,
        db: Database,
        logger: Logger,
        debug_mode: bool = False,
    ) -> None:
        self.db = db
        self.logger = logger
        self.debug_mode = debug_mode
        self._registry: dict[str, type] = {}
        self._discover()

    # -------------------------------------------------------------------------
    # Discovery
    # -------------------------------------------------------------------------
    def _discover(self) -> None:
        from igem_backend.modules.report.reports import base_report as _base
        from igem_backend.modules.report.reports.base_report import ReportBase
        import igem_backend.modules.report.reports as reports_pkg

        for _finder, module_name, _is_pkg in pkgutil.iter_modules(
            reports_pkg.__path__
        ):
            if module_name == "base_report":
                continue
            full_name = f"igem_backend.modules.report.reports.{module_name}"
            try:
                mod = importlib.import_module(full_name)
            except Exception as exc:
                self.logger.log(
                    f"[report] Could not load module '{module_name}': {exc}",
                    "WARNING",
                )
                continue

            for attr_name in dir(mod):
                attr = getattr(mod, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, ReportBase)
                    and attr is not ReportBase
                    and hasattr(attr, "REPORT_NAME")
                ):
                    self._registry[attr.REPORT_NAME] = attr
                    self.logger.log(
                        f"[report] Registered '{attr.REPORT_NAME}'",
                        "DEBUG",
                    )

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    def list(self) -> list[dict]:
        """Return a list of registered reports with name and description."""
        return [
            {
                "name": name,
                "version": cls.REPORT_VERSION,
                "description": cls.REPORT_DESCRIPTION,
            }
            for name, cls in sorted(self._registry.items())
        ]

    def explain(self, report_name: str) -> str:
        """Return markdown documentation for a report (or a short description)."""
        doc_path = (
            Path(__file__).parent
            / "reports_explain"
            / f"{report_name}.md"
        )
        if doc_path.exists():
            return doc_path.read_text(encoding="utf-8")

        cls = self._registry.get(report_name)
        if cls:
            return f"# {report_name}\n\n{cls.REPORT_DESCRIPTION}\n"

        available = ", ".join(sorted(self._registry.keys()))
        return f"Report '{report_name}' not found. Available: {available}"

    def run(self, report_name: str, **kwargs) -> pd.DataFrame:
        """Run a registered report and return a DataFrame."""
        cls = self._registry.get(report_name)
        if cls is None:
            available = ", ".join(sorted(self._registry.keys()))
            raise ValueError(
                f"Report '{report_name}' not found. Available: {available}"
            )

        report = cls()
        t0 = time.time()
        self.logger.log(f"[report] Running '{report_name}'...", "INFO")

        with self.db.get_session() as session:
            df = report.run(session=session, **kwargs)

        elapsed = time.time() - t0
        self.logger.footer(
            f"[report] '{report_name}' complete: {len(df)} rows in {elapsed:.1f}s"
        )
        return df
