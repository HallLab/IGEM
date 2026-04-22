from __future__ import annotations

from typing import Optional, Sequence

from igem_backend.core.components.base_component import BaseComponent
from igem_backend.modules.etl.etl_manager import ETLManager


class ETLComponent(BaseComponent):
    """
    ETL component wrapping ETLManager.

    Reads download_path and processed_path from SystemConfig so the
    caller never needs to pass file system paths explicitly.
    """

    def _manager(self) -> ETLManager:
        return ETLManager(
            db=self.core.require_db(),
            debug_mode=self.core.debug_mode,
            logger=self.core.logger,
        )

    def _paths(self) -> tuple[str, str]:
        data_root = self.core.settings.get("data_root", "igem_data")
        return f"{data_root}/downloads", f"{data_root}/processed"

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    def run(
        self,
        data_sources: Optional[Sequence[str]] = None,
        source_system: Optional[Sequence[str]] = None,
        steps: Optional[Sequence[str]] = None,
        force_steps: Optional[Sequence[str]] = None,
    ) -> None:
        """Run ETL for specific data sources (or a whole source system)."""
        download_path, processed_path = self._paths()
        self._manager().run(
            data_sources=data_sources,
            source_system=source_system,
            steps=steps,
            force_steps=force_steps,
            download_path=download_path,
            processed_path=processed_path,
        )

    def run_all(
        self,
        source_system: Optional[Sequence[str]] = None,
        data_sources: Optional[Sequence[str]] = None,
        only_active: bool = True,
        stop_on_error: bool = False,
    ) -> dict[str, int]:
        """Run ETL for all active data sources (resume-friendly)."""
        download_path, processed_path = self._paths()
        return self._manager().run_all(
            data_sources=data_sources,
            source_system=source_system,
            download_path=download_path,
            processed_path=processed_path,
            only_active=only_active,
            stop_on_error=stop_on_error,
        )

    def rollback(
        self,
        data_sources: Optional[Sequence[str]] = None,
        source_system: Optional[Sequence[str]] = None,
        package_ids: Optional[Sequence[int]] = None,
        delete_files: bool = False,
    ) -> bool:
        """Rollback ETL data for given data sources or package IDs."""
        download_path, processed_path = self._paths()
        return self._manager().rollback(
            data_sources=data_sources,
            source_system=source_system,
            package_ids=package_ids,
            delete_files=delete_files,
            download_path=download_path,
            processed_path=processed_path,
        )

    def status(self) -> list[dict]:
        """Return the latest ETL run status for each registered data source."""
        return self._manager().status()
