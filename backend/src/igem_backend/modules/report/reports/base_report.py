from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class ReportBase(ABC):
    """
    Abstract base for all IGEM curated reports.

    Subclasses must define REPORT_NAME, REPORT_VERSION, REPORT_DESCRIPTION
    and implement run(), available_columns(), example_input().
    """

    REPORT_NAME: str
    REPORT_VERSION: str = "1.0.0"
    REPORT_DESCRIPTION: str = ""

    @abstractmethod
    def run(self, session, **kwargs) -> pd.DataFrame:
        """Execute the report and return results as a DataFrame."""

    @abstractmethod
    def available_columns(self) -> list[str]:
        """Return the full ordered list of column names this report can produce."""

    @abstractmethod
    def example_input(self) -> dict[str, Any]:
        """Return a sample kwargs dict suitable for a demo run."""

    # -------------------------------------------------------------------------
    # Helpers available to all report subclasses
    # -------------------------------------------------------------------------

    def param(self, kwargs: dict, key: str, default=None):
        return kwargs.get(key, default)

    def resolve_input_list(self, value) -> list[str]:
        """Normalize a variety of input forms to a flat list of strings."""
        if value is None:
            return []
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        if isinstance(value, (list, tuple)):
            items: list[str] = []
            for v in value:
                items.extend(self.resolve_input_list(v))
            return items
        return [str(value)]
