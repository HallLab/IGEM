from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from igem.modules.report.schemas import ReportInfo, ReportRunResponse


class ReportResult:
    """
    Wrapper around a report execution returned by the IGEM server.

    Attributes:
        message: multiline server-side execution log (from the backend logger)
        elapsed_seconds: total server-side execution time
        df: pandas DataFrame built from the server rows
        stats: dict with total_rows and, when the report produces a 'status'
            column, counts per status value (e.g. found / not_found)
        report: ReportInfo with name/version/description
    """

    def __init__(self, response: ReportRunResponse) -> None:
        self._response = response
        self.df: pd.DataFrame = pd.DataFrame(
            response.rows, columns=response.columns
        )

    @property
    def message(self) -> str:
        return self._response.message

    @property
    def elapsed_seconds(self) -> float:
        return self._response.elapsed_seconds

    @property
    def stats(self) -> dict[str, Any]:
        return self._response.stats

    @property
    def report(self) -> ReportInfo:
        return self._response.report

    def save_csv(self, path: str | Path, index: bool = False) -> Path:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(out, index=index)
        return out

    def __repr__(self) -> str:
        return (
            f"<ReportResult report={self.report.name!r} "
            f"rows={len(self.df)} elapsed={self.elapsed_seconds:.2f}s>"
        )
