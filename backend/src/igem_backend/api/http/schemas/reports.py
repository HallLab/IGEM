from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class ReportInfo(BaseModel):
    name: str
    version: str
    description: str


class ReportRunRequest(BaseModel):
    params: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Report-specific keyword arguments (passed verbatim to the "
            "report's run method). See 'GET /reports/{name}' for the "
            "expected shape of each report."
        ),
    )
    columns: Optional[list[str]] = Field(
        default=None,
        description=(
            "Optional column subset applied after the report runs. "
            "Unknown columns cause a 400 response."
        ),
    )


class ReportRunResponse(BaseModel):
    status: Literal["ok"]
    report: ReportInfo
    message: str
    elapsed_seconds: float
    columns: list[str]
    rows: list[dict[str, Any]]
    stats: dict[str, Any]


class ReportListResponse(BaseModel):
    reports: list[ReportInfo]


class ReportExplainResponse(BaseModel):
    name: str
    markdown: str
