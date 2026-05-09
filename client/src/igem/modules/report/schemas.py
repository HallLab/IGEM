from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class ReportInfo(BaseModel):
    name: str
    version: str
    description: str


class ReportRunRequest(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)
    columns: Optional[list[str]] = None


class ReportRunResponse(BaseModel):
    status: str
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
