from __future__ import annotations

import json
import time
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, status

from igem_backend.api.http.deps import get_ge
from igem_backend.api.http.log_capture import capture_logger_output
from igem_backend.api.http.schemas.reports import (
    ReportExplainResponse,
    ReportInfo,
    ReportListResponse,
    ReportRunRequest,
    ReportRunResponse,
)
from igem_backend.ge import GE

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("", response_model=ReportListResponse)
def list_reports(ge: GE = Depends(get_ge)) -> ReportListResponse:
    reports = [ReportInfo(**r) for r in ge.report.list()]
    return ReportListResponse(reports=reports)


@router.get("/{name}", response_model=ReportExplainResponse)
def explain_report(name: str, ge: GE = Depends(get_ge)) -> ReportExplainResponse:
    available = {r["name"] for r in ge.report.list()}
    if name not in available:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report '{name}' not found. Available: {sorted(available)}",
        )
    return ReportExplainResponse(name=name, markdown=ge.report.explain(name))


@router.post("/{name}/run", response_model=ReportRunResponse)
def run_report(
    name: str,
    body: ReportRunRequest,
    ge: GE = Depends(get_ge),
) -> ReportRunResponse:
    registry = {r["name"]: r for r in ge.report.list()}
    meta = registry.get(name)
    if meta is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Report '{name}' not found. "
                f"Available: {sorted(registry.keys())}"
            ),
        )

    t0 = time.time()
    with capture_logger_output(ge.core.logger) as log_lines:
        try:
            df = ge.report.run(name, **body.params)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc),
            ) from exc
    elapsed = time.time() - t0

    if body.columns:
        available_cols = df.columns.tolist()
        unknown = [c for c in body.columns if c not in available_cols]
        if unknown:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Unknown column(s): {unknown}. "
                    f"Available: {available_cols}"
                ),
            )
        df = df[body.columns]

    return ReportRunResponse(
        status="ok",
        report=ReportInfo(**meta),
        message="\n".join(log_lines),
        elapsed_seconds=round(elapsed, 3),
        columns=df.columns.tolist(),
        rows=_df_to_records(df),
        stats=_compute_stats(df),
    )


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    # to_json handles NaN→null and preserves types; safer than to_dict for JSON.
    return json.loads(df.to_json(orient="records", date_format="iso"))


def _compute_stats(df: pd.DataFrame) -> dict[str, Any]:
    stats: dict[str, Any] = {"total_rows": int(len(df))}
    if "status" in df.columns and not df.empty:
        counts = df["status"].value_counts(dropna=False).to_dict()
        stats["by_status"] = {
            (str(k) if k is not None else "null"): int(v)
            for k, v in counts.items()
        }
    return stats
