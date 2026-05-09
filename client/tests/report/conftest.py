"""
Shared fixtures for the report/ test suite.

Provides a FakeHttp class that mocks the .get / .post surface used by
ReportManager, plus pre-canned response bodies for the common report
operations (list / explain / run).

The igem_with_fake_http fixture builds a real IGEM facade and replaces
the lazily-initialised http client with a FakeHttp before any component
touches it, so facade smoke tests can exercise the delegation chain
without a server.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


class FakeResponse:
    """Stand-in for httpx.Response with the surface ReportManager touches."""

    def __init__(
        self,
        status_code: int,
        json_body: Any = None,
        text: str | None = None,
    ) -> None:
        self.status_code = status_code
        self._json = json_body
        self.text = (
            text
            if text is not None
            else json.dumps(json_body)
            if json_body is not None
            else ""
        )

    def json(self) -> Any:
        if self._json is None:
            raise ValueError("no JSON body set on FakeResponse")
        return self._json


class FakeHttp:
    """
    Mock for the IGEMCore.http property.

    Records every request and lets tests pre-program responses keyed by
    (method, url). Unprogrammed routes return a 404 so a missing
    set_response() call shows up as an explicit error.
    """

    def __init__(self) -> None:
        self.gets: list[str] = []
        self.posts: list[tuple[str, Any]] = []
        self._responses: dict[tuple[str, str], FakeResponse] = {}

    def set_response(
        self,
        method: str,
        url: str,
        status_code: int = 200,
        json_body: Any = None,
        text: str | None = None,
    ) -> None:
        self._responses[(method.upper(), url)] = FakeResponse(
            status_code, json_body=json_body, text=text
        )

    def get(self, url: str) -> FakeResponse:
        self.gets.append(url)
        return self._responses.get(
            ("GET", url),
            FakeResponse(404, {"detail": f"no fake response for GET {url}"}),
        )

    def post(self, url: str, json: Any = None) -> FakeResponse:
        self.posts.append((url, json))
        return self._responses.get(
            ("POST", url),
            FakeResponse(404, {"detail": f"no fake response for POST {url}"}),
        )


@pytest.fixture
def fake_http() -> FakeHttp:
    return FakeHttp()


# ---------------------------------------------------------------------------
# Sample server responses
# ---------------------------------------------------------------------------
@pytest.fixture
def list_response() -> dict:
    return {
        "reports": [
            {
                "name": "gene_annotations",
                "version": "1.0.0",
                "description": "Master gene annotation table.",
            },
            {
                "name": "pathway_annotations",
                "version": "1.0.0",
                "description": "Master pathway annotation table.",
            },
        ]
    }


@pytest.fixture
def explain_response() -> dict:
    return {
        "name": "gene_annotations",
        "markdown": "# gene_annotations\n\nDocs body.",
    }


@pytest.fixture
def gene_run_response() -> dict:
    """A gene_annotations run with one matched and one not_found row."""
    return {
        "status": "ok",
        "report": {
            "name": "gene_annotations",
            "version": "1.0.0",
            "description": "Master gene annotation table.",
        },
        "message": "[report] Running 'gene_annotations'...",
        "elapsed_seconds": 0.123,
        "columns": ["input_value", "gene_symbol", "status"],
        "rows": [
            {"input_value": "TP53", "gene_symbol": "TP53", "status": "found"},
            {"input_value": "NOPE", "gene_symbol": None, "status": "not_found"},
        ],
        "stats": {"total_rows": 2, "found": 1, "not_found": 1},
    }


# ---------------------------------------------------------------------------
# IGEM facade with HTTP swapped for FakeHttp
# ---------------------------------------------------------------------------
@pytest.fixture
def igem_with_fake_http(fake_http):
    """
    A real IGEM facade whose underlying HTTP client is replaced with a
    FakeHttp before any component touches it. Used by the
    ReportComponent smoke tests.
    """
    from igem import IGEM

    igem = IGEM(server_url="http://test.local")
    igem.core._http = fake_http
    try:
        yield igem
    finally:
        igem.core._http = None
