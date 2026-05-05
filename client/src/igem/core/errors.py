from __future__ import annotations

from typing import Any


class IGEMError(Exception):
    """Base exception for the IGEM client."""


class IGEMAPIError(IGEMError):
    """Raised when the IGEM server returns a non-2xx response."""

    def __init__(self, status_code: int, detail: Any) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"[{status_code}] {detail}")


class IGEMConfigError(IGEMError):
    """Raised when client configuration is missing or invalid."""
