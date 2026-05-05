from __future__ import annotations

from fastapi import HTTPException, Request, status

from igem_backend.ge import GE


def get_ge(request: Request) -> GE:
    ge = getattr(request.app.state, "ge", None)
    if ge is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="GE instance is not initialized on the app state.",
        )
    return ge
