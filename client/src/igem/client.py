import httpx

from igem.models import HealthResponse


class IGEM:
    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        self._http = httpx.Client(base_url=base_url, timeout=timeout)

    def health(self) -> HealthResponse:
        response = self._http.get("/health")
        response.raise_for_status()
        return HealthResponse.model_validate(response.json())

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "IGEM":
        return self

    def __exit__(self, *_) -> None:
        self.close()
