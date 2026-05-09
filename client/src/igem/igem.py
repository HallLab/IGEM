from __future__ import annotations

from typing import Any, Optional

from igem.config import (
    get_api_key_from_config,
    get_server_url_from_config,
    settings,
)
from igem.core.components.analyze_component import AnalyzeComponent
from igem.core.components.data_component import DataComponent
from igem.core.components.describe_component import DescribeComponent
from igem.core.components.modify_component import ModifyComponent
from igem.core.components.plot_component import PlotComponent
from igem.core.components.report_component import ReportComponent
from igem.core.core import IGEMCore


class IGEM:
    """
    IGEM client facade — local analysis + IGEM-server knowledge base.

    Three transport modes share the same component API:

        # 1. HTTP server (laptop connected, production):
        with IGEM(server_url="http://localhost:8000") as igem:
            ...

        # 2. Embedded server (laptop offline, HPC SLURM):
        #    IGEM-Server runs in-process against a Parquet snapshot.
        with IGEM(server_url="embedded:///path/to/snapshot") as igem:
            ...

        # 3. Default from .igem.toml or IGEM_URL env var:
        with IGEM() as igem:
            ...

    Embedded mode requires installing the optional backend dep:
        pip install 'igem[embedded]'

    Usage example (transport-agnostic):

        from igem import IGEM

        with IGEM() as igem:
            # local data loading (no transport involved):
            geno = igem.data.read_plink("path/to/prefix")
            phen = igem.data.read_phenotypes("nhanes.csv",
                                             outcomes=["GLUCOSE"])

            # server-side knowledge queries:
            result = igem.report.gene_annotations(input_values=["TP53"])

    The ``server_url`` falls back to:
      1. constructor argument
      2. IGEM_URL env var
      3. .igem.toml [client] server_url
      4. http://localhost:8000

    Authentication is not enforced today; ``api_key`` (and the ``IGEM_API_KEY``
    env var / ``[client] api_key`` toml entry) are accepted as a forward-
    compatible placeholder.
    """

    def __init__(
        self,
        server_url: Optional[str] = None,
        api_key: Optional[str] = None,
        debug_mode: bool = False,
        timeout_seconds: Optional[float] = None,
    ) -> None:
        resolved_url = (
            server_url
            or get_server_url_from_config()
            or settings.default_server_url
        )
        resolved_key = api_key or get_api_key_from_config()
        resolved_timeout = timeout_seconds or settings.request_timeout_seconds

        self.core = IGEMCore(
            server_url=resolved_url,
            api_key=resolved_key,
            debug_mode=debug_mode,
            timeout_seconds=resolved_timeout,
        )

        self.data = DataComponent(self.core)
        self.modify = ModifyComponent(self.core)
        self.describe = DescribeComponent(self.core)
        self.analyze = AnalyzeComponent(self.core)
        self.report = ReportComponent(self.core)
        self.plot = PlotComponent(self.core)

    # ------------------------------------------------------------------
    # Top-level convenience
    # ------------------------------------------------------------------
    def health(self) -> dict[str, Any]:
        resp = self.core.http.get("/health")
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        self.core.close()

    def __enter__(self) -> "IGEM":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"<IGEM version={self.core.version} "
            f"server_url={self.core.server_url}>"
        )
