__version__ = "0.1.0"

from igem.core.errors import IGEMAPIError, IGEMConfigError, IGEMError
from igem.igem import IGEM
from igem.modules import data
from igem.modules.report.result import ReportResult

__all__ = [
    "IGEM",
    "IGEMError",
    "IGEMAPIError",
    "IGEMConfigError",
    "ReportResult",
    "data",
]
