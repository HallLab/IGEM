"""
Statistical analysis (Phase 1).

Public surface:

- :class:`RegressionResults` — chainable, immutable per-test results.
- :func:`ewas` — exposure-wide association study (linear / logistic).
- :func:`lrt` — likelihood ratio test on nested models.
- :func:`apply_correction` — multi-test p-value adjustment.
- :func:`infer_family` — outcome dtype → ``"linear"`` / ``"logistic"``.
"""

from igem.modules.analyze._corrections import (
    apply_correction,
    list_methods,
)
from igem.modules.analyze._family import infer_family, validate_family
from igem.modules.analyze.ewas import ewas
from igem.modules.analyze.gwas import gwas
from igem.modules.analyze.lrt import lrt
from igem.modules.analyze.results import RegressionResults

__all__ = [
    "RegressionResults",
    "ewas",
    "gwas",
    "lrt",
    "apply_correction",
    "list_methods",
    "infer_family",
    "validate_family",
]
