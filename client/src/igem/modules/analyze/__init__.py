"""
Statistical analysis.

Public surface:

- :class:`RegressionResults` — chainable, immutable per-test results.
- :func:`association_study` — unified EWAS / GWAS / PheWAS entrypoint
  with list-of-outcomes, list-of-regressors, configurable encoding,
  parallelism, and per-outcome p-value correction.
- :func:`interaction_study` — pairwise interaction tests via LRT.
- :func:`ewas` — thin wrapper of ``association_study`` (single outcome,
  phenotype regressors).
- :func:`gwas` — sgkit-vectorised additive-linear GWAS (kept for
  biobank-scale performance).
- :func:`lrt` — likelihood ratio test on nested models (utility).
- :func:`apply_correction` — multi-test p-value adjustment.
- :func:`infer_family` — outcome dtype → ``"linear"`` / ``"logistic"``.
"""

from igem.modules.analyze._corrections import (
    apply_correction,
    list_methods,
)
from igem.modules.analyze._family import infer_family, validate_family
from igem.modules.analyze.association import association_study
from igem.modules.analyze.ewas import ewas
from igem.modules.analyze.gwas import gwas
from igem.modules.analyze.interaction import interaction_study
from igem.modules.analyze.lrt import lrt
from igem.modules.analyze.results import RegressionResults

__all__ = [
    "RegressionResults",
    "association_study",
    "interaction_study",
    "ewas",
    "gwas",
    "lrt",
    "apply_correction",
    "list_methods",
    "infer_family",
    "validate_family",
]
