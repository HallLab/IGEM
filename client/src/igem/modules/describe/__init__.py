"""
Descriptive statistics for phenotypes and genotypes.

Phenotype helpers (read-only, fast on tabular data):
  - :func:`summarize`
  - :func:`missing_report`
  - :func:`correlation_matrix`
  - :func:`value_counts`

Genotype helpers (delegate stats to sgkit):
  - :func:`variant_stats`
  - :func:`sample_stats`
  - :func:`genotype_summary`

All functions return new pandas DataFrames / dicts and never mutate
the input wrapper.
"""

from igem.modules.describe.genotypes import (
    genotype_summary,
    sample_stats,
    variant_stats,
)
from igem.modules.describe.phenotypes import (
    correlation_matrix,
    missing_report,
    summarize,
    value_counts,
)

__all__ = [
    "summarize",
    "missing_report",
    "correlation_matrix",
    "value_counts",
    "variant_stats",
    "sample_stats",
    "genotype_summary",
]
