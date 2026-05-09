"""
Descriptive statistics for phenotypes and genotypes.

Phenotype helpers (read-only, fast on tabular data):
  - :func:`summarize` — per-column summary
  - :func:`summarize_by` — per-group, per-column summary
  - :func:`dataset_summary` — dataset-level overview
  - :func:`missing_report`
  - :func:`correlation_matrix`
  - :func:`correlation_pairs`
  - :func:`crosstab` — 2-way contingency table
  - :func:`value_counts`
  - :func:`skewness`

Genotype helpers (delegate stats to sgkit):
  - :func:`variant_stats`
  - :func:`sample_stats`
  - :func:`heterozygosity` — per-sample het rate + outlier flag
  - :func:`genotype_summary`

All functions return new pandas DataFrames / dicts and never mutate
the input wrapper.
"""

from igem.modules.describe.genotypes import (
    genotype_summary,
    heterozygosity,
    sample_stats,
    variant_stats,
)
from igem.modules.describe.phenotypes import (
    correlation_matrix,
    correlation_pairs,
    crosstab,
    dataset_summary,
    missing_report,
    skewness,
    summarize,
    summarize_by,
    value_counts,
)

__all__ = [
    "summarize",
    "summarize_by",
    "dataset_summary",
    "missing_report",
    "correlation_matrix",
    "correlation_pairs",
    "crosstab",
    "value_counts",
    "skewness",
    "variant_stats",
    "sample_stats",
    "heterozygosity",
    "genotype_summary",
]
