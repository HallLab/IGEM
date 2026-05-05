"""
Quality-control filters and phenotype transformations.

All operations are pure functions: they accept :class:`Genotypes` /
:class:`Phenotypes` and return new instances, preserving the role
metadata attached to phenotype wrappers. Genotype filters rely on
sgkit's stats helpers (``variant_stats``, ``sample_stats``,
``hardy_weinberg_test``) so they stay lazy on biobank-scale inputs.
"""

from igem.modules.modify.genotypes import (
    filter_biallelic,
    filter_hwe,
    filter_maf,
    filter_missingness_samples,
    filter_missingness_variants,
)
from igem.modules.modify.phenotypes import (
    categorize,
    drop_missing,
    recode,
)

__all__ = [
    "filter_biallelic",
    "filter_maf",
    "filter_missingness_variants",
    "filter_missingness_samples",
    "filter_hwe",
    "categorize",
    "recode",
    "drop_missing",
]
