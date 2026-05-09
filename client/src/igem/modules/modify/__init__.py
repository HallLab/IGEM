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
    filter_by_region,
    filter_heterozygosity_outliers,
    filter_hwe,
    filter_maf,
    filter_missingness_samples,
    filter_missingness_variants,
    filter_variants,
    keep_snvs_only,
    prune_ld,
)
from igem.modules.modify.phenotypes import (
    auto_classify,
    colfilter,
    colfilter_min_cat_n,
    colfilter_min_n,
    colfilter_percent_zero,
    discretize,
    drop_missing,
    make_binary,
    make_categorical,
    make_continuous,
    merge_observations,
    merge_variables,
    move_variables,
    recode,
    remove_outliers,
    rowfilter_incomplete_obs,
    transform,
)
from igem.modules.modify.selectors import filter_samples

__all__ = [
    "filter_biallelic",
    "filter_maf",
    "filter_missingness_variants",
    "filter_missingness_samples",
    "filter_hwe",
    "filter_heterozygosity_outliers",
    "keep_snvs_only",
    "filter_variants",
    "filter_by_region",
    "filter_samples",
    "prune_ld",
    "discretize",
    "recode",
    "drop_missing",
    "transform",
    "remove_outliers",
    "auto_classify",
    "make_binary",
    "make_categorical",
    "make_continuous",
    "colfilter",
    "colfilter_min_n",
    "colfilter_min_cat_n",
    "colfilter_percent_zero",
    "merge_observations",
    "merge_variables",
    "move_variables",
    "rowfilter_incomplete_obs",
]
