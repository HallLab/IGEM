"""
Genotype descriptive statistics.

Wraps sgkit's stats helpers (``variant_stats``, ``sample_stats``,
``hardy_weinberg_test``) and returns user-facing pandas DataFrames /
dicts. HWE is attempted in :func:`variant_stats` but its absence does
not break the call — small or non-biallelic inputs may produce no
p-value.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from igem.modules.data import Genotypes


# ----------------------------------------------------------------------
# variant_stats
# ----------------------------------------------------------------------
def variant_stats(geno: Genotypes) -> pd.DataFrame:
    """
    Per-variant statistics as a DataFrame.

    Columns when available:
      ``variant_id``, ``contig``, ``position``,
      ``n_called``, ``n_het``, ``n_hom_ref``, ``n_hom_alt``,
      ``call_rate``, ``maf``, ``hwe_pvalue``.
    """
    import sgkit

    ds = sgkit.variant_stats(geno.ds)
    try:
        ds = sgkit.hardy_weinberg_test(ds)
    except Exception:
        pass

    df = pd.DataFrame()
    if "variant_id" in ds.variables:
        df["variant_id"] = (
            np.asarray(ds["variant_id"].values).astype(object)
        )
    if "variant_contig" in ds.variables and "contigs" in ds.attrs:
        df["contig"] = np.asarray(ds.attrs["contigs"])[
            ds["variant_contig"].values
        ]

    for label, var in (
        ("position", "variant_position"),
        ("n_called", "variant_n_called"),
        ("n_het", "variant_n_het"),
        ("n_hom_ref", "variant_n_hom_ref"),
        ("n_hom_alt", "variant_n_hom_alt"),
        ("call_rate", "variant_call_rate"),
    ):
        if var in ds.variables:
            df[label] = np.asarray(ds[var].values)

    if "variant_allele_frequency" in ds.variables:
        af = np.asarray(ds["variant_allele_frequency"].values)
        if af.ndim == 2 and af.shape[1] >= 2:
            maf = np.nanmin(af, axis=1)
        else:
            maf = np.minimum(af, 1.0 - af)
        df["maf"] = maf

    if "variant_hwe_p_value" in ds.variables:
        df["hwe_pvalue"] = np.asarray(ds["variant_hwe_p_value"].values)

    return df


# ----------------------------------------------------------------------
# sample_stats
# ----------------------------------------------------------------------
def sample_stats(geno: Genotypes) -> pd.DataFrame:
    """
    Per-sample statistics as a DataFrame.

    Columns when available:
      ``sample_id``, ``n_called``, ``n_het``, ``n_hom_ref``,
      ``n_hom_alt``, ``call_rate``.
    """
    import sgkit

    ds = sgkit.sample_stats(geno.ds)
    df = pd.DataFrame()
    if "sample_id" in ds.variables:
        df["sample_id"] = (
            np.asarray(ds["sample_id"].values).astype(object)
        )
    for label, var in (
        ("n_called", "sample_n_called"),
        ("n_het", "sample_n_het"),
        ("n_hom_ref", "sample_n_hom_ref"),
        ("n_hom_alt", "sample_n_hom_alt"),
        ("call_rate", "sample_call_rate"),
    ):
        if var in ds.variables:
            df[label] = np.asarray(ds[var].values)
    return df


# ----------------------------------------------------------------------
# genotype_summary
# ----------------------------------------------------------------------
def genotype_summary(geno: Genotypes) -> dict[str, Any]:
    """
    High-level overview for printing or logging. Aggregates per-variant
    and per-sample stats into a flat ``dict`` of scalars.
    """
    vstats = variant_stats(geno)
    sstats = sample_stats(geno)

    summary: dict[str, Any] = {
        "n_samples": geno.n_samples,
        "n_variants": geno.n_variants,
    }

    if "call_rate" in vstats.columns and len(vstats):
        summary["variant_call_rate_mean"] = float(vstats["call_rate"].mean())
        summary["variant_call_rate_min"] = float(vstats["call_rate"].min())

    if "call_rate" in sstats.columns and len(sstats):
        summary["sample_call_rate_mean"] = float(sstats["call_rate"].mean())
        summary["sample_call_rate_min"] = float(sstats["call_rate"].min())

    if "maf" in vstats.columns and len(vstats):
        summary["maf_mean"] = float(vstats["maf"].mean())
        summary["n_variants_maf_lt_0.01"] = int(
            (vstats["maf"] < 0.01).sum()
        )
        summary["n_variants_maf_lt_0.05"] = int(
            (vstats["maf"] < 0.05).sum()
        )

    return summary
