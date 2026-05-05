"""Genome-Wide Association Study via sgkit's vectorised regression."""
from __future__ import annotations

from typing import Iterable, Optional

import numpy as np
import pandas as pd
import xarray as xr

from igem.modules.analyze._family import infer_family, validate_family
from igem.modules.analyze.results import RegressionResults, make_metadata
from igem.modules.data import Genotypes, Phenotypes


def gwas(
    geno: Genotypes,
    phen: Phenotypes,
    outcome: str,
    *,
    covariates: Optional[Iterable[str]] = None,
    family: Optional[str] = None,
) -> RegressionResults:
    """
    Variant-wise association via sgkit ``gwas_linear_regression``.

    For each variant, fits

        outcome ~ dosage + covariates

    where ``dosage`` is the count of non-reference alleles per sample
    (assumes biallelic input — run :func:`igem.modify.filter_biallelic`
    first if your data has multi-allelic sites).

    Parameters
    ----------
    geno : Genotypes
        Genotype wrapper (sgkit-format Dataset). Samples are aligned
        to ``phen`` by ID; the intersection is used for the regression.
    phen : Phenotypes
        Phenotype wrapper. Provides the outcome and (default)
        covariates plus the ``sample_id`` mapping.
    outcome : str
        Column name in ``phen.df``.
    covariates : iterable of str, optional
        Columns from ``phen.df`` to include as covariates. Defaults to
        ``phen.covariates``.
    family : str, optional
        ``"linear"`` or ``"logistic"``. Auto-detected from the outcome
        when ``None``. Logistic GWAS is not implemented in this phase
        — sgkit 0.10 has no vectorised logistic GWAS, and a per-variant
        statsmodels loop on biobank-scale data is not viable.

    Returns
    -------
    RegressionResults
        One row per variant: ``variable, n, beta, se, ci_low, ci_high,
        p_value``. ``variable`` carries the ``variant_id`` from the
        Dataset. ``n`` is the number of samples used after aligning
        and dropping NaNs in outcome / covariates (constant across
        variants; per-variant missing dosage is handled by sgkit).
    """
    if outcome not in phen.df.columns:
        raise ValueError(
            f"outcome {outcome!r} not in phenotype dataframe; "
            f"columns: {list(phen.df.columns)}"
        )

    cov_list = (
        list(covariates) if covariates is not None else list(phen.covariates)
    )
    missing = [c for c in cov_list if c not in phen.df.columns]
    if missing:
        raise ValueError(
            f"covariates not in phenotype dataframe: {missing}"
        )

    fam = family or infer_family(phen.df[outcome])
    validate_family(fam)
    if fam != "linear":
        raise NotImplementedError(
            "GWAS is only implemented for family='linear' in this phase. "
            "sgkit 0.10 has no vectorised logistic GWAS; per-variant "
            "logistic loops on biobank-scale data would be too slow. "
            "Track this limitation in the Phase 3 backlog."
        )

    aligned, n_dropped = _align_phen_to_geno(
        geno=geno, phen=phen, outcome=outcome, covariates=cov_list,
    )
    if aligned.n_samples == 0:
        raise ValueError(
            "no overlapping samples between geno and phen after dropna"
        )
    if aligned.n_samples < (len(cov_list) + 3):
        raise ValueError(
            f"insufficient overlapping samples: {aligned.n_samples} for "
            f"{len(cov_list) + 1} parameters"
        )

    ds = aligned.geno_subset.ds.copy()
    ds = _attach_dosage(ds)
    ds = _attach_phen_variables(
        ds=ds, df=aligned.phen_df,
        outcome=outcome, covariates=cov_list,
    )

    import sgkit

    out = sgkit.gwas_linear_regression(
        ds,
        dosage="call_dosage",
        covariates=cov_list,
        traits=[outcome],
    )

    beta = np.asarray(out["variant_linreg_beta"].values).ravel()
    tval = np.asarray(out["variant_linreg_t_value"].values).ravel()
    pval = np.asarray(out["variant_linreg_p_value"].values).ravel()
    se = np.divide(
        np.abs(beta), np.abs(tval),
        out=np.full_like(beta, np.nan, dtype=float),
        where=np.abs(tval) > 0,
    )

    ci_low = beta - 1.96 * se
    ci_high = beta + 1.96 * se

    variant_ids = (
        np.asarray(out["variant_id"].values).astype(object)
        if "variant_id" in out.variables
        else np.array([f"V{i}" for i in range(len(beta))], dtype=object)
    )

    df = pd.DataFrame(
        {
            "variable": variant_ids,
            "n": np.full(len(beta), aligned.n_samples, dtype=int),
            "beta": beta,
            "se": se,
            "ci_low": ci_low,
            "ci_high": ci_high,
            "p_value": pval,
        }
    )
    formula = (
        f"{outcome} ~ {{variant}} + " + " + ".join(cov_list)
        if cov_list
        else f"{outcome} ~ {{variant}}"
    )

    return RegressionResults(
        df=df,
        family=fam,
        outcome=outcome,
        covariates=cov_list,
        formula_template=formula,
        errors=pd.DataFrame(columns=["variable", "error"]),
        metadata=make_metadata(
            n_samples=aligned.n_samples,
            n_dropped=n_dropped,
            extras={
                "call": "gwas",
                "n_variants": int(len(beta)),
                "backend": "sgkit.gwas_linear_regression",
            },
        ),
    )


# ----------------------------------------------------------------------
# internals
# ----------------------------------------------------------------------
class _Aligned:
    __slots__ = ("geno_subset", "phen_df", "n_samples")

    def __init__(self, geno_subset: Genotypes, phen_df: pd.DataFrame) -> None:
        self.geno_subset = geno_subset
        self.phen_df = phen_df
        self.n_samples = int(len(phen_df))


def _align_phen_to_geno(
    *,
    geno: Genotypes,
    phen: Phenotypes,
    outcome: str,
    covariates: list[str],
) -> tuple[_Aligned, int]:
    """
    Intersect samples by ID, subset both, drop NaN in outcome/covariates,
    and return phen rows in the geno's sample order.
    """
    geno_samples = (
        np.asarray(geno.ds["sample_id"].values).astype(str).tolist()
    )
    phen_indexed = phen.df.set_index(
        phen.df[phen.sample_id_col].astype(str)
    )
    common_in_geno_order = [s for s in geno_samples if s in phen_indexed.index]

    geno_sub = geno.select(samples=common_in_geno_order)
    phen_aligned = phen_indexed.loc[common_in_geno_order]

    keep_cols = [outcome, *covariates]
    valid = phen_aligned[keep_cols].dropna()
    n_dropped = int(len(phen_aligned) - len(valid))

    if len(valid) != len(phen_aligned):
        valid_ids = valid.index.astype(str).tolist()
        geno_sub = geno_sub.select(samples=valid_ids)

    return _Aligned(geno_subset=geno_sub, phen_df=valid), n_dropped


def _attach_dosage(ds: xr.Dataset) -> xr.Dataset:
    """
    Add a ``call_dosage`` variable (variants × samples) computed as the
    number of non-reference alleles per sample. Missing genotypes
    (any ploid is -1) become NaN.

    Stays Dask-friendly: uses xarray ops, no numpy materialization.
    """
    if "call_dosage" in ds.variables:
        return ds
    cg = ds["call_genotype"]
    valid = cg >= 0
    alt_count = xr.where(valid, (cg > 0).astype("int8"), 0).sum(dim="ploidy")
    all_missing = (~valid).all(dim="ploidy")
    dosage = xr.where(all_missing, np.nan, alt_count.astype("float64"))
    ds = ds.assign(call_dosage=dosage)
    return ds


def _attach_phen_variables(
    *,
    ds: xr.Dataset,
    df: pd.DataFrame,
    outcome: str,
    covariates: list[str],
) -> xr.Dataset:
    """Attach trait + covariate arrays (samples-dim) as ds variables."""
    ds = ds.assign({outcome: ("samples", df[outcome].to_numpy(dtype=float))})
    for c in covariates:
        ds = ds.assign({c: ("samples", df[c].to_numpy(dtype=float))})
    return ds
