"""
Cross-cutting selectors that apply to either ``Genotypes`` or
``Phenotypes``.

Genotype-only filters live in :mod:`igem.modules.modify.genotypes`;
phenotype-only transforms live in :mod:`igem.modules.modify.phenotypes`.
This module hosts the small surface where the same operation makes
sense on both.
"""
from __future__ import annotations

from typing import Iterable, Union

from igem.modules.data import Genotypes, Phenotypes


def filter_samples(
    obj: Union[Genotypes, Phenotypes],
    sample_ids: Iterable[str],
    *,
    keep: bool = True,
) -> Union[Genotypes, Phenotypes]:
    """
    Filter a ``Genotypes`` or ``Phenotypes`` by sample-ID list.

    With ``keep=True`` (default), retain only the listed samples.
    With ``keep=False``, drop them instead. IDs that are not in the
    wrapper are ignored silently.

    Common use cases:

    - **GWAS QC** — drop withdrawn-consent samples, related
      individuals identified by external IBD analysis, or
      cohort-specific outliers.
    - **EWAS subsetting** — restrict analysis to samples that survive
      a covariate-completeness filter computed externally.

    Wrapper-aware: role metadata (outcomes / covariates / exposures /
    survey design) and lazy backing store (xarray dask graphs for
    genotypes) are preserved.
    """
    ids = set(sample_ids)
    if isinstance(obj, Genotypes):
        all_samples = list(obj.samples)
        if keep:
            target = [s for s in all_samples if s in ids]
        else:
            target = [s for s in all_samples if s not in ids]
        return obj.select(samples=target)
    if isinstance(obj, Phenotypes):
        if keep:
            target = [s for s in obj.samples if s in ids]
        else:
            target = [s for s in obj.samples if s not in ids]
        return obj.select_samples(target)
    raise TypeError(
        f"filter_samples expects Genotypes or Phenotypes; "
        f"got {type(obj).__name__}"
    )
