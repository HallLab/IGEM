"""
Data loading for IGEM.

Genotype loaders return :class:`Genotypes` (a thin wrapper around an
sgkit-format ``xarray.Dataset``). Phenotype loaders return
:class:`Phenotypes` (a wrapper around ``pandas.DataFrame`` with role
metadata).

Genotype I/O is lazy / Dask-backed where the backend supports it; call
``.to_numpy()`` only after subsetting.
"""

from igem.modules.data.genotypes import (
    Genotypes,
    read_plink,
    read_vcf,
    read_zarr,
)
from igem.modules.data.phenotypes import (
    Phenotypes,
    read_nhanes_xpt,
    read_phenotypes,
)

__all__ = [
    "Genotypes",
    "Phenotypes",
    "read_plink",
    "read_vcf",
    "read_zarr",
    "read_phenotypes",
    "read_nhanes_xpt",
]
