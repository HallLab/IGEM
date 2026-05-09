from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd


PathLike = Union[str, Path]


def read_table(source: PathLike, **kwargs) -> pd.DataFrame:
    """
    Read a tabular file into a ``pandas.DataFrame``.

    Format is inferred from the suffix:

      - ``.parquet``                                 -> ``pd.read_parquet``
      - ``.xpt``                                     -> SAS XPT via ``pd.read_sas``
      - ``.tsv`` / ``.tsv.gz`` / ``.txt`` / ``.txt.gz``
                                                     -> tab-separated via ``pd.read_table``
      - any other suffix (``.csv``, ``.csv.gz``, ...)
                                                     -> ``pd.read_csv``

    ``**kwargs`` are forwarded to the underlying pandas reader. Gzip-
    compressed CSV/TSV are handled transparently by pandas.

    Use this when you just need a DataFrame (annotations, gene lists,
    SNP→gene maps, ad-hoc tables). For phenotypes with role metadata,
    use :func:`igem.modules.data.read_phenotypes`. For GWAS / PheWAS
    summary statistics with a canonical schema, use
    :func:`igem.modules.data.read_sumstats`.
    """
    path = Path(source)
    name = path.name.lower()

    if name.endswith(".parquet"):
        return pd.read_parquet(path, **kwargs)
    if name.endswith(".xpt"):
        return pd.read_sas(path, format="xport", encoding="utf-8")
    if name.endswith((".tsv", ".tsv.gz", ".txt", ".txt.gz")):
        return pd.read_table(path, **kwargs)
    return pd.read_csv(path, **kwargs)
