from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd

from igem.modules.data.tables import read_table


PathLike = Union[str, Path]


CANONICAL_COLS = (
    "variant_id",
    "chrom",
    "pos",
    "effect_allele",
    "other_allele",
    "beta",
    "se",
    "pval",
    "n",
    "eaf",
)


# Canonical-name -> source-column-name, per upstream tool. Entries set
# to ``None`` mean the tool does not emit that column in its default output.
_PRESETS: dict[str, dict[str, Optional[str]]] = {
    "plink2": {
        "variant_id": "ID",
        "chrom": "#CHROM",
        "pos": "POS",
        "effect_allele": "A1",
        "other_allele": "REF",
        "beta": "BETA",
        "se": "SE",
        "pval": "P",
        "n": "OBS_CT",
        "eaf": "A1_FREQ",
    },
    "regenie": {
        "variant_id": "ID",
        "chrom": "CHROM",
        "pos": "GENPOS",
        "effect_allele": "ALLELE1",
        "other_allele": "ALLELE0",
        "beta": "BETA",
        "se": "SE",
        "pval": "LOG10P",
        "n": "N",
        "eaf": "A1FREQ",
    },
    "bolt": {
        "variant_id": "SNP",
        "chrom": "CHR",
        "pos": "BP",
        "effect_allele": "ALLELE1",
        "other_allele": "ALLELE0",
        "beta": "BETA",
        "se": "SE",
        "pval": "P_BOLT_LMM",
        "n": None,
        "eaf": "A1FREQ",
    },
    "gwas-catalog": {
        "variant_id": "hm_rsid",
        "chrom": "hm_chrom",
        "pos": "hm_pos",
        "effect_allele": "hm_effect_allele",
        "other_allele": "hm_other_allele",
        "beta": "hm_beta",
        "se": "standard_error",
        "pval": "p_value",
        "n": None,
        "eaf": "hm_effect_allele_frequency",
    },
}


def read_sumstats(
    source: PathLike,
    *,
    preset: Optional[str] = None,
    schema: Optional[dict[str, str]] = None,
    **read_kwargs,
) -> pd.DataFrame:
    """
    Read GWAS / PheWAS summary statistics into a DataFrame with a
    canonical schema.

    Canonical columns (kept when present in the source):
    ``variant_id, chrom, pos, effect_allele, other_allele,
    beta, se, pval, n, eaf``.

    Parameters
    ----------
    source : str | Path
        Path to a tabular file (``.tsv``/``.csv``/``.txt``/``.parquet``,
        optionally gzipped). Format is inferred via :func:`read_table`.
    preset : {"plink2", "regenie", "bolt", "gwas-catalog"}, optional
        Source-tool preset for the column mapping.
    schema : dict, optional
        ``canonical_name -> source_column_name`` mapping. Overrides the
        preset entry-by-entry; required when no preset matches.
    **read_kwargs : forwarded to :func:`read_table`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with one row per variant and columns from
        :data:`CANONICAL_COLS` that were present in the source.

    Notes
    -----
    Sumstats files in the wild use tab-separated layout regardless of
    suffix (``.glm.linear``, ``.regenie``, ``.bolt``, ...). For any
    suffix that :func:`read_table` does not recognise on its own, this
    function defaults ``sep="\\t"`` — pass an explicit ``sep=`` in
    ``read_kwargs`` to override.

    REGENIE reports ``LOG10P`` (i.e. ``-log10(p)``); when
    ``preset="regenie"`` the ``pval`` column is converted back to a plain
    p-value (override by passing ``schema={"pval": "..."}`` to keep the
    raw value). Source columns not in the canonical set are dropped — to
    keep them, read the raw file with :func:`read_table` instead.
    """
    name = str(source).lower()
    _table_suffixes = (
        ".parquet",
        ".xpt",
        ".tsv", ".tsv.gz",
        ".txt", ".txt.gz",
        ".csv", ".csv.gz",
    )
    if not name.endswith(_table_suffixes):
        read_kwargs.setdefault("sep", "\t")

    mapping: dict[str, str] = {}
    convert_log10p = False

    if preset is not None:
        if preset not in _PRESETS:
            raise ValueError(
                f"Unknown preset {preset!r}. "
                f"Available: {sorted(_PRESETS)}"
            )
        mapping.update(
            {k: v for k, v in _PRESETS[preset].items() if v is not None}
        )
        convert_log10p = preset == "regenie"

    if schema is not None:
        mapping.update(schema)
        if "pval" in schema:
            convert_log10p = False

    if not mapping:
        raise ValueError(
            "Provide `preset` or `schema` so source columns can be "
            "mapped to the canonical sumstats schema."
        )

    df = read_table(source, **read_kwargs)
    missing = [src for src in mapping.values() if src not in df.columns]
    if missing:
        raise KeyError(
            f"Source columns not present in {Path(source).name}: "
            f"{missing}. Available: {list(df.columns)}"
        )

    out = pd.DataFrame({canon: df[src] for canon, src in mapping.items()})

    if convert_log10p and "pval" in out.columns:
        out["pval"] = np.power(10.0, -out["pval"].astype(float))

    return out
