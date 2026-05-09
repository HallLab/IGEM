"""
Genotype encodings used by ``association_study`` and
``interaction_study`` when the regressor is a variant.

Each encoding maps a per-variant dosage vector :math:`x \\in \\{0, 1, 2\\}`
(allele count, with ``-1`` for missing) onto a regressor representation
suited for a different genetic-architecture assumption:

- ``additive``   — dosage as-is. Linear allele-count effect.
- ``dominant``   — :math:`\\mathbb{1}[x \\geq 1]`. Any alt allele triggers.
- ``recessive``  — :math:`\\mathbb{1}[x = 2]`. Two alt alleles required.
- ``codominant`` — one-hot 3 levels (ref / het / hom-alt), reference = ref.
- ``edge``       — biologically-informed scoring via per-variant lookup
  (see :func:`encode_edge`).

All functions return a ``pandas.DataFrame`` with the sample as index
and one or more columns per variant. Missing dosages (``-1``) become
``NaN`` in the output.
"""
from __future__ import annotations

from typing import Iterable, Literal, Optional

import numpy as np
import pandas as pd


EncodingName = Literal[
    "additive", "dominant", "recessive", "codominant", "edge",
]
_ENCODINGS = ("additive", "dominant", "recessive", "codominant", "edge")


def encode(
    dosage: pd.DataFrame,
    *,
    method: EncodingName = "additive",
    edge_encoding_info: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Apply ``method`` to a (samples × variants) dosage frame.

    Input ``dosage`` has variant_id as columns, sample_id as index, and
    integer values in :math:`\\{0, 1, 2, -1\\}` (``-1`` = missing).
    """
    if method not in _ENCODINGS:
        raise ValueError(
            f"method must be one of {list(_ENCODINGS)}; got {method!r}"
        )

    arr = dosage.to_numpy(dtype=float)
    arr = np.where(arr < 0, np.nan, arr)
    samples = dosage.index
    variants = list(dosage.columns)

    if method == "additive":
        return pd.DataFrame(arr, index=samples, columns=variants)

    if method == "dominant":
        out = np.where(np.isnan(arr), np.nan, (arr >= 1).astype(float))
        return pd.DataFrame(out, index=samples, columns=variants)

    if method == "recessive":
        out = np.where(np.isnan(arr), np.nan, (arr == 2).astype(float))
        return pd.DataFrame(out, index=samples, columns=variants)

    if method == "codominant":
        return _encode_codominant(arr, samples=samples, variants=variants)

    # method == "edge"
    if edge_encoding_info is None:
        raise ValueError(
            "encoding='edge' requires edge_encoding_info "
            "(per-variant DataFrame mapping genotype → score)"
        )
    return _encode_edge(
        arr, samples=samples, variants=variants,
        edge_encoding_info=edge_encoding_info,
    )


def _encode_codominant(
    arr: np.ndarray,
    *,
    samples: pd.Index,
    variants: Iterable[str],
) -> pd.DataFrame:
    """
    One-hot encode each variant into two columns: ``<v>_het`` (1 if
    dosage==1) and ``<v>_hom_alt`` (1 if dosage==2). The hom-ref level
    (dosage==0) is the implicit reference.

    NaN dosages propagate to NaN in both encoded columns.
    """
    pieces: list[pd.DataFrame] = []
    for col_idx, variant in enumerate(variants):
        col = arr[:, col_idx]
        het = np.where(np.isnan(col), np.nan, (col == 1).astype(float))
        hom_alt = np.where(np.isnan(col), np.nan, (col == 2).astype(float))
        pieces.append(
            pd.DataFrame(
                {
                    f"{variant}_het": het,
                    f"{variant}_hom_alt": hom_alt,
                },
                index=samples,
            )
        )
    return pd.concat(pieces, axis=1)


def _encode_edge(
    arr: np.ndarray,
    *,
    samples: pd.Index,
    variants: Iterable[str],
    edge_encoding_info: pd.DataFrame,
) -> pd.DataFrame:
    """
    Map dosage to a custom score per variant via ``edge_encoding_info``.

    Expected schema for ``edge_encoding_info``:

    - index: ``variant_id``
    - columns: ``score_0``, ``score_1``, ``score_2`` — values to
      substitute for dosages 0, 1, 2 respectively (typically derived
      from biological evidence, e.g. functional impact scores or
      knowledge-graph-derived weights).

    Variants present in ``arr`` but absent from ``edge_encoding_info``
    raise ``ValueError`` — the user must explicitly score every variant
    they pass in.
    """
    required = ("score_0", "score_1", "score_2")
    missing_cols = [c for c in required if c not in edge_encoding_info.columns]
    if missing_cols:
        raise ValueError(
            f"edge_encoding_info missing required columns: {missing_cols}; "
            f"expected {list(required)}"
        )

    info = edge_encoding_info[list(required)]
    variants = list(variants)
    missing_variants = [v for v in variants if v not in info.index]
    if missing_variants:
        raise ValueError(
            f"edge_encoding_info missing entries for {len(missing_variants)} "
            f"variants (e.g. {missing_variants[:3]}); every variant must "
            f"have a score row"
        )

    out = np.full_like(arr, np.nan)
    for col_idx, variant in enumerate(variants):
        scores_row = info.loc[variant]
        scores = np.array(
            [scores_row["score_0"], scores_row["score_1"], scores_row["score_2"]],
            dtype=float,
        )
        col = arr[:, col_idx]
        valid = ~np.isnan(col)
        out[valid, col_idx] = scores[col[valid].astype("int64")]
    return pd.DataFrame(out, index=samples, columns=variants)
