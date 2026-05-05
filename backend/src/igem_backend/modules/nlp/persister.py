"""
EntityMatch persister — writes resolved NLP matches to the database.

Reads a DataFrame produced by the NLP transform step (entity_matches.parquet)
and inserts EntityMatch rows in batches. Each row in the DataFrame corresponds
to one entity mention found in a source text span.

No dedup check is performed within a single run: the parquet is the canonical
source of truth for what was found, and each ETLPackage produces its own set
of EntityMatch rows (no cross-package dedup needed at this layer).
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def persist_matches(
    df: pd.DataFrame,
    session: Session,
    etl_package_id: int,
    batch_size: int = 500,
) -> tuple[int, int]:
    """
    Persist EntityMatch rows from a DataFrame.

    Expected DataFrame columns (all required unless marked optional):
      source_record_id, source_field, text_hash (optional),
      matched_text, span_start, span_end, context,
      alias_id (nullable), entity_id,
      entity_type_id, entity_type_name, entity_domain,
      match_method, confidence, review_status

    Returns
    -------
    (created, errors)
    """
    from igem_backend.modules.db.models.model_nlp import EntityMatch

    if df.empty:
        return 0, 0

    created = errors = 0

    for i, (_, row) in enumerate(df.iterrows()):
        entity_id = _int(row.get("entity_id"))
        if entity_id is None:
            errors += 1
            continue

        alias_id = _int(row.get("alias_id"))

        session.add(EntityMatch(
            etl_package_id=etl_package_id,
            source_record_id=_str(row.get("source_record_id")),
            source_field=_str(row.get("source_field")),
            text_hash=_str(row.get("text_hash")),
            matched_text=(_str(row.get("matched_text")) or "")[:500],
            span_start=_int(row.get("span_start")),
            span_end=_int(row.get("span_end")),
            context=(_str(row.get("context")) or "")[:1000],
            alias_id=alias_id,
            entity_id=entity_id,
            match_method=_str(row.get("match_method")) or "exact_aho",
            confidence=float(row.get("confidence") or 0.0),
            review_status=_str(row.get("review_status")) or "auto",
        ))
        created += 1

        if (i + 1) % batch_size == 0:
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error("EntityMatch batch commit failed at row %d: %s", i + 1, e)
                errors += batch_size
                created -= batch_size

    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error("EntityMatch final commit failed: %s", e)
        errors += created % batch_size
        created -= created % batch_size

    return created, errors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _str(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s and s != "nan" else None


def _int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        f = float(val)
        return int(f) if not (f != f) else None  # NaN check
    except (ValueError, TypeError):
        return None
