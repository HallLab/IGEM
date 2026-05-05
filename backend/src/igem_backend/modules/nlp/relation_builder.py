"""
Relation builder — creates EntityRelationship rows from co-occurring EntityMatch.

Two entities are "co-occurring" when they both appear in the same source record
(same source_record_id). Each unique co-occurring pair becomes one
EntityRelationship row with:

  relation_type  = co_occurs_with
  discovery_method = "nlp"
  confidence_score = min(confidence_a, confidence_b)   — weakest link
  evidence_count  = number of distinct source records where the pair co-occurs

Guards:
  - Records with > MAX_ENTITIES_PER_RECORD distinct entities are skipped to
    avoid N² explosion (a description mentioning 50 entities would generate
    1,225 pairs, most of which are noise).
  - Self-pairs (entity_id_a == entity_id_b) are skipped.
  - Canonical ordering is handled by EntityQueryMixin._canonical_order, so
    (A, B) and (B, A) resolve to the same row in the DB.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

MAX_ENTITIES_PER_RECORD = 20


def build_from_matches(
    df: pd.DataFrame,
    session: Session,
    data_source_id: int,
    etl_package_id: int,
    batch_size: int = 500,
    max_entities_per_record: int = MAX_ENTITIES_PER_RECORD,
) -> tuple[int, int]:
    """
    Build co-occurrence EntityRelationship rows from an entity_matches DataFrame.

    Parameters
    ----------
    df:
        DataFrame with columns: source_record_id, entity_id, entity_type_id,
        confidence, review_status.
    session:
        Active SQLAlchemy session.
    data_source_id:
        FK for the NLP data source.
    etl_package_id:
        FK for the current ETL package.
    batch_size:
        Commit interval (number of new relationships).
    max_entities_per_record:
        Records with more distinct entities than this are skipped.

    Returns
    -------
    (created, skipped)
    """
    from igem_backend.modules.db.models.model_entities import (
        EntityRelationship,
        EntityRelationshipType,
        EntityType,
    )

    # Resolve rel_type_id for "co_occurs_with"
    rel_type = (
        session.query(EntityRelationshipType)
        .filter_by(code="co_occurs_with")
        .first()
    )
    if rel_type is None:
        logger.error("EntityRelationshipType 'co_occurs_with' not found.")
        return 0, 0

    rel_type_id = rel_type.id

    # Gene type id — for canonical ordering (Gene always entity_1)
    gene_type = session.query(EntityType).filter_by(name="Genes").first()
    gene_type_id = gene_type.id if gene_type else None

    # -----------------------------------------------------------------------
    # Step 1: Aggregate pair evidence across all source records
    # -----------------------------------------------------------------------
    # pair_key → (evidence_count, max_confidence, e1_type_id, e2_type_id, source_ref)
    pair_data: dict[tuple[int, int], dict] = defaultdict(
        lambda: {"count": 0, "confidence": 0.0, "e1_tid": None, "e2_tid": None, "source_ref": None}
    )

    needed_cols = {"source_record_id", "entity_id", "entity_type_id", "confidence"}
    if not needed_cols.issubset(df.columns):
        logger.error("entity_matches DataFrame missing columns: %s", needed_cols - set(df.columns))
        return 0, 0

    for record_id, group in df.groupby("source_record_id"):
        # Unique entities in this record
        entities = (
            group[["entity_id", "entity_type_id", "confidence"]]
            .drop_duplicates(subset=["entity_id"])
        )

        if len(entities) > max_entities_per_record:
            continue

        rows = entities.to_dict("records")
        for i, ma in enumerate(rows):
            for mb in rows[i + 1:]:
                ea_id = int(ma["entity_id"])
                eb_id = int(mb["entity_id"])

                if ea_id == eb_id:
                    continue

                # Canonical order: Gene first, else smaller id first
                ea_tid = _tid(ma["entity_type_id"])
                eb_tid = _tid(mb["entity_type_id"])
                e1_id, e1_tid, e2_id, e2_tid = _canonical(
                    ea_id, ea_tid, eb_id, eb_tid, gene_type_id
                )

                key = (e1_id, e2_id)
                conf = min(float(ma["confidence"]), float(mb["confidence"]))
                d = pair_data[key]
                d["count"] += 1
                d["confidence"] = max(d["confidence"], conf)
                d["e1_tid"] = e1_tid
                d["e2_tid"] = e2_tid
                if d["source_ref"] is None:
                    d["source_ref"] = str(record_id)

    if not pair_data:
        return 0, 0

    # -----------------------------------------------------------------------
    # Step 2: Load existing pairs to skip (unique constraint check)
    # -----------------------------------------------------------------------
    existing_pairs: set[tuple[int, int]] = {
        (row.entity_1_id, row.entity_2_id)
        for row in session.query(
            EntityRelationship.entity_1_id,
            EntityRelationship.entity_2_id,
        ).filter_by(
            relationship_type_id=rel_type_id,
            data_source_id=data_source_id,
        ).all()
    }

    # -----------------------------------------------------------------------
    # Step 3: Persist new pairs
    # -----------------------------------------------------------------------
    created = skipped = 0
    pending = 0

    for (e1_id, e2_id), d in pair_data.items():
        if (e1_id, e2_id) in existing_pairs:
            skipped += 1
            continue

        session.add(EntityRelationship(
            entity_1_id=e1_id,
            entity_2_id=e2_id,
            entity_1_type_id=d["e1_tid"],
            entity_2_type_id=d["e2_tid"],
            relationship_type_id=rel_type_id,
            data_source_id=data_source_id,
            etl_package_id=etl_package_id,
            discovery_method="nlp",
            confidence_score=round(d["confidence"], 4),
            evidence_count=d["count"],
            source_ref=d["source_ref"],
        ))
        existing_pairs.add((e1_id, e2_id))
        created += 1
        pending += 1

        if pending >= batch_size:
            try:
                session.commit()
                pending = 0
            except Exception as e:
                session.rollback()
                logger.error("Relation batch commit failed: %s", e)
                created -= pending
                skipped += pending
                pending = 0

    if pending:
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error("Relation final commit failed: %s", e)
            created -= pending
            skipped += pending

    return created, skipped


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tid(val) -> Optional[int]:
    try:
        return int(float(val)) if val is not None and str(val) != "nan" else None
    except (ValueError, TypeError):
        return None


def _canonical(
    ea_id: int,
    ea_tid: Optional[int],
    eb_id: int,
    eb_tid: Optional[int],
    gene_type_id: Optional[int],
) -> tuple[int, Optional[int], int, Optional[int]]:
    """Gene always entity_1; tie-break by smaller entity_id."""
    if gene_type_id is not None:
        ea_gene = ea_tid == gene_type_id
        eb_gene = eb_tid == gene_type_id
        if eb_gene and not ea_gene:
            return eb_id, eb_tid, ea_id, ea_tid
        if ea_gene and eb_gene and ea_id > eb_id:
            return eb_id, eb_tid, ea_id, ea_tid
    elif ea_id > eb_id:
        return eb_id, eb_tid, ea_id, ea_tid
    return ea_id, ea_tid, eb_id, eb_tid
