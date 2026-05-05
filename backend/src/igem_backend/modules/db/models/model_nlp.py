"""
NLP entity resolution tables.

EntityMatch — observational: "entity X was mentioned in source record Y".

Distinct from EntityRelationship (relational fact). The relation_builder module
groups EntityMatch rows by source_record_id to produce EntityRelationship rows
with discovery_method="nlp" and relation_type="co_occurs_with".

Flow:
  entity_resolver → EntityMatch (per span)
  relation_builder → EntityRelationship (per co-occurring pair)
"""

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from igem_backend.modules.db.base import Base
from igem_backend.modules.db.types import PKBigIntOrInt


class EntityMatch(Base):
    """
    A single entity mention found by the NLP entity resolver.

    alias_id uses SET NULL (not CASCADE) so that alias refresh cycles
    do not erase match history — the span observation remains valid
    even if the alias row is re-created.
    """

    __tablename__ = "entity_matches"

    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    # --- Provenance of the source text ---
    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

    # Identifier of the source record (e.g. HMDB accession, CTD row key)
    source_record_id = Column(String(255), nullable=True, index=True)
    # Field within the source record that contained the text
    source_field = Column(String(100), nullable=True)
    # sha256[:16] of the full source text — for audit / reproducibility
    text_hash = Column(String(16), nullable=True)

    # --- The matched span ---
    matched_text = Column(String(500), nullable=False)
    span_start = Column(Integer, nullable=True)
    span_end = Column(Integer, nullable=True)
    # ~100-char window around the span for human review
    context = Column(String(1000), nullable=True)

    # --- Resolution ---
    # SET NULL: alias refresh preserves match history
    alias_id = Column(
        BigInteger,
        ForeignKey("entity_aliases.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    alias = relationship("EntityAlias", passive_deletes=True)

    # Denormalized for query performance (avoids join through alias)
    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entity = relationship("Entity", passive_deletes=True)

    # --- How it was found ---
    match_method = Column(
        Enum(
            "exact_aho",    # Aho-Corasick exact match on alias_norm
            "code_pattern", # regex for structured IDs (HGNC:, ENSG, HMDB...)
            "tsvector",     # PostgreSQL full-text (morphological)
            "fuzzy",        # rapidfuzz approximate match
            "embedding",    # SapBERT + pgvector cosine similarity
            "manual",       # human-entered
            name="match_method_enum",
            create_constraint=True,
        ),
        nullable=False,
    )
    # 0.0–1.0; exact unambiguous match = 1.0 / n_candidates
    confidence = Column(Float, nullable=False)

    # --- Human review loop ---
    review_status = Column(
        Enum(
            "auto",       # accepted automatically (confidence >= threshold)
            "confirmed",  # human confirmed correct
            "rejected",   # human marked wrong
            "pending",    # ambiguous — awaiting human review
            name="match_review_status_enum",
            create_constraint=True,
        ),
        nullable=False,
        default="auto",
    )

    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        # Grouping by source record (relation_builder's primary access pattern)
        Index("ix_em_package_record", "etl_package_id", "source_record_id"),
        # Filter by entity + method (dedup checks)
        Index("ix_em_entity_method", "entity_id", "match_method"),
        # Human review queue
        Index("ix_em_review_status", "review_status"),
    )
