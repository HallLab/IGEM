"""
Variants domain models.

This module mixes two model styles:

- **Declarative classes** (subclasses of ``Base``) for dimension
  tables, lookups, and small relational tables. These behave like
  any other ORM model.

- **Imperative ``Table()`` factories** (``map_*`` functions) for the
  five chromosome-partitioned tables. Imperative is required here
  because PostgreSQL ``PARTITION BY LIST`` clauses cannot be
  expressed in SQLAlchemy 2.x declarative metadata; the dialect
  decides at runtime whether to register a plain SQLite table or a
  PG composite-PK table whose actual DDL ships in
  :mod:`igem_backend.modules.db.core_ddl`.

The five partitioned tables (PostgreSQL only):

- ``variant_masters``               — one row per ALT allele
- ``variant_molecular_effects``     — VEP-style per-transcript consequences
- ``variant_effect_predictions``    — predictor scores (CADD, SpliceAI, …)
- ``variant_regulatory_elements``   — ENCODE / Ensembl reg elements
- ``variant_gene_regulatory_evidence`` — eQTL / sQTL / pQTL evidence

On SQLite (dev/test only), each is a plain non-partitioned table
with the same column shape but a simple ``variant_id INTEGER PK``.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    Identity,
    Integer,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from igem_backend.modules.db.base import Base
from igem_backend.modules.db.types import PKBigIntOrInt


# ---------------------------------------------------------------------------
# Dimension tables (declarative — small, regular ORM)
# ---------------------------------------------------------------------------

class VariantConsequenceGroup(Base):
    """High-level grouping of variant consequences (e.g. coding, regulatory)."""

    __tablename__ = "variant_consequence_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), unique=True, nullable=False)


class VariantConsequenceCategory(Base):
    """Mid-level category for variant consequences."""

    __tablename__ = "variant_consequence_categories"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), unique=True, nullable=False)


class VariantConsequence(Base):
    """
    Atomic SO variant consequence with severity ranking.
    (e.g. stop_gained, missense_variant, synonymous_variant)
    """

    __tablename__ = "variant_consequences"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), unique=True, nullable=False)
    severity_rank = Column(Integer, nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)

    consequence_group_id = Column(
        Integer,
        ForeignKey("variant_consequence_groups.id", ondelete="SET NULL"),
        nullable=True,
    )
    consequence_category_id = Column(
        Integer,
        ForeignKey("variant_consequence_categories.id", ondelete="SET NULL"),
        nullable=True,
    )


class VariantImpact(Base):
    """VEP impact classification (HIGH, MODERATE, LOW, MODIFIER)."""

    __tablename__ = "variant_impacts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), unique=True, nullable=False)
    severity_rank = Column(Integer, nullable=False)


class VariantBiotype(Base):
    """Transcript biotype classification (e.g. protein_coding, lncRNA, miRNA)."""

    __tablename__ = "variant_biotypes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), unique=True, nullable=False)
    description = Column(Text, nullable=True)


# ---------------------------------------------------------------------------
# Partitioned tables (imperative — dialect-specific shape)
# ---------------------------------------------------------------------------
#
# These functions are invoked from
# ``igem_backend.utils.db_loader.register_imperative_tables`` and
# register Table objects into ``Base.metadata`` so ORM/Core queries
# work transparently. The actual DDL for the partitioned parents on
# PostgreSQL lives in ``core_ddl.py`` and is executed by migrations
# / the create-DB path; this metadata is for query-time only.

def map_variant_masters(engine, metadata) -> Table:
    """
    One row per ALT allele in a given assembly.

    PostgreSQL: composite PK ``(chromosome, variant_id)`` with
    ``variant_id`` as a BIGINT identity column, parent declared
    ``PARTITION BY LIST (chromosome)`` via raw DDL.

    SQLite: plain table with ``variant_id`` as the autoincrement PK
    (no partitioning).
    """
    is_sqlite = engine.dialect.name == "sqlite"

    if "variant_masters" in metadata.tables:
        return metadata.tables["variant_masters"]

    common_cols = [
        # IGEM-specific (no FK constraints — keep partition-friendly)
        Column("entity_id", BigInteger, nullable=True),
        Column("assembly_id", Integer, nullable=True),
        # Position
        Column("position_start", BigInteger, nullable=False),
        Column("position_end", BigInteger, nullable=False),
        Column("reference_allele", String(64), nullable=False),
        Column("alternate_allele", String(256), nullable=False),
        # External IDs
        Column("rsid", String(32), nullable=True),
        # Classification
        Column("variant_type", String(20), nullable=True),
        Column("allele_type", String(20), nullable=True),
        # Frequencies
        Column("ac", BigInteger, nullable=True),
        Column("an", BigInteger, nullable=True),
        Column("af", Float, nullable=True),
        Column("grpmax", String(32), nullable=True),
        Column("grpmax_af", Float, nullable=True),
        # Pathogenicity / predictor summaries
        Column("cadd_raw_score", Float, nullable=True),
        Column("cadd_phred", Float, nullable=True),
        Column("revel_max", Float, nullable=True),
        Column("spliceai_ds_max", Float, nullable=True),
        Column("pangolin_largest_ds", Float, nullable=True),
        Column("sift_max", Float, nullable=True),
        Column("polyphen_max", Float, nullable=True),
        # Provenance (no FK)
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
    ]

    constraints = (
        UniqueConstraint(
            "chromosome",
            "position_start",
            "position_end",
            "reference_allele",
            "alternate_allele",
            name="uq_variant_masters_natkey",
        ),
        CheckConstraint(
            "position_end >= position_start",
            name="ck_variant_masters_pos",
        ),
        CheckConstraint(
            "af IS NULL OR (af >= 0.0 AND af <= 1.0)",
            name="ck_variant_masters_af",
        ),
        CheckConstraint(
            "(ac IS NULL AND an IS NULL) "
            "OR (ac IS NOT NULL AND an IS NOT NULL "
            "AND ac >= 0 AND an >= 0 AND ac <= an)",
            name="ck_variant_masters_ac_an",
        ),
    )

    if is_sqlite:
        return Table(
            "variant_masters",
            metadata,
            Column(
                "variant_id", Integer, primary_key=True, autoincrement=True,
            ),
            Column("chromosome", Integer, nullable=False),
            *common_cols,
            *constraints,
        )

    return Table(
        "variant_masters",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column(
            "variant_id",
            BigInteger,
            nullable=False,
            server_default=Identity(always=False),
        ),
        *common_cols,
        PrimaryKeyConstraint(
            "chromosome", "variant_id", name="pk_variant_masters",
        ),
        *constraints,
    )


def map_variant_molecular_effects(engine, metadata) -> Table:
    """
    One row per (variant allele × transcript × atomic consequence).

    Connected to ``variant_masters`` by the logical key
    ``(chromosome, variant_id)``; no physical FK.
    """
    is_sqlite = engine.dialect.name == "sqlite"

    if "variant_molecular_effects" in metadata.tables:
        return metadata.tables["variant_molecular_effects"]

    common_cols = [
        Column("variant_key", String(256), nullable=False),
        # Raw VEP identity / context
        Column("gene_id", String(32), nullable=True),
        Column("gene_symbol", String(64), nullable=True),
        Column("transcript_id", String(32), nullable=False),
        Column("feature_type", String(32), nullable=True),
        # Raw consequence
        Column("consequence_raw", String(255), nullable=True),
        # Normalized consequence layers (no FK — dimension lookup)
        Column("consequence_id", Integer, nullable=False),
        Column("impact_id", Integer, nullable=True),
        Column("biotype_id", Integer, nullable=True),
        Column("consequence_group_id", Integer, nullable=True),
        Column("consequence_category_id", Integer, nullable=True),
        # Derived severity helpers
        Column("consequence_rank", Integer, nullable=True),
        Column("impact_rank", Integer, nullable=True),
        Column("is_most_severe_for_annotation", Boolean, nullable=True),
        Column("is_most_severe_for_variant", Boolean, nullable=True),
        Column("most_severe_consequence_per_annotation_id", Integer, nullable=True),
        Column("most_severe_consequence_per_variant_id", Integer, nullable=True),
        # VEP context
        Column("canonical", Boolean, nullable=True),
        Column("mane_select", Boolean, nullable=True),
        Column("mane_plus_clinical", Boolean, nullable=True),
        # HGVS / protein context
        Column("hgvsc", String(128), nullable=True),
        Column("hgvsp", String(128), nullable=True),
        Column("cdna_position", String(32), nullable=True),
        Column("cds_position", String(32), nullable=True),
        Column("protein_position", String(32), nullable=True),
        Column("amino_acids", String(32), nullable=True),
        Column("codons", String(64), nullable=True),
        Column("ensp", String(32), nullable=True),
        # LoF / LOFTEE
        Column("lof_flag", Boolean, nullable=True),
        Column("lof_confidence", String(8), nullable=True),
        Column("lof_filter", String(128), nullable=True),
        Column("lof_flags", String(256), nullable=True),
        Column("lof_info", Text, nullable=True),
        # Provenance
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
    ]

    constraints = (
        CheckConstraint(
            "lof_confidence IS NULL "
            "OR lof_confidence IN ('HC', 'LC', 'Filtered', 'NA')",
            name="ck_vme_lof_conf",
        ),
    )

    variant_id_type = Integer if is_sqlite else BigInteger
    return Table(
        "variant_molecular_effects",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("variant_id", variant_id_type, nullable=False),
        *common_cols,
        PrimaryKeyConstraint(
            "chromosome",
            "variant_id",
            "transcript_id",
            "consequence_id",
            name="pk_variant_molecular_effects",
        ),
        *constraints,
    )


def map_variant_effect_predictions(engine, metadata) -> Table:
    """One row per predictor result per variant (optionally per transcript)."""
    is_sqlite = engine.dialect.name == "sqlite"

    if "variant_effect_predictions" in metadata.tables:
        return metadata.tables["variant_effect_predictions"]

    variant_id_type = Integer if is_sqlite else BigInteger
    return Table(
        "variant_effect_predictions",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("variant_id", variant_id_type, nullable=False),
        Column("predictor_key", String(128), nullable=False),
        Column("transcript_id", String(32), nullable=True),
        Column("predictor_name", String(64), nullable=False),
        Column("predictor_version", String(32), nullable=True),
        Column("score", Float, nullable=True),
        Column("classification", String(64), nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
        PrimaryKeyConstraint(
            "chromosome",
            "variant_id",
            "predictor_key",
            name="pk_variant_effect_predictions",
        ),
    )


def map_variant_regulatory_elements(engine, metadata) -> Table:
    """One row per (variant × regulatory element × bio_context)."""
    is_sqlite = engine.dialect.name == "sqlite"

    if "variant_regulatory_elements" in metadata.tables:
        return metadata.tables["variant_regulatory_elements"]

    variant_id_type = Integer if is_sqlite else BigInteger
    return Table(
        "variant_regulatory_elements",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("variant_id", variant_id_type, nullable=False),
        Column("reg_element_key", String(192), nullable=False),
        Column("regulatory_element_id", String(64), nullable=False),
        Column("element_type", String(32), nullable=True),
        Column("bio_context", String(128), nullable=True),
        Column("score", Float, nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
        PrimaryKeyConstraint(
            "chromosome",
            "variant_id",
            "reg_element_key",
            name="pk_variant_regulatory_elements",
        ),
    )


def map_variant_gene_regulatory_evidence(engine, metadata) -> Table:
    """One row per (variant × gene × QTL type × bio_context)."""
    is_sqlite = engine.dialect.name == "sqlite"

    if "variant_gene_regulatory_evidence" in metadata.tables:
        return metadata.tables["variant_gene_regulatory_evidence"]

    variant_id_type = Integer if is_sqlite else BigInteger
    return Table(
        "variant_gene_regulatory_evidence",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("variant_id", variant_id_type, nullable=False),
        Column("evidence_key", String(256), nullable=False),
        Column("gene_id", String(32), nullable=False),
        Column("bio_context", String(128), nullable=True),
        Column("qtl_type", String(16), nullable=False),
        Column("beta", Float, nullable=True),
        Column("se", Float, nullable=True),
        Column("p_value", Float, nullable=True),
        Column("n", Integer, nullable=True),
        Column("effect_allele", String(64), nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
        PrimaryKeyConstraint(
            "chromosome",
            "variant_id",
            "evidence_key",
            name="pk_variant_gene_regulatory_evidence",
        ),
        CheckConstraint(
            "p_value IS NULL OR (p_value >= 0.0 AND p_value <= 1.0)",
            name="ck_vgre_p",
        ),
    )


# ---------------------------------------------------------------------------
# Non-partitioned auxiliary tables
# ---------------------------------------------------------------------------

class VariantSNPMerge(Base):
    """
    dbSNP merge history: maps obsolete rsIDs to their canonical replacement.
    Used to resolve stale rsIDs from older studies.
    """

    __tablename__ = "variant_snp_merges"

    rs_obsolete_id = Column(BigInteger, primary_key=True)
    rs_canonical_id = Column(BigInteger, nullable=False, index=True)

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="SET NULL"),
        nullable=True,
    )
    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="SET NULL"),
        nullable=True,
    )
