from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from igem_backend.modules.db.base import Base
from igem_backend.modules.db.types import PKBigIntOrInt


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


class VariantMasters(Base):
    """
    One row per ALT allele (chr, start, ref, alt) in GRCh38.

    Linked to a canonical Entity (type=Variants) via entity_id.
    Population frequencies and pathogenicity scores are stored here;
    transcript-level effects will be added in Onda 2 (VariantMolecularEffects).
    """

    __tablename__ = "variant_masters"

    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity = relationship("Entity")

    chromosome = Column(Integer, nullable=False, index=True)
    position_start = Column(BigInteger, nullable=False)
    position_end = Column(BigInteger, nullable=False)
    reference_allele = Column(String(64), nullable=False)
    alternate_allele = Column(String(256), nullable=False)

    rsid = Column(String(32), nullable=True, index=True)
    variant_type = Column(String(20), nullable=True)
    allele_type = Column(String(20), nullable=True)

    # Population frequency
    af = Column(Float, nullable=True)

    # Pathogenicity scores
    cadd_phred = Column(Float, nullable=True)
    revel_max = Column(Float, nullable=True)
    spliceai_ds_max = Column(Float, nullable=True)

    assembly_id = Column(
        Integer,
        ForeignKey("genome_assemblies.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "chromosome",
            "position_start",
            "position_end",
            "reference_allele",
            "alternate_allele",
            name="uq_variant_masters_allele",
        ),
    )


class VariantSNPMerge(Base):
    """
    dbSNP merge history: maps obsolete rsIDs to their canonical replacement.
    Used to resolve stale rsIDs from older studies.
    """

    __tablename__ = "variant_snp_merges"

    rs_obsolete_id = Column(BigInteger, primary_key=True)
    rs_canonical_id = Column(BigInteger, nullable=False, index=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )
