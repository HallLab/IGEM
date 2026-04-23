from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import relationship

from igem_backend.modules.db.base import Base


class GeneLocusGroup(Base):
    """HGNC locus group (e.g. protein-coding gene, RNA gene, pseudogene)."""

    __tablename__ = "gene_locus_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(100), nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )


class GeneLocusType(Base):
    """HGNC locus type (e.g. miRNA, snRNA, T cell receptor gene)."""

    __tablename__ = "gene_locus_types"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(100), nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )


class GeneGroup(Base):
    """Functional or curated grouping of genes (e.g. kinase family, HOX cluster)."""

    __tablename__ = "gene_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(String(512), nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    members = relationship("GeneGroupMembership", back_populates="gene_group")


class GeneMaster(Base):
    """
    Gene-specific attributes linked to a canonical Entity (type=Genes).

    One row per approved HGNC gene symbol. Stores HGNC metadata that is
    not covered by EntityAlias (approval status, locus classification).
    Cross-references (HGNC ID, Ensembl, Entrez) live in EntityAlias.
    """

    __tablename__ = "gene_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_id = Column(
        Integer,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    entity = relationship("Entity")

    symbol = Column(String(64), nullable=True, index=True)
    hgnc_status = Column(String(50), nullable=True)
    chromosome = Column(Integer, nullable=True, index=True)

    locus_group_id = Column(
        Integer,
        ForeignKey("gene_locus_groups.id", ondelete="SET NULL"),
        nullable=True,
    )
    locus_group = relationship("GeneLocusGroup")

    locus_type_id = Column(
        Integer,
        ForeignKey("gene_locus_types.id", ondelete="SET NULL"),
        nullable=True,
    )
    locus_type = relationship("GeneLocusType")

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    group_memberships = relationship("GeneGroupMembership", back_populates="gene")


class GeneGroupMembership(Base):
    """M:N junction between GeneMaster and GeneGroup."""

    __tablename__ = "gene_group_memberships"

    gene_id = Column(
        Integer,
        ForeignKey("gene_masters.id", ondelete="CASCADE"),
        primary_key=True,
    )
    group_id = Column(
        Integer,
        ForeignKey("gene_groups.id", ondelete="CASCADE"),
        primary_key=True,
    )

    gene = relationship("GeneMaster", back_populates="group_memberships")
    gene_group = relationship("GeneGroup", back_populates="members")

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )
