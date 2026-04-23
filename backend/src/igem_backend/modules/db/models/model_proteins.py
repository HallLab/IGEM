from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from igem_backend.modules.db.base import Base


class ProteinPfam(Base):
    """Pfam protein family/domain definitions."""

    __tablename__ = "protein_pfams"

    id = Column(Integer, primary_key=True)
    pfam_acc = Column(String(50), unique=True, nullable=False, index=True)
    pfam_id = Column(String(50), nullable=True, index=True)
    description = Column(String(255), nullable=True)
    long_description = Column(String(255), nullable=True)
    type = Column(String(50), nullable=True)  # Domain, Family, Repeat, Motif
    clan_acc = Column(String(50), nullable=True)
    clan_name = Column(String(50), nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    protein_links = relationship("ProteinPfamLink", back_populates="pfam")


class ProteinMaster(Base):
    """
    Canonical protein record (UniProt entry or equivalent).

    A protein may have multiple Entity rows (isoforms), linked via
    ProteinEntity. ProteinMaster holds shared attributes; isoform-specific
    data is on ProteinEntity.
    """

    __tablename__ = "protein_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # UniProt accession (primary identifier)
    protein_id = Column(String(20), unique=True, nullable=False, index=True)

    function = Column(String(512), nullable=True)
    location = Column(String(255), nullable=True)
    tissue_expression = Column(String(255), nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    entities = relationship("ProteinEntity", back_populates="protein_master")
    pfam_links = relationship("ProteinPfamLink", back_populates="protein")


class ProteinEntity(Base):
    """
    Links Entity (type=Proteins) to ProteinMaster, supporting isoforms.

    One ProteinMaster may map to multiple Entity rows (canonical + isoforms).
    """

    __tablename__ = "protein_entities"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entity = relationship("Entity")

    protein_id = Column(
        Integer,
        ForeignKey("protein_masters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    protein_master = relationship("ProteinMaster", back_populates="entities")

    is_isoform = Column(Boolean, nullable=False, default=False)
    isoform_accession = Column(String(20), nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "entity_id", "protein_id", name="uq_protein_entity_link"
        ),
    )


class ProteinPfamLink(Base):
    """M:N junction between ProteinMaster and ProteinPfam."""

    __tablename__ = "protein_pfam_links"

    protein_id = Column(
        Integer,
        ForeignKey("protein_masters.id", ondelete="CASCADE"),
        primary_key=True,
    )
    pfam_pk_id = Column(
        Integer,
        ForeignKey("protein_pfams.id", ondelete="CASCADE"),
        primary_key=True,
    )

    protein = relationship("ProteinMaster", back_populates="pfam_links")
    pfam = relationship("ProteinPfam", back_populates="protein_links")

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )
