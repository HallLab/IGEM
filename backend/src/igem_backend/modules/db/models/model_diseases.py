from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from igem_backend.modules.db.base import Base


class DiseaseGroup(Base):
    """Thematic grouping of diseases (e.g. cancer, cardiovascular, rare disease)."""

    __tablename__ = "disease_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(String(255), nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    members = relationship("DiseaseGroupMembership", back_populates="disease_group")


class DiseaseMaster(Base):
    """
    Disease-specific attributes linked to a canonical Entity (type=Diseases).

    Aggregates cross-references from multiple disease ontologies.
    Primary identifier (disease_id) is the most authoritative available:
    MONDO > OMIM > MESH > ICD10.
    """

    __tablename__ = "disease_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    entity = relationship("Entity")

    # Primary external identifier (most authoritative available)
    disease_id = Column(String(50), nullable=False, unique=True, index=True)

    label = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)

    # Cross-references
    icd10 = Column(String(20), nullable=True, index=True)
    mondo_id = Column(String(20), nullable=True, index=True)
    omim_id = Column(String(20), nullable=True, index=True)
    mesh_id = Column(String(20), nullable=True, index=True)
    orphanet_id = Column(String(20), nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    group_memberships = relationship(
        "DiseaseGroupMembership", back_populates="disease"
    )


class DiseaseGroupMembership(Base):
    """M:N junction between DiseaseMaster and DiseaseGroup."""

    __tablename__ = "disease_group_memberships"

    id = Column(Integer, primary_key=True, autoincrement=True)

    disease_id = Column(
        Integer,
        ForeignKey("disease_masters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    group_id = Column(
        Integer,
        ForeignKey("disease_groups.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    disease = relationship("DiseaseMaster", back_populates="group_memberships")
    disease_group = relationship("DiseaseGroup", back_populates="members")

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "disease_id", "group_id", name="uq_disease_group_membership"
        ),
    )
