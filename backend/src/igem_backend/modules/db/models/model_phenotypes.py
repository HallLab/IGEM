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


class PhenotypeMaster(Base):
    """
    Human Phenotype Ontology (HPO) term linked to a canonical Entity
    (type=Phenotypes).

    Primary identifier: HP:xxxxxxx
    Source: https://purl.obolibrary.org/obo/hp.obo
    """

    __tablename__ = "phenotype_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    entity = relationship("Entity")

    hp_id = Column(String(15), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    definition = Column(Text, nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    parent_relations = relationship(
        "PhenotypeRelation",
        foreign_keys="[PhenotypeRelation.child_id]",
        back_populates="child",
    )
    child_relations = relationship(
        "PhenotypeRelation",
        foreign_keys="[PhenotypeRelation.parent_id]",
        back_populates="parent",
    )


class PhenotypeRelation(Base):
    """
    Parent-child relationship in the HPO DAG.

    relation_type: is_a, part_of
    Convention: child is_a/part_of parent (same direction as GO DAG).
    """

    __tablename__ = "phenotype_relations"

    id = Column(Integer, primary_key=True, autoincrement=True)

    parent_id = Column(
        Integer,
        ForeignKey("phenotype_masters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    parent = relationship(
        "PhenotypeMaster",
        foreign_keys=[parent_id],
        back_populates="child_relations",
    )

    child_id = Column(
        Integer,
        ForeignKey("phenotype_masters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    child = relationship(
        "PhenotypeMaster",
        foreign_keys=[child_id],
        back_populates="parent_relations",
    )

    relation_type = Column(String(50), nullable=False)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "parent_id", "child_id", "relation_type",
            name="uq_phenotype_relation",
        ),
    )
