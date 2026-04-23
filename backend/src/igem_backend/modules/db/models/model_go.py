from sqlalchemy import (
    BigInteger,
    Column,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from igem_backend.modules.db.base import Base


class GOMaster(Base):
    """
    Gene Ontology term linked to a canonical Entity (type=Gene Ontology).

    Namespace values:
    - MF: Molecular Function
    - BP: Biological Process
    - CC: Cellular Component
    """

    __tablename__ = "go_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    entity = relationship("Entity")

    go_id = Column(String(15), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)

    namespace = Column(
        Enum(
            "MF", "BP", "CC",
            name="go_namespace_enum",
            create_constraint=True,
            validate_strings=True,
        ),
        nullable=False,
    )

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    parent_relations = relationship(
        "GORelation",
        foreign_keys="[GORelation.child_id]",
        back_populates="child",
    )
    child_relations = relationship(
        "GORelation",
        foreign_keys="[GORelation.parent_id]",
        back_populates="parent",
    )


class GORelation(Base):
    """
    Parent-child relationship in the GO DAG.

    relation_type follows GO semantics: is_a, part_of, regulates,
    positively_regulates, negatively_regulates.
    """

    __tablename__ = "go_relations"

    id = Column(Integer, primary_key=True, autoincrement=True)

    parent_id = Column(
        Integer,
        ForeignKey("go_masters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    parent = relationship(
        "GOMaster", foreign_keys=[parent_id], back_populates="child_relations"
    )

    child_id = Column(
        Integer,
        ForeignKey("go_masters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    child = relationship(
        "GOMaster", foreign_keys=[child_id], back_populates="parent_relations"
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
            name="uq_go_relation",
        ),
    )
