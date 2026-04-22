from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from igem_backend.modules.db.base import Base
from igem_backend.modules.db.types import PKBigIntOrInt


class EntityGroup(Base):
    """
    Conceptual category of a biological entity (e.g. Gene, Variant, Exposome).

    Used to enforce semantic boundaries in OxO integration and to drive
    relationship type validation (GxE, GxG, ExE, etc.).
    """

    __tablename__ = "entity_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(String(512), nullable=True)

    entities = relationship("Entity", back_populates="entity_group")


class Entity(Base):
    """
    A unique biological or exposome concept: gene, variant, chemical,
    disease, exposure, pathway, etc.

    Each entity belongs to one EntityGroup and accumulates aliases from
    multiple data sources via EntityAlias.
    """

    __tablename__ = "entities"

    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    group_id = Column(
        Integer,
        ForeignKey("entity_groups.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    entity_group = relationship("EntityGroup", back_populates="entities")

    has_conflict = Column(Boolean, nullable=True, default=None)
    is_active = Column(Boolean, nullable=True, default=True)

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

    entity_aliases = relationship("EntityAlias", back_populates="entity")

    relationships_as_1 = relationship(
        "EntityRelationship",
        foreign_keys="[EntityRelationship.entity_1_id]",
        back_populates="entity_1",
    )
    relationships_as_2 = relationship(
        "EntityRelationship",
        foreign_keys="[EntityRelationship.entity_2_id]",
        back_populates="entity_2",
    )

    primary_name = relationship(
        "EntityAlias",
        primaryjoin="and_(Entity.id==EntityAlias.entity_id, EntityAlias.is_primary==True)",
        viewonly=True,
        uselist=False,
    )


class EntityAlias(Base):
    """
    All known names, synonyms, and cross-reference codes for an Entity.

    Replaces the MVP WordTerm pattern: every text string that resolves to an
    entity (HGNC symbol, Entrez ID, Ensembl ID, common synonym, NLP-extracted
    mention) is stored here.

    NLP pipeline:  text span → alias_norm lookup → EntityAlias → Entity
    """

    __tablename__ = "entity_aliases"

    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entity = relationship("Entity", back_populates="entity_aliases")

    group_id = Column(
        Integer,
        ForeignKey("entity_groups.id", ondelete="SET NULL"),
        nullable=True,
    )
    entity_group = relationship("EntityGroup", passive_deletes=True)

    alias_value = Column(String(1000), nullable=False)

    # preferred | synonym | code
    alias_type = Column(String(16), nullable=False, default="synonym")

    # HGNC | NCBI | ENSEMBL | UNIPROT | MESH | OMIM | NLP | LITERATURE | ...
    xref_source = Column(String(32), nullable=True)

    is_primary = Column(Boolean, nullable=True, default=None)
    is_active = Column(Boolean, nullable=True, default=True)

    # Lowercased/stripped value for stable NLP matching
    alias_norm = Column(String(1000), nullable=True, index=True)

    locale = Column(String(8), nullable=True)

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

    __table_args__ = (
        UniqueConstraint(
            "entity_id",
            "alias_type",
            "xref_source",
            "alias_value",
            name="uq_entity_aliases_semantic",
        ),
    )


class EntityRelationshipType(Base):
    """
    Semantic type of a relationship between two entities.

    Covers structured types (is_a, part_of, encodes, interacts_with) and
    IGEM-specific OxO interaction types (GxG, GxE, ExE, OxO).
    """

    __tablename__ = "entity_relationship_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50), unique=True, nullable=False)
    description = Column(String(255), nullable=True)

    relationships = relationship("EntityRelationship", back_populates="relationship_type")


class EntityRelationship(Base):
    """
    A directed relationship between two entities.

    Extends the BF4 model with IGEM-specific discovery metadata:
    - discovery_method: how this relationship was found
      (structured = curated DB, regex = IGEM MVP approach,
       nlp = scispaCy extraction, pubmed = PubMed mining, manual)
    - confidence_score: float 0–1; null means curated (implicit 1.0)
    - evidence_count: number of text occurrences supporting this relationship
    - source_ref: PMID, DOI, or URL of the evidence source
    """

    __tablename__ = "entity_relationships"

    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    entity_1_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entity_1 = relationship(
        "Entity",
        foreign_keys=[entity_1_id],
        back_populates="relationships_as_1",
    )

    entity_1_group_id = Column(
        Integer,
        ForeignKey("entity_groups.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity_1_group = relationship(
        "EntityGroup", foreign_keys=[entity_1_group_id], passive_deletes=True
    )

    entity_2_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entity_2 = relationship(
        "Entity",
        foreign_keys=[entity_2_id],
        back_populates="relationships_as_2",
    )

    entity_2_group_id = Column(
        Integer,
        ForeignKey("entity_groups.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity_2_group = relationship(
        "EntityGroup", foreign_keys=[entity_2_group_id], passive_deletes=True
    )

    relationship_type_id = Column(
        Integer,
        ForeignKey("entity_relationship_types.id", ondelete="CASCADE"),
        nullable=False,
    )
    relationship_type = relationship(
        "EntityRelationshipType", back_populates="relationships"
    )

    # --- IGEM OxO discovery metadata ---
    discovery_method = Column(
        Enum(
            "structured",
            "regex",
            "nlp",
            "pubmed",
            "manual",
            name="discovery_method_enum",
            create_constraint=True,
            validate_strings=True,
        ),
        nullable=False,
        default="structured",
    )
    # null = curated (implicit confidence 1.0); float for NLP/PubMed discovered
    confidence_score = Column(Float, nullable=True)
    # number of text co-occurrences supporting this relationship
    evidence_count = Column(Integer, nullable=True)
    # PMID, DOI, or URL of primary evidence
    source_ref = Column(String(512), nullable=True)

    # Provenance
    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

    __table_args__ = (
        UniqueConstraint(
            "entity_1_id",
            "entity_2_id",
            "relationship_type_id",
            "data_source_id",
            name="uq_entity_relationships_pair_source",
        ),
    )
