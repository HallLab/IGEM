import enum

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship


from igem_backend.modules.db.base import Base
from igem_backend.modules.db.types import PKBigIntOrInt


class EntityDomain(str, enum.Enum):
    GENOMICS = "Genomics"
    EXPOSOME = "Exposome"
    KNOWLEDGE = "Knowledge"


class EntityType(Base):
    """
    Specific category of a biological or environmental entity (e.g. Gene,
    Variant, Chemical, Disease, Exposome, Pathway).

    Each EntityType belongs to one EntityDomain (Genomics, Exposome, Knowledge),
    stored as an Enum column — no separate table, no join required.
    The domain drives OxO relationship classification (GxG, GxE, ExE, GxK).
    """

    __tablename__ = "entity_types"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    domain = Column(
        Enum(
            "Genomics",
            "Exposome",
            "Knowledge",
            name="entity_domain_enum",
            create_constraint=True,
            validate_strings=True,
        ),
        nullable=False,
    )
    description = Column(String(512), nullable=True)

    entities = relationship("Entity", back_populates="entity_type")


class Entity(Base):
    """
    A unique biological or exposome concept: gene, variant, chemical,
    disease, exposure, pathway, etc.

    Each entity belongs to one EntityType and accumulates aliases from
    multiple data sources via EntityAlias.
    """

    __tablename__ = "entities"

    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    type_id = Column(
        Integer,
        ForeignKey("entity_types.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    entity_type = relationship("EntityType", back_populates="entities")

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
        primaryjoin=(
            "and_(Entity.id==EntityAlias.entity_id,"
            " EntityAlias.is_primary==True)"
        ),
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

    type_id = Column(
        Integer,
        ForeignKey("entity_types.id", ondelete="SET NULL"),
        nullable=True,
    )
    entity_type = relationship("EntityType", passive_deletes=True)

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

    relationships = relationship(
        "EntityRelationship", back_populates="relationship_type"
    )


class EntityRelationship(Base):
    """
    A directed relationship between two entities.

    Extends the BF4 model with IGEM-specific discovery metadata:
    - discovery_method: how this relationship was found
      (structured = curated DB, regex = IGEM MVP approach,
       nlp = scispaCy extraction, pubmed = PubMed mining, manual)
    - confidence_score: float 0-1; null means curated (implicit 1.0)
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

    entity_1_type_id = Column(
        Integer,
        ForeignKey("entity_types.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity_1_type = relationship(
        "EntityType", foreign_keys=[entity_1_type_id], passive_deletes=True
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

    entity_2_type_id = Column(
        Integer,
        ForeignKey("entity_types.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity_2_type = relationship(
        "EntityType", foreign_keys=[entity_2_type_id], passive_deletes=True
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
        # Single-column lookups (used with UNION for bidirectional queries)
        Index("ix_er_entity1_id", "entity_1_id"),
        Index("ix_er_entity2_id", "entity_2_id"),
        # OxO domain-pair queries: GxE, GxG, ExE, GxK
        Index("ix_er_type_pair", "entity_1_type_id", "entity_2_type_id"),
        # Filter by discovery source
        Index("ix_er_discovery_method", "discovery_method"),
        Index("ix_er_data_source_id", "data_source_id"),
    )


class EntityLocation(Base):
    """
    Genomic coordinates for entities with a chromosomal position.

    Used by Genes, Variants, and Epigenomic marks. One entity may have
    locations on multiple assemblies (e.g. GRCh38 and GRCh37).

    Chromosome encoding: 1-22 autosomes | 23=X | 24=Y | 25=MT
    """

    __tablename__ = "entity_locations"

    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entity = relationship("Entity")

    entity_type_id = Column(
        Integer,
        ForeignKey("entity_types.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    assembly_id = Column(
        Integer,
        ForeignKey("genome_assemblies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    chromosome = Column(Integer, nullable=False, index=True)
    start_pos = Column(BigInteger, nullable=False)
    end_pos = Column(BigInteger, nullable=False)

    strand = Column(
        Enum("+", "-", name="strand_enum", create_constraint=True),
        nullable=True,
    )

    # Cytogenetic band (e.g. 12p13.31)
    region_label = Column(String(50), nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "entity_id", "assembly_id", name="uq_entity_location_assembly"
        ),
    )
