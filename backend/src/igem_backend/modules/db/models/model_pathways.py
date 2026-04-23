from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import relationship

from igem_backend.modules.db.base import Base


class PathwayMaster(Base):
    """
    Biological pathway linked to a canonical Entity (type=Pathways).

    Covers pathways from Reactome (R-HSA-xxxxx) and KEGG (map00010).
    The pathway_id is the source-native identifier; the canonical Entity
    carries display names and synonyms via EntityAlias.
    """

    __tablename__ = "pathway_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    entity = relationship("Entity")

    # Source-native identifier (e.g. R-HSA-109581, map00010)
    pathway_id = Column(String(100), nullable=False, unique=True, index=True)

    description = Column(String(512), nullable=True)

    # Source system (Reactome, KEGG, BioCarta, ...)
    source_db = Column(String(50), nullable=True)

    # Species (e.g. Homo sapiens)
    organism = Column(String(100), nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )
