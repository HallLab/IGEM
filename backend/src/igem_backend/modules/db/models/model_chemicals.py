from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import relationship

from igem_backend.modules.db.base import Base


class ChemicalMaster(Base):
    """
    Chemical substance identity linked to a canonical Entity (type=Chemicals).

    Represents WHAT a substance IS (molecular identity), not how it is
    encountered as an exposure. Exposure context lives in ExposureMaster.

    Primary identifier: CTD MeSH ID (ctd_id) when available, else CAS number.

    Sources: CTD, ChEBI, PubChem.
    """

    __tablename__ = "chemical_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    entity = relationship("Entity")

    # External identifiers
    ctd_id = Column(String(20), nullable=True, index=True)   # MeSH ID in CTD
    cas_number = Column(String(20), nullable=True, index=True)
    pubchem_cid = Column(String(20), nullable=True, index=True)
    chebi_id = Column(String(20), nullable=True, index=True)

    # Structure
    inchi_key = Column(String(27), nullable=True, index=True)
    smiles = Column(String(4000), nullable=True)
    formula = Column(String(100), nullable=True)

    # Properties
    molecular_weight = Column(Float, nullable=True)
    chemical_class = Column(String(100), nullable=True)

    # Classification flags
    is_drug = Column(Boolean, nullable=True)
    is_environmental = Column(Boolean, nullable=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )
