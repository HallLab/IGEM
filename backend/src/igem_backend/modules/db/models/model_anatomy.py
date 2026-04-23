from sqlalchemy import (
    BigInteger,
    Column,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from igem_backend.modules.db.base import Base


class AnatomyMaster(Base):
    """
    Anatomical entity linked to a canonical Entity (type=Anatomy).

    Primary identifier: UBERON:xxxxxxx
    Source: UBERON basic ontology (uberon/basic.obo)

    anatomy_level classification:
      system    — body systems (nervous system, cardiovascular system)
                  source: name contains 'system' or ' apparatus'
      organ     — discrete organs (liver, heart, kidney)
                  source: organ_slim / major_organ subset tags
      tissue    — tissue types (parenchyma, epithelium, mucosa)
                  source: name-based heuristic
      region    — anatomical regions and areas (cerebral cortex, hippocampus)
                  source: default for unclassified terms
      structure — cellular/subcellular structures (synapse, axon terminal)
                  source: name-based heuristic

    NULL anatomy_level means the classification heuristic was inconclusive.
    """

    __tablename__ = "anatomy_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    entity = relationship("Entity")

    uberon_id = Column(String(20), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    definition = Column(Text, nullable=True)

    anatomy_level = Column(
        Enum(
            "system", "organ", "tissue", "region", "structure",
            name="anatomy_level_enum",
            create_constraint=True,
            validate_strings=True,
        ),
        nullable=True,
        index=True,
    )

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="SET NULL"), nullable=True
    )
    etl_package_id = Column(
        Integer, ForeignKey("etl_packages.id", ondelete="SET NULL"), nullable=True
    )
