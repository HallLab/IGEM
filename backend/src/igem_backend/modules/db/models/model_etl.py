from sqlalchemy import JSON, Boolean, Column, DateTime, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from igem_backend.modules.db.base import Base


def _etl_status_enum(name: str):
    return Enum(
        "pending",
        "running",
        "completed",
        "failed",
        "not-applicable",
        "up-to-date",
        name=name,
        create_constraint=True,
        validate_strings=True,
    )


class ETLSourceSystem(Base):
    """A data source provider, e.g. NCBI, UniProt, PubMed."""

    __tablename__ = "etl_source_systems"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(String(1024), nullable=True)
    homepage = Column(String(512), nullable=True)
    active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    data_sources = relationship("ETLDataSource", back_populates="source_system")


class ETLDataSource(Base):
    """
    A specific dataset ingested by a DTP.

    Tracks the source URL, format, DTP script version, and active status.
    """

    __tablename__ = "etl_data_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    dtp_version = Column(String(50), nullable=True)
    schema_version = Column(String(50), nullable=True)

    source_system_id = Column(
        Integer, ForeignKey("etl_source_systems.id", ondelete="CASCADE"), nullable=False
    )
    source_system = relationship("ETLSourceSystem", back_populates="data_sources")

    data_type = Column(String(50), nullable=False)
    source_url = Column(String(512), nullable=True)
    format = Column(String(20), nullable=False)
    dtp_script = Column(String(255), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    etl_packages = relationship(
        "ETLPackage", back_populates="data_source", cascade="all, delete-orphan"
    )


class ETLPackage(Base):
    """
    A single ETL execution run for a data source.

    Tracks extract / transform / load phases independently with timing,
    row counts, file hashes, and status per phase.
    """

    __tablename__ = "etl_packages"

    id = Column(Integer, primary_key=True, autoincrement=True)

    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id", ondelete="CASCADE"), nullable=False
    )
    data_source = relationship("ETLDataSource", back_populates="etl_packages")

    status = Column(_etl_status_enum("etl_status_enum"), nullable=False, default="pending")
    operation_type = Column(String(50), nullable=True, default="insert")
    version_tag = Column(String(50), nullable=True)
    note = Column(String(255), nullable=True)
    active = Column(Boolean, default=True)

    extract_start = Column(DateTime, nullable=True)
    extract_end = Column(DateTime, nullable=True)
    extract_rows = Column(Integer, nullable=True)
    extract_hash = Column(String(128), nullable=True)
    extract_status = Column(_etl_status_enum("etl_extract_status_enum"), nullable=True, default="pending")

    transform_start = Column(DateTime, nullable=True)
    transform_end = Column(DateTime, nullable=True)
    transform_rows = Column(Integer, nullable=True)
    transform_hash = Column(String(128), nullable=True)
    transform_status = Column(_etl_status_enum("etl_transform_status_enum"), nullable=True, default="pending")

    load_start = Column(DateTime, nullable=True)
    load_end = Column(DateTime, nullable=True)
    load_rows = Column(Integer, nullable=True)
    load_hash = Column(String(128), nullable=True)
    load_status = Column(_etl_status_enum("etl_load_status_enum"), nullable=True, default="pending")

    stats = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
