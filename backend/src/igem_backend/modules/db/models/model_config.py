from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.sql import func

from igem_backend.modules.db.base import Base


class SystemConfig(Base):
    """Global configuration key-value pairs."""

    __tablename__ = "system_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(50), unique=True, nullable=False)
    value = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False, default="string")
    description = Column(String(255), nullable=True)
    editable = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )


class IgemMetadata(Base):
    """Tracks schema and ETL versioning of this IGEM instance."""

    __tablename__ = "igem_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    schema_version = Column(String(50), nullable=False)
    schema_revision = Column(String(64), nullable=True)
    etl_version = Column(String(50), nullable=True)
    description = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )


class GenomeAssembly(Base):
    """Reference genome assemblies used for variant and gene location mapping."""

    __tablename__ = "genome_assemblies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    accession = Column(String(50), unique=True, nullable=False)
    assembly_name = Column(String(50), nullable=False)
    chromosome = Column(String(10), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
