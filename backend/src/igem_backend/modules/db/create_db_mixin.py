from __future__ import annotations

import json
import os
from typing import Dict, List, Optional

from igem_backend.modules.db.base import Base
from igem_backend.utils.db_loader import bootstrap_models
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

SEED_UNIQUE_KEYS: Dict[str, List[str]] = {
    "SystemConfig": ["key"],
    "IgemMetadata": ["schema_version"],
    "ETLSourceSystem": ["name"],
    "ETLDataSource": ["name"],
    "EntityType": ["name"],
    "EntityRelationshipType": ["id"],
    "GenomeAssembly": ["accession"],
}


class CreateDBMixin:
    """
    DB bootstrap mixin: creates tables and seeds initial data.

    Supports SQLite (dev) and PostgreSQL (production).
    """

    def ensure_postgres_database(self, db_uri: str) -> bool:
        url = make_url(db_uri)
        if not url.database:
            raise ValueError("db_uri must include a database name.")

        target_db = url.database
        admin_url = url.set(database="postgres")
        admin_engine = create_engine(admin_url, future=True)

        with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"),
                {"db": target_db},
            ).scalar()
            if exists:
                return False
            conn.execute(text(f'CREATE DATABASE "{target_db}" OWNER "{url.username}"'))
            return True

    def create_db(self, overwrite: bool = False) -> bool:
        if self.exists_db(new_db=True) and not overwrite:
            self.logger.log(f"Database already exists at {self.db_uri}", "WARNING")
            return False

        if getattr(self, "db_uri", None):
            url = make_url(self._normalize_uri(self.db_uri))
            if url.drivername.startswith("postgresql"):
                try:
                    created = self.ensure_postgres_database(self.db_uri)
                    if created:
                        self.logger.log(f"Created PostgreSQL database '{url.database}'", "INFO")
                except Exception as e:
                    self.logger.log(f"Could not ensure PostgreSQL database: {e}", "ERROR")
                    raise

        self.connect(check_exists=False)

        try:
            self.logger.log("Bootstrapping models...", "INFO")
            bootstrap_models(self.engine)

            self.logger.log("Creating tables...", "INFO")
            Base.metadata.create_all(self.engine)

            self.logger.log("Seeding initial data...", "INFO")
            self._seed_all()

            self.logger.footer(f"Database created at {self.db_uri}")
            return True

        except Exception as e:
            self.logger.log(f"Failed to create database: {e}", "ERROR")
            raise

    def upgrade_db(self) -> None:
        """Re-apply seeds idempotently (safe to run repeatedly)."""
        self.connect(check_exists=True)
        bootstrap_models(self.engine)
        self._seed_all()
        self.logger.footer("Database upgraded successfully")

    def _seed_all(self) -> None:
        seed_dir = os.path.join(os.path.dirname(__file__), "seed")
        self._seed("initial_config.json", "model_config", "SystemConfig")
        self._seed("initial_metadata.json", "model_config", "IgemMetadata")
        self._seed("initial_source_systems.json", "model_etl", "ETLSourceSystem", key="source_systems")
        self._seed("initial_data_sources.json", "model_etl", "ETLDataSource", key="data_sources")
        self._seed("initial_entity_types.json", "model_entities", "EntityType", key="entity_types")
        self._seed("initial_entity_relationship_types.json", "model_entities", "EntityRelationshipType", key="entity_relationship_types")
        self._seed("initial_genome_assemblies.json", "model_config", "GenomeAssembly", key="genome_assemblies")

    def _seed(
        self,
        filename: str,
        module_name: str,
        model_name: str,
        key: Optional[str] = None,
    ) -> None:
        from importlib import import_module

        model_module = import_module(f"igem_backend.modules.db.models.{module_name}")
        model_class = getattr(model_module, model_name)

        seed_dir = os.path.join(os.path.dirname(__file__), "seed")
        json_path = os.path.join(seed_dir, filename)
        if not os.path.exists(json_path):
            self.logger.log(f"Seed file not found: {json_path}", "WARNING")
            return

        unique_keys = SEED_UNIQUE_KEYS.get(model_name)
        if not unique_keys:
            raise RuntimeError(f"No unique key config for seed model: {model_name}")

        with self.get_session() as session:
            with open(json_path) as f:
                data = json.load(f)
            records = data.get(key, data) if key else data

            created = updated = skipped = 0
            for item in records:
                # Resolve source_system FK by name
                if "source_system" in item:
                    from igem_backend.modules.db.models.model_etl import ETLSourceSystem
                    fk_name = item.pop("source_system")
                    fk_obj = session.query(ETLSourceSystem).filter_by(name=fk_name).first()
                    if not fk_obj:
                        self.logger.log(f"ETLSourceSystem not found: {fk_name}", "WARNING")
                        skipped += 1
                        continue
                    item["source_system_id"] = fk_obj.id

                lookup = {k: item.get(k) for k in unique_keys}
                if any(v is None for v in lookup.values()):
                    skipped += 1
                    continue

                existing = session.query(model_class).filter_by(**lookup).one_or_none()
                if existing is None:
                    session.add(model_class(**item))
                    created += 1
                else:
                    for k, v in item.items():
                        if v is not None:
                            setattr(existing, k, v)
                    updated += 1

            session.commit()
            self.logger.log(
                f"Seeded {model_name}: created={created} updated={updated} skipped={skipped}",
                "INFO",
            )
