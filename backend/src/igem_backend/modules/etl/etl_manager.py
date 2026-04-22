from __future__ import annotations

import glob
import importlib
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Sequence

from sqlalchemy import MetaData, func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from igem_backend.modules.db.database import Database
from igem_backend.modules.db.models.model_entities import Entity, EntityRelationship
from igem_backend.modules.db.models.model_etl import ETLDataSource, ETLPackage, ETLSourceSystem
from igem_backend.utils.logger import Logger

PURGE_ORDER = [
    "entity_relationships",
    "entity_aliases",
    "entities",
]


@dataclass(frozen=True)
class StepResult:
    ok: bool
    message: str
    hash_value: Optional[str] = None


class ETLManager:
    """
    IGEM ETL Orchestrator.

    One SQLAlchemy session per DataSource run (extract → transform → load).
    DTPs receive both `session` (ORM) and `db` (engine/dialect access).
    """

    def __init__(self, db: Database, debug_mode: bool = False, logger: Optional[Logger] = None):
        self.db = db
        self.debug_mode = debug_mode
        self.logger = logger or Logger()
        self._dtp_cache: dict[str, Any] = {}

    # -------------------------------------------------------------------------
    # Public entry points
    # -------------------------------------------------------------------------
    def run(
        self,
        data_sources: Optional[Sequence[str]] = None,
        source_system: Optional[Sequence[str]] = None,
        steps: Optional[Sequence[str]] = None,
        force_steps: Optional[Sequence[str]] = None,
        download_path: Optional[str] = None,
        processed_path: Optional[str] = None,
    ) -> None:
        """Run ETL for selected data sources."""
        steps = [s.lower() for s in (steps or ["extract", "transform", "load"])]
        force_steps = [s.lower() for s in (force_steps or [])]

        with self.db.get_session() as session:
            ds_ids = self._resolve_ds_ids(session, source_system, data_sources)

        if not ds_ids:
            self.logger.log("No matching active DataSources found.", "WARNING")
            return

        for ds_id in ds_ids:
            with self.db.get_session() as session:
                ds = self._load_ds(session, ds_id)
                self._run_one(session, ds, steps, force_steps, download_path, processed_path)

    def run_all(
        self,
        data_sources: Optional[Sequence[str]] = None,
        source_system: Optional[Sequence[str]] = None,
        download_path: Optional[str] = None,
        processed_path: Optional[str] = None,
        only_active: bool = True,
        stop_on_error: bool = False,
    ) -> dict[str, int]:
        """
        Resume-friendly ETL for all data sources.
        Skips sources whose latest load is already successful.
        """
        with self.db.get_session() as session:
            ds_ids = self._resolve_ds_ids(session, source_system, data_sources, only_active)

        if not ds_ids:
            self.logger.log("No matching DataSources found for run_all.", "WARNING")
            return {"selected": 0, "skipped": 0, "succeeded": 0, "failed": 0}

        ok_statuses = {"completed", "up-to-date", "not-applicable"}
        summary = {"selected": len(ds_ids), "skipped": 0, "succeeded": 0, "failed": 0}

        for ds_id in ds_ids:
            with self.db.get_session() as session:
                ds = self._load_ds(session, ds_id)

                if self._latest_load_status(session, ds.id) in ok_statuses:
                    summary["skipped"] += 1
                    self.logger.log(f"Skipping '{ds.name}' (already up-to-date).", "INFO")
                    continue

                self._run_one(session, ds, ["extract", "transform", "load"], [],
                              download_path, processed_path)

                if self._latest_load_status(session, ds.id) in ok_statuses:
                    summary["succeeded"] += 1
                    self.logger.log(f"run_all succeeded for '{ds.name}'.", "INFO")
                else:
                    summary["failed"] += 1
                    self.logger.log(f"run_all failed for '{ds.name}'.", "ERROR")
                    if stop_on_error:
                        break

        self.logger.log(
            f"run_all summary: selected={summary['selected']} "
            f"skipped={summary['skipped']} succeeded={summary['succeeded']} "
            f"failed={summary['failed']}",
            "INFO",
        )
        return summary

    def rollback(
        self,
        data_sources: Optional[Sequence[str]] = None,
        source_system: Optional[Sequence[str]] = None,
        package_ids: Optional[Sequence[int]] = None,
        delete_files: bool = False,
        download_path: Optional[str] = None,
        processed_path: Optional[str] = None,
    ) -> bool:
        """Rollback ETL data by data_source or specific package_ids."""
        if package_ids:
            return self._rollback_by_packages(package_ids)

        with self.db.get_session() as session:
            ds_ids = self._resolve_ds_ids(session, source_system, data_sources)

        all_ok = True
        for ds_id in ds_ids:
            with self.db.get_session() as session:
                ds = self._load_ds(session, ds_id)
                ok, _ = self._rollback_ds(session, ds)
                if not ok:
                    all_ok = False
                    continue
                if delete_files:
                    self._clean_files(ds, download_path, processed_path)
        return all_ok

    # -------------------------------------------------------------------------
    # Core ETL pipeline
    # -------------------------------------------------------------------------
    def _run_one(
        self,
        session: Session,
        ds: ETLDataSource,
        steps: Sequence[str],
        force_steps: Sequence[str],
        download_path: Optional[str],
        processed_path: Optional[str],
    ) -> None:
        self.logger.log(f"Starting ETL for '{ds.name}'", "INFO")
        try:
            module = self._load_module(ds)
            if "extract" in steps:
                self._run_extract(session, module, ds, download_path, force_steps)
            if "transform" in steps:
                self._run_transform(session, module, ds, download_path, processed_path, force_steps)
            if "load" in steps:
                self._run_load(session, module, ds, processed_path, force_steps)
            self.logger.log(f"ETL finished for '{ds.name}'", "INFO")
        except (SQLAlchemyError, Exception) as e:
            self.logger.log(f"ETL failed for '{ds.name}': {e}", "ERROR")
            try:
                session.rollback()
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # Extract
    # -------------------------------------------------------------------------
    def _run_extract(self, session, module, ds, download_path, force_steps):
        pkg = self._create_pkg(session, ds, "extract")
        if not pkg:
            return

        dtp = module.DTP(logger=self.logger, debug_mode=self.debug_mode,
                         datasource=ds, package=pkg, session=session, db=self.db)
        ok, message, file_hash = dtp.extract(raw_dir=download_path)

        pkg.extract_end = datetime.now()
        pkg.extract_hash = file_hash

        if ok and file_hash:
            prev = self._find_pkg(session, ds.id, "extract",
                                  ["completed", "up-to-date"], ETLPackage.extract_end,
                                  extra=[ETLPackage.extract_hash == file_hash])
            if prev and "extract" not in force_steps:
                pkg.status = pkg.extract_status = "up-to-date"
                self.logger.log(f"[Extract] Up-to-date for '{ds.name}'", "INFO")
            else:
                pkg.status = pkg.extract_status = "completed"
                self.logger.log(f"[Extract] Completed for '{ds.name}'", "INFO")
        elif ok:
            pkg.status = pkg.extract_status = "completed"
        else:
            pkg.status = pkg.extract_status = "failed"
            pkg.stats = {"error": message, "step": "extract"}
            self.logger.log(f"[Extract] Failed for '{ds.name}': {message}", "ERROR")

        session.commit()

    # -------------------------------------------------------------------------
    # Transform
    # -------------------------------------------------------------------------
    def _run_transform(self, session, module, ds, download_path, processed_path, force_steps):
        last_extract = self._find_pkg(session, ds.id, "extract",
                                      ["completed", "up-to-date"], ETLPackage.extract_end)
        if not last_extract:
            self.logger.log(f"[Transform] No successful extract for '{ds.name}' — skipping.", "WARNING")
            return

        prev = self._find_pkg(session, ds.id, "transform", ["completed", "up-to-date"],
                               ETLPackage.transform_end,
                               extra=[ETLPackage.transform_hash == last_extract.extract_hash])
        if prev and "transform" not in force_steps:
            self.logger.log(f"[Transform] Up-to-date for '{ds.name}'", "INFO")
            return

        pkg = self._create_pkg(session, ds, "transform")
        if not pkg:
            return
        pkg.transform_hash = last_extract.extract_hash
        session.commit()

        dtp = module.DTP(logger=self.logger, debug_mode=self.debug_mode,
                         datasource=ds, package=pkg, session=session, db=self.db)
        ok, message = dtp.transform(download_path, processed_path)

        pkg.transform_end = datetime.now()
        pkg.status = pkg.transform_status = "completed" if ok else "failed"
        if not ok:
            pkg.stats = {"error": message, "step": "transform"}
            self.logger.log(f"[Transform] Failed for '{ds.name}': {message}", "ERROR")
        else:
            self.logger.log(f"[Transform] Completed for '{ds.name}'", "INFO")
        session.commit()

    # -------------------------------------------------------------------------
    # Load
    # -------------------------------------------------------------------------
    def _run_load(self, session, module, ds, processed_path, force_steps):
        last_transform = self._find_pkg(session, ds.id, "transform",
                                        ["completed", "up-to-date"], ETLPackage.transform_end)
        if not last_transform:
            self.logger.log(f"[Load] No successful transform for '{ds.name}' — skipping.", "WARNING")
            return

        prev = self._find_pkg(session, ds.id, "load", ["completed", "up-to-date"],
                               ETLPackage.load_end,
                               extra=[ETLPackage.load_hash == last_transform.transform_hash])
        if prev and "load" not in force_steps:
            self.logger.log(f"[Load] Up-to-date for '{ds.name}'", "INFO")
            return

        pkg = self._create_pkg(session, ds, "load")
        if not pkg:
            return
        pkg.load_hash = last_transform.transform_hash
        session.commit()

        dtp = module.DTP(logger=self.logger, debug_mode=self.debug_mode,
                         datasource=ds, package=pkg, session=session, db=self.db)
        ok, message = dtp.load(processed_path)

        pkg.load_end = datetime.now()
        pkg.status = pkg.load_status = "completed" if ok else "failed"
        if not ok:
            pkg.stats = {"error": message, "step": "load"}
            self.logger.log(f"[Load] Failed for '{ds.name}': {message}", "ERROR")
        else:
            self.logger.log(f"[Load] Completed for '{ds.name}'", "INFO")
        session.commit()

    # -------------------------------------------------------------------------
    # Rollback helpers
    # -------------------------------------------------------------------------
    def _rollback_ds(self, session: Session, ds: ETLDataSource) -> tuple[bool, str]:
        conflicts = self._check_conflicts(session, data_source_id=ds.id)
        if conflicts:
            msg = (f"Rollback blocked for '{ds.name}': {conflicts} relationship(s) "
                   "from other sources depend on entities being rolled back.")
            self.logger.log(msg, "ERROR")
            return False, msg

        try:
            deleted = self._purge_by_ds(session, ds.id)
            session.commit()
            msg = f"Rollback completed for '{ds.name}' (deleted={sum(deleted.values())} rows)"
            self.logger.log(msg, "INFO")
            return True, msg
        except Exception as e:
            session.rollback()
            msg = f"Rollback failed for '{ds.name}': {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

    def _rollback_by_packages(self, package_ids: Sequence[int]) -> bool:
        all_ok = True
        for pkg_id in package_ids:
            with self.db.get_session() as session:
                conflicts = self._check_conflicts(session, package_id=pkg_id)
                if conflicts:
                    self.logger.log(f"Rollback blocked for package {pkg_id}: {conflicts} conflicts.", "ERROR")
                    all_ok = False
                    continue
                try:
                    deleted = self._purge_by_pkg(session, pkg_id)
                    session.commit()
                    self.logger.log(f"Rollback completed for package {pkg_id} (deleted={sum(deleted.values())})", "INFO")
                except Exception as e:
                    session.rollback()
                    self.logger.log(f"Rollback failed for package {pkg_id}: {e}", "ERROR")
                    all_ok = False
        return all_ok

    def _check_conflicts(
        self,
        session: Session,
        data_source_id: Optional[int] = None,
        package_id: Optional[int] = None,
    ) -> int:
        if data_source_id is None and package_id is None:
            return 0

        if package_id is not None:
            entity_ids_q = session.query(Entity.id).filter(Entity.etl_package_id == package_id)
        else:
            entity_ids_q = session.query(Entity.id).filter(Entity.data_source_id == data_source_id)

        entity_ids_sub = entity_ids_q.subquery()

        uses = or_(
            EntityRelationship.entity_1_id.in_(select(entity_ids_sub.c.id)),
            EntityRelationship.entity_2_id.in_(select(entity_ids_sub.c.id)),
        )

        if package_id is not None:
            mismatch = EntityRelationship.etl_package_id != package_id
        else:
            mismatch = EntityRelationship.data_source_id != data_source_id

        return int(
            session.execute(
                select(func.count(EntityRelationship.id))
                .where(uses)
                .where(mismatch)
            ).scalar() or 0
        )

    def _purge_by_ds(self, session: Session, ds_id: int) -> dict[str, int]:
        return self._purge(session, "data_source_id", ds_id)

    def _purge_by_pkg(self, session: Session, pkg_id: int) -> dict[str, int]:
        return self._purge(session, "etl_package_id", pkg_id)

    def _purge(self, session: Session, col: str, val: int) -> dict[str, int]:
        engine = session.get_bind()
        meta = MetaData()
        meta.reflect(bind=engine)
        deleted: dict[str, int] = {}

        # Delete in safe order (children first)
        ordered = [t for name in PURGE_ORDER if (t := meta.tables.get(name))]
        rest = [t for t in reversed(list(meta.sorted_tables))
                if t.name not in PURGE_ORDER and col in t.c]
        for table in ordered + rest:
            if col not in table.c:
                continue
            result = session.execute(table.delete().where(table.c[col] == val))
            if result.rowcount:
                deleted[table.name] = int(result.rowcount)
        return deleted

    # -------------------------------------------------------------------------
    # Package / datasource helpers
    # -------------------------------------------------------------------------
    def _create_pkg(self, session: Session, ds: ETLDataSource, op: str) -> Optional[ETLPackage]:
        try:
            pkg = ETLPackage(
                data_source_id=ds.id,
                status="running",
                operation_type=op,
                active=True,
                extract_status="not-applicable",
                transform_status="not-applicable",
                load_status="not-applicable",
            )
            setattr(pkg, f"{op}_start", datetime.now())
            setattr(pkg, f"{op}_status", "running")
            session.add(pkg)
            session.commit()
            self.logger.log(f"Created ETLPackage id={pkg.id} op={op} for '{ds.name}'", "DEBUG")
            return pkg
        except Exception as e:
            self.logger.log(f"Could not create ETLPackage for '{ds.name}': {e}", "ERROR")
            session.rollback()
            return None

    def _find_pkg(
        self,
        session: Session,
        ds_id: int,
        op: str,
        ok_statuses: list[str],
        order_col,
        extra: Optional[list] = None,
    ) -> Optional[ETLPackage]:
        q = session.query(ETLPackage).filter(
            ETLPackage.data_source_id == ds_id,
            ETLPackage.operation_type == op,
            ETLPackage.status.in_(ok_statuses),
        )
        if extra:
            for f in extra:
                q = q.filter(f)
        return q.order_by(order_col.desc()).first()

    def _latest_load_status(self, session: Session, ds_id: int) -> Optional[str]:
        row = (
            session.query(ETLPackage.load_status)
            .filter(ETLPackage.data_source_id == ds_id, ETLPackage.operation_type == "load")
            .order_by(ETLPackage.created_at.desc(), ETLPackage.id.desc())
            .first()
        )
        return str(row[0]).strip().lower() if row and row[0] else None

    def _load_ds(self, session: Session, ds_id: int) -> ETLDataSource:
        return (
            session.query(ETLDataSource)
            .options(selectinload(ETLDataSource.source_system))
            .filter(ETLDataSource.id == ds_id)
            .one()
        )

    def _resolve_ds_ids(
        self,
        session: Session,
        source_system: Optional[Sequence[str]],
        data_sources: Optional[Sequence[str]],
        only_active: bool = True,
    ) -> list[int]:
        q = session.query(ETLDataSource.id)
        if only_active:
            q = q.join(ETLSourceSystem).filter(
                ETLDataSource.active.is_(True),
                ETLSourceSystem.active.is_(True),
            )
        if source_system:
            q = q.filter(ETLSourceSystem.name.in_(list(source_system)))
        if data_sources:
            q = q.filter(ETLDataSource.name.in_(list(data_sources)))
        return [r[0] for r in q.order_by(ETLDataSource.id.asc()).all()]

    def _load_module(self, ds: ETLDataSource):
        script = (ds.dtp_script or "").strip().lower()
        if not script:
            raise ValueError(f"DataSource '{ds.name}' has no dtp_script defined.")
        if script not in self._dtp_cache:
            self._dtp_cache[script] = importlib.import_module(
                f"igem_backend.modules.etl.dtps.{script}"
            )
        return self._dtp_cache[script]

    # -------------------------------------------------------------------------
    # File cleanup
    # -------------------------------------------------------------------------
    def _clean_files(self, ds, download_path, processed_path):
        for base in filter(None, [download_path, processed_path]):
            pattern = os.path.join(base, ds.source_system.name, ds.name + "*")
            for path in glob.glob(pattern):
                try:
                    shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)
                except Exception as e:
                    self.logger.log(f"Could not delete {path}: {e}", "WARNING")

    def status(self) -> list[dict]:
        """Return a summary of the latest ETL run per data source."""
        with self.db.get_session() as session:
            rows = (
                session.query(ETLDataSource, ETLPackage)
                .outerjoin(
                    ETLPackage,
                    (ETLPackage.data_source_id == ETLDataSource.id) &
                    (ETLPackage.id == (
                        session.query(func.max(ETLPackage.id))
                        .filter(ETLPackage.data_source_id == ETLDataSource.id)
                        .correlate(ETLDataSource)
                        .scalar_subquery()
                    ))
                )
                .order_by(ETLDataSource.id)
                .all()
            )
            return [
                {
                    "data_source": ds.name,
                    "active": ds.active,
                    "status": pkg.status if pkg else "never_run",
                    "load_status": pkg.load_status if pkg else None,
                    "updated_at": pkg.created_at.isoformat() if pkg else None,
                }
                for ds, pkg in rows
            ]
