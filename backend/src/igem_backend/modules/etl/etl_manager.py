from __future__ import annotations

import glob
import importlib
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Sequence

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from igem_backend.modules.db.database import Database
from igem_backend.modules.db.models.model_entities import (
    Entity,
    EntityRelationship,
)
from igem_backend.modules.db.models.model_etl import (
    ETLDataSource,
    ETLPackage,
    ETLSourceSystem,
)
from igem_backend.utils.logger import Logger


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

    def __init__(
        self,
        db: Database,
        debug_mode: bool = False,
        logger: Optional[Logger] = None,
    ):
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
        default_steps = ["extract", "transform", "load"]
        steps = [s.lower() for s in (steps or default_steps)]
        force_steps = [s.lower() for s in (force_steps or [])]

        with self.db.get_session() as session:
            ds_ids = self._resolve_ds_ids(session, source_system, data_sources)

        if not ds_ids:
            self.logger.log("No matching active DataSources found.", "WARNING")
            return

        for ds_id in ds_ids:
            with self.db.get_session() as session:
                ds = self._load_ds(session, ds_id)
                self._run_one(
                    session, ds, steps, force_steps,
                    download_path, processed_path,
                )

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
            ds_ids = self._resolve_ds_ids(
                session, source_system, data_sources, only_active
            )

        if not ds_ids:
            self.logger.log(
                "No matching DataSources found for run_all.", "WARNING"
            )
            return {"selected": 0, "skipped": 0, "succeeded": 0, "failed": 0}

        ok_statuses = {"completed", "up-to-date", "not-applicable"}
        summary = {
            "selected": len(ds_ids),
            "skipped": 0,
            "succeeded": 0,
            "failed": 0,
        }

        for ds_id in ds_ids:
            with self.db.get_session() as session:
                ds = self._load_ds(session, ds_id)

                if self._latest_load_status(session, ds.id) in ok_statuses:
                    summary["skipped"] += 1
                    self.logger.log(
                        f"Skipping '{ds.name}' (up-to-date).", "INFO"
                    )
                    continue

                self._run_one(
                    session, ds,
                    ["extract", "transform", "load"], [],
                    download_path, processed_path,
                )

                if self._latest_load_status(session, ds.id) in ok_statuses:
                    summary["succeeded"] += 1
                    self.logger.log(
                        f"run_all succeeded for '{ds.name}'.", "INFO"
                    )
                else:
                    summary["failed"] += 1
                    self.logger.log(
                        f"run_all failed for '{ds.name}'.", "ERROR"
                    )
                    if stop_on_error:
                        break

        self.logger.footer(
            f"run_all concluido: selected={summary['selected']} "
            f"skipped={summary['skipped']} succeeded={summary['succeeded']} "
            f"failed={summary['failed']}"
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

        status = "concluido" if all_ok else "concluido com erros"
        self.logger.footer(f"Rollback {status}")
        return all_ok

    # -------------------------------------------------------------------------
    # Core ETL pipeline
    # -------------------------------------------------------------------------

    @staticmethod
    def _fmt_duration(seconds: float) -> str:
        """Format elapsed seconds as '1h 23m 45s', '4m 12s', or '38s'."""
        s = int(seconds)
        h, rem = divmod(s, 3600)
        m, s = divmod(rem, 60)
        if h:
            return f"{h}h {m}m {s}s"
        if m:
            return f"{m}m {s}s"
        return f"{s}s"

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
        t0 = time.perf_counter()
        try:
            module = self._load_module(ds)
            if "extract" in steps:
                self._run_extract(
                    session, module, ds, download_path, force_steps
                )
            if "transform" in steps:
                self._run_transform(
                    session, module, ds,
                    download_path, processed_path, force_steps,
                )
            if "load" in steps:
                self._run_load(
                    session, module, ds, processed_path, force_steps
                )
            elapsed = self._fmt_duration(time.perf_counter() - t0)
            self.logger.footer(
                f"ETL concluido para '{ds.name}' em {elapsed}"
            )
        except (SQLAlchemyError, Exception) as e:
            elapsed = self._fmt_duration(time.perf_counter() - t0)
            self.logger.log(
                f"ETL failed for '{ds.name}' after {elapsed}: {e}", "ERROR"
            )
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

        t0 = time.perf_counter()
        dtp = module.DTP(
            logger=self.logger, debug_mode=self.debug_mode,
            datasource=ds, package=pkg, session=session, db=self.db,
        )
        ok, message, file_hash, step_stats = dtp.extract(raw_dir=download_path)
        elapsed = self._fmt_duration(time.perf_counter() - t0)

        pkg.extract_end = datetime.now()
        pkg.extract_hash = file_hash
        pkg.version_tag = getattr(module.DTP, "DTP_VERSION", None)
        pkg.extract_rows = step_stats.row_count or None

        if ok and file_hash:
            prev = self._find_pkg(
                session, ds.id, "extract",
                ["completed", "up-to-date"], ETLPackage.extract_end,
                extra=[ETLPackage.extract_hash == file_hash],
            )
            if prev and "extract" not in force_steps:
                pkg.status = pkg.extract_status = "up-to-date"
                pkg.stats = step_stats.to_dict()
                self.logger.log(
                    f"[Extract] Up-to-date for '{ds.name}' ({elapsed})", "INFO"
                )
            else:
                pkg.status = pkg.extract_status = "completed"
                pkg.stats = step_stats.to_dict()
                self.logger.log(
                    f"[Extract] Completed for '{ds.name}' in {elapsed}", "INFO"
                )
        elif ok:
            pkg.status = pkg.extract_status = "completed"
            pkg.stats = step_stats.to_dict()
        else:
            pkg.status = pkg.extract_status = "failed"
            pkg.stats = {
                "error": message,
                "step": "extract",
                **step_stats.to_dict(),
            }
            self.logger.log(
                f"[Extract] Failed for '{ds.name}' after {elapsed}: {message}",
                "ERROR",
            )

        session.commit()

    # -------------------------------------------------------------------------
    # Transform
    # -------------------------------------------------------------------------
    def _run_transform(
        self, session, module, ds, download_path, processed_path, force_steps
    ):
        last_extract = self._find_pkg(
            session, ds.id, "extract",
            ["completed", "up-to-date"], ETLPackage.extract_end,
        )
        if not last_extract:
            self.logger.log(
                f"[Transform] No successful extract for '{ds.name}'"
                " — skipping.",
                "WARNING",
            )
            return

        prev = self._find_pkg(
            session, ds.id, "transform", ["completed", "up-to-date"],
            ETLPackage.transform_end,
            extra=[ETLPackage.transform_hash == last_extract.extract_hash],
        )
        if prev and "transform" not in force_steps:
            self.logger.log(
                f"[Transform] Up-to-date for '{ds.name}'", "INFO"
            )
            return

        pkg = self._create_pkg(session, ds, "transform")
        if not pkg:
            return
        pkg.transform_hash = last_extract.extract_hash
        session.commit()

        t0 = time.perf_counter()
        dtp = module.DTP(
            logger=self.logger, debug_mode=self.debug_mode,
            datasource=ds, package=pkg, session=session, db=self.db,
        )
        ok, message, step_stats = dtp.transform(download_path, processed_path)
        elapsed = self._fmt_duration(time.perf_counter() - t0)

        pkg.transform_end = datetime.now()
        pkg.transform_rows = step_stats.row_count or None
        pkg.status = pkg.transform_status = "completed" if ok else "failed"
        if not ok:
            pkg.stats = {
                "error": message,
                "step": "transform",
                **step_stats.to_dict(),
            }
            self.logger.log(
                f"[Transform] Failed for '{ds.name}' after {elapsed}: "
                f"{message}",
                "ERROR",
            )
        else:
            pkg.stats = step_stats.to_dict()
            self.logger.log(
                f"[Transform] Completed for '{ds.name}' in {elapsed}", "INFO"
            )
        session.commit()

    # -------------------------------------------------------------------------
    # Load
    # -------------------------------------------------------------------------
    def _run_load(self, session, module, ds, processed_path, force_steps):
        last_transform = self._find_pkg(
            session, ds.id, "transform",
            ["completed", "up-to-date"], ETLPackage.transform_end,
        )
        if not last_transform:
            self.logger.log(
                f"[Load] No successful transform for '{ds.name}' — skipping.",
                "WARNING",
            )
            return

        prev = self._find_pkg(
            session, ds.id, "load", ["completed", "up-to-date"],
            ETLPackage.load_end,
            extra=[ETLPackage.load_hash == last_transform.transform_hash],
        )
        if prev and "load" not in force_steps:
            self.logger.log(f"[Load] Up-to-date for '{ds.name}'", "INFO")
            return

        pkg = self._create_pkg(session, ds, "load")
        if not pkg:
            return
        pkg.load_hash = last_transform.transform_hash
        session.commit()

        t0 = time.perf_counter()
        dtp = module.DTP(
            logger=self.logger, debug_mode=self.debug_mode,
            datasource=ds, package=pkg, session=session, db=self.db,
        )
        ok, message, step_stats = dtp.load(processed_path)
        elapsed = self._fmt_duration(time.perf_counter() - t0)

        pkg.load_end = datetime.now()
        pkg.load_rows = step_stats.row_count or None
        pkg.status = pkg.load_status = "completed" if ok else "failed"
        if not ok:
            pkg.stats = {
                "error": message,
                "step": "load",
                **step_stats.to_dict(),
            }
            self.logger.log(
                f"[Load] Failed for '{ds.name}' after {elapsed}: {message}",
                "ERROR",
            )
        else:
            pkg.stats = step_stats.to_dict()
            self.logger.log(
                f"[Load] Completed for '{ds.name}' in {elapsed}", "INFO"
            )
        session.commit()

    # -------------------------------------------------------------------------
    # Rollback helpers
    # -------------------------------------------------------------------------
    def _rollback_ds(
        self, session: Session, ds: ETLDataSource
    ) -> tuple[bool, str]:
        strategy = self._rollback_strategy(ds)
        try:
            counts = self._apply_rollback(
                session, strategy, data_source_id=ds.id
            )
            session.commit()
            msg = (
                f"Rollback ({strategy}) completed for '{ds.name}': "
                + ", ".join(f"{k}={v}" for k, v in counts.items())
            )
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
                pkg = session.get(ETLPackage, pkg_id)
                if not pkg:
                    self.logger.log(
                        f"Package {pkg_id} not found.", "WARNING"
                    )
                    continue
                ds = self._load_ds(session, pkg.data_source_id)
                strategy = self._rollback_strategy(ds)
                try:
                    counts = self._apply_rollback(
                        session, strategy, package_id=pkg_id
                    )
                    session.commit()
                    self.logger.log(
                        f"Rollback ({strategy}) pkg={pkg_id}: "
                        + ", ".join(f"{k}={v}" for k, v in counts.items()),
                        "INFO",
                    )
                except Exception as e:
                    session.rollback()
                    self.logger.log(
                        f"Rollback failed for package {pkg_id}: {e}", "ERROR"
                    )
                    all_ok = False
        return all_ok

    def _rollback_strategy(self, ds: ETLDataSource) -> str:
        """Read ROLLBACK_STRATEGY from the DTP class; default to 'deactivate'."""
        try:
            module = self._load_module(ds)
            return getattr(module.DTP, "ROLLBACK_STRATEGY", "deactivate")
        except Exception:
            return "deactivate"

    def _apply_rollback(
        self,
        session: Session,
        strategy: str,
        data_source_id: Optional[int] = None,
        package_id: Optional[int] = None,
    ) -> dict[str, int]:
        """
        Execute rollback according to strategy.

        deactivate  — master DTPs: set is_active=False on entities and aliases.
                      Relationships from other sources stay intact.
        delete      — relationship DTPs: delete EntityRelationship rows.
                      Safe because relationships don't own entities.
        mixed       — both: deactivate master data + delete relationships.
        """
        from igem_backend.modules.db.models.model_entities import EntityAlias

        counts: dict[str, int] = {}

        if strategy in ("deactivate", "mixed"):
            by_pkg = package_id is not None
            filt_e = (
                Entity.etl_package_id == package_id
                if by_pkg
                else Entity.data_source_id == data_source_id
            )
            filt_a = (
                EntityAlias.etl_package_id == package_id
                if by_pkg
                else EntityAlias.data_source_id == data_source_id
            )
            counts["entities_deactivated"] = (
                session.query(Entity)
                .filter(filt_e)
                .update({"is_active": False}, synchronize_session=False)
            )
            counts["aliases_deactivated"] = (
                session.query(EntityAlias)
                .filter(filt_a)
                .update({"is_active": False}, synchronize_session=False)
            )

        if strategy in ("delete", "mixed"):
            filt_r = (
                EntityRelationship.etl_package_id == package_id
                if package_id is not None
                else EntityRelationship.data_source_id == data_source_id
            )
            counts["relationships_deleted"] = (
                session.query(EntityRelationship)
                .filter(filt_r)
                .delete(synchronize_session=False)
            )

        return counts

    # -------------------------------------------------------------------------
    # Package / datasource helpers
    # -------------------------------------------------------------------------
    def _create_pkg(
        self, session: Session, ds: ETLDataSource, op: str
    ) -> Optional[ETLPackage]:
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
            self.logger.log(
                f"Created ETLPackage id={pkg.id} op={op} for '{ds.name}'",
                "DEBUG",
            )
            return pkg
        except Exception as e:
            self.logger.log(
                f"Could not create ETLPackage for '{ds.name}': {e}", "ERROR"
            )
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

    def _latest_load_status(
        self, session: Session, ds_id: int
    ) -> Optional[str]:
        row = (
            session.query(ETLPackage.load_status)
            .filter(
                ETLPackage.data_source_id == ds_id,
                ETLPackage.operation_type == "load",
            )
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
            raise ValueError(
                f"DataSource '{ds.name}' has no dtp_script defined."
            )
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
            pattern = os.path.join(
                base, ds.source_system.name, ds.name + "*"
            )
            for path in glob.glob(pattern):
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    else:
                        os.remove(path)
                except Exception as e:
                    self.logger.log(
                        f"Could not delete {path}: {e}", "WARNING"
                    )

    def status(self) -> list[dict]:
        """
        Return one status row per data source with per-step breakdown.

        Each row contains the latest package for each operation type
        (extract / transform / load) independently, so partial runs and
        resume scenarios are reflected correctly.
        """
        with self.db.get_session() as session:
            # Latest package id per (data_source_id, operation_type)
            latest_sq = (
                session.query(
                    ETLPackage.data_source_id,
                    ETLPackage.operation_type,
                    func.max(ETLPackage.id).label("max_id"),
                )
                .group_by(
                    ETLPackage.data_source_id, ETLPackage.operation_type
                )
                .subquery()
            )
            latest_pkgs = (
                session.query(ETLPackage)
                .join(
                    latest_sq,
                    (ETLPackage.data_source_id == latest_sq.c.data_source_id)
                    & (ETLPackage.operation_type == latest_sq.c.operation_type)
                    & (ETLPackage.id == latest_sq.c.max_id),
                )
                .all()
            )

            # {ds_id: {"extract": pkg, "transform": pkg, "load": pkg}}
            pkg_map: dict[int, dict[str, ETLPackage]] = {}
            for pkg in latest_pkgs:
                pkg_map.setdefault(pkg.data_source_id, {})[
                    pkg.operation_type
                ] = pkg

            data_sources = (
                session.query(ETLDataSource)
                .order_by(ETLDataSource.id)
                .all()
            )

            result = []
            for ds in data_sources:
                steps = pkg_map.get(ds.id, {})
                ext = steps.get("extract")
                trn = steps.get("transform")
                lod = steps.get("load")

                timestamps = [
                    t for t in [
                        lod.load_end if lod else None,
                        trn.transform_end if trn else None,
                        ext.extract_end if ext else None,
                    ] if t is not None
                ]
                last_run = max(timestamps) if timestamps else None

                result.append({
                    "data_source":      ds.name,
                    "active":           ds.active,
                    "extract_status":   ext.status if ext else None,
                    "transform_status": trn.status if trn else None,
                    "load_status":      lod.status if lod else None,
                    "load_rows":        lod.load_rows if lod else None,
                    "version_tag":      (
                        (lod or ext or trn).version_tag
                        if (lod or ext or trn) else None
                    ),
                    "last_run": (
                        last_run.strftime("%m-%d %H:%M") if last_run else None
                    ),
                })
            return result
