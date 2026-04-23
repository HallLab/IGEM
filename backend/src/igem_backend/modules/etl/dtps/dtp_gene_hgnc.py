"""
HGNC Gene Baseline DTP.

Pipeline role:
- First step in the Gene ingestion pipeline.
- Establishes the canonical Gene entity universe in IGEM.
- Later DTPs (gene_ncbi, CTD, etc.) reference gene entities created here.

What is loaded:
- Entity (type=Genes) + EntityAlias per approved HGNC gene symbol
- GeneMaster with locus classification, chromosome, approval status
- GeneLocusGroup / GeneLocusType (lookup tables, populated on first run)
- GeneGroup + GeneGroupMembership (HGNC gene family annotations)
"""

import json
import re
from typing import Optional

import numpy as np
import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_gene_hgnc"
    DTP_VERSION = "1.0.0"
    DTP_TYPE = "master"
    ROLLBACK_STRATEGY = "deactivate"

    ALIAS_SCHEMA = {
        "symbol":          ("preferred", "HGNC",    True),
        "hgnc_id":         ("code",      "HGNC",    None),
        "ensembl_gene_id": ("code",      "ENSEMBL", None),
        "entrez_id":       ("code",      "NCBI",    None),
        "ucsc_id":         ("code",      "UCSC",    None),
        "name":            ("synonym",   "HGNC",    None),
        "prev_symbol":     ("synonym",   "HGNC",    None),
        "prev_name":       ("synonym",   "HGNC",    None),
        "alias_symbol":    ("synonym",   "HGNC",    None),
        "alias_name":      ("synonym",   "HGNC",    None),
    }

    def __init__(self, logger, debug_mode, datasource, package, session, db):
        super().__init__()
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db

    # -------------------------------------------------------------------------
    # EXTRACT
    # -------------------------------------------------------------------------
    def extract(
        self, raw_dir: str
    ) -> tuple[bool, str, Optional[str], ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Extract starting...", "INFO")
        try:
            landing = self._dtp_dir(raw_dir)
            file_path = landing / "hgnc_data.json"

            url = self.data_source.source_url
            self.logger.log(f"Fetching HGNC JSON from {url}", "INFO")

            resp = requests.get(
                url, headers={"Accept": "application/json"}, timeout=120
            )
            resp.raise_for_status()

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(resp.text)

            file_hash = self._hash_file(file_path)
            file_size = file_path.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = f"HGNC data downloaded to {file_path}"
            self.logger.log(msg, "INFO")
            return True, msg, file_hash, stats

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Extract failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, None, ETLStepStats(errors=1)

    # -------------------------------------------------------------------------
    # TRANSFORM
    # -------------------------------------------------------------------------
    def transform(
        self, raw_dir: str, processed_dir: str
    ) -> tuple[bool, str, ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Transform starting...", "INFO")
        try:
            input_file = self._dtp_dir(raw_dir) / "hgnc_data.json"
            if not input_file.exists():
                return False, f"Input file not found: {input_file}", ETLStepStats(errors=1)

            output_dir = self._dtp_dir(processed_dir)
            out_parquet = output_dir / "master_data.parquet"

            with open(input_file, encoding="utf-8") as f:
                data = json.load(f)

            df = pd.DataFrame(data["response"]["docs"])
            df.to_parquet(out_parquet, index=False)

            if self.debug_mode:
                df.to_csv(output_dir / "master_data.csv", index=False)

            stats = ETLStepStats(
                total=len(df),
                columns=len(df.columns),
                output_size_bytes=out_parquet.stat().st_size,
            )
            msg = f"Transformed {len(df)} HGNC records -> {out_parquet}"
            self.logger.log(msg, "INFO")
            return True, msg, stats

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Transform failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, ETLStepStats(errors=1)

    # -------------------------------------------------------------------------
    # LOAD
    # -------------------------------------------------------------------------
    def load(self, processed_dir: str) -> tuple[bool, str, ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Load starting...", "INFO")

        parquet_file = self._dtp_dir(processed_dir) / "master_data.parquet"
        if not parquet_file.exists():
            return False, f"Processed file not found: {parquet_file}", ETLStepStats(errors=1)

        try:
            df = pd.read_parquet(parquet_file, engine="pyarrow")
        except Exception as e:
            return False, f"Could not read parquet: {e}", ETLStepStats(errors=1)

        try:
            type_id = self.get_entity_type_id("Genes")
        except ValueError as e:
            return False, str(e), ETLStepStats(errors=1)

        # Pre-load lookup caches to avoid per-row DB hits
        from igem_backend.modules.db.models.model_genes import (
            GeneGroup,
            GeneGroupMembership,
            GeneLocusGroup,
            GeneLocusType,
            GeneMaster,
        )

        locus_group_cache: dict[str, int] = {
            r.name: r.id for r in self.session.query(GeneLocusGroup).all()
        }
        locus_type_cache: dict[str, int] = {
            r.name: r.id for r in self.session.query(GeneLocusType).all()
        }
        gene_group_cache: dict[str, int] = {
            r.name: r.id for r in self.session.query(GeneGroup).all()
        }

        total = created = updated = warnings = 0
        gene_masters_created = gene_masters_updated = 0
        locus_groups_before = len(locus_group_cache)
        locus_types_before = len(locus_type_cache)
        gene_groups_before = len(gene_group_cache)
        memberships_created = 0
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                warnings += 1
                continue

            is_active = row.get("status") == "Approved"
            aliases = self.build_aliases(row, self.ALIAS_SCHEMA)
            primary = next((a for a in aliases if a.get("is_primary")), None)
            if not primary:
                warnings += 1
                continue

            secondary = [a for a in aliases if a is not primary]

            # --- Entity + EntityAlias ---
            entity_id, is_new = self.get_or_create_entity(
                name=primary["alias_value"],
                type_id=type_id,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                alias_type=primary["alias_type"],
                xref_source=primary["xref_source"],
                alias_norm=primary["alias_norm"],
                is_active=is_active,
                auto_commit=False,
            )
            if entity_id is None:
                warnings += 1
                continue

            if is_new:
                created += 1
            else:
                updated += 1

            self.add_aliases(
                entity_id=entity_id,
                type_id=type_id,
                aliases=secondary,
                is_active=is_active,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                auto_commit=False,
            )

            # --- GeneMaster ---
            locus_group_id = self._get_or_create_locus(
                GeneLocusGroup, locus_group_cache,
                row.get("locus_group"),
            )
            locus_type_id = self._get_or_create_locus(
                GeneLocusType, locus_type_cache,
                row.get("locus_type"),
            )
            chromosome = self._parse_chromosome(row.get("location"))

            gm = (
                self.session.query(GeneMaster)
                .filter_by(entity_id=entity_id)
                .one_or_none()
            )
            if gm is None:
                gm = GeneMaster(
                    entity_id=entity_id,
                    symbol=symbol,
                    hgnc_status=row.get("status"),
                    chromosome=chromosome,
                    locus_group_id=locus_group_id,
                    locus_type_id=locus_type_id,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(gm)
                gene_masters_created += 1
            else:
                gm.symbol = symbol
                gm.hgnc_status = row.get("status")
                gm.chromosome = chromosome
                gm.locus_group_id = locus_group_id
                gm.locus_type_id = locus_type_id
                gene_masters_updated += 1

            # flush to get gm.id before writing memberships
            self.session.flush()

            # --- GeneGroup memberships ---
            raw_groups = row.get("gene_group")
            if isinstance(raw_groups, (list, np.ndarray)):
                gene_groups: list[str] = [
                    str(g) for g in raw_groups
                    if g is not None and str(g) not in ("nan", "")
                ]
            elif raw_groups is None or (
                isinstance(raw_groups, float) and pd.isna(raw_groups)
            ):
                gene_groups = []
            else:
                s = str(raw_groups).strip()
                gene_groups = [s] if s and s != "nan" else []
            for group_name in gene_groups:
                group_name = str(group_name).strip()
                if not group_name:
                    continue
                group_id = self._get_or_create_gene_group(
                    GeneGroup, gene_group_cache, group_name
                )
                exists = (
                    self.session.query(GeneGroupMembership)
                    .filter_by(gene_id=gm.id, group_id=group_id)
                    .one_or_none()
                )
                if exists is None:
                    self.session.add(GeneGroupMembership(
                        gene_id=gm.id,
                        group_id=group_id,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    ))
                    memberships_created += 1

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    self.logger.log(
                        f"[{self.DTP_NAME}] Committed batch {i + 1}/{total}",
                        "DEBUG",
                    )
                except Exception as e:
                    self.session.rollback()
                    return False, f"Batch commit failed at row {i + 1}: {e}"

        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            return False, f"Final commit failed: {e}"

        self._log_trunc_summary()
        stats = ETLStepStats(
            total=total,
            created=created,
            updated=updated,
            warnings=warnings,
            extras={
                "gene_masters_created": gene_masters_created,
                "gene_masters_updated": gene_masters_updated,
                "locus_groups_created": len(locus_group_cache) - locus_groups_before,
                "locus_types_created": len(locus_type_cache) - locus_types_before,
                "gene_groups_created": len(gene_group_cache) - gene_groups_before,
                "memberships_created": memberships_created,
                "truncations": self.trunc_metrics.copy(),
            },
        )
        msg = (
            f"[{self.DTP_NAME}] Load complete: "
            f"total={total} created={created} updated={updated} warnings={warnings}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _parse_chromosome(location) -> Optional[int]:
        """Parse HGNC location string (e.g. '12p13.31', 'Xq28') → integer 1-25."""
        if not location or (isinstance(location, float)):
            return None
        loc = str(location).strip().lower()
        if not loc or loc in ("nan", "reserved", ""):
            return None
        if "mitochondria" in loc or loc == "mt":
            return 25
        if loc.startswith("x"):
            return 23
        if loc.startswith("y"):
            return 24
        m = re.match(r"^(\d+)", loc)
        if m:
            n = int(m.group(1))
            return n if 1 <= n <= 22 else None
        return None

    def _get_or_create_locus(
        self, model_class, cache: dict, name
    ) -> Optional[int]:
        """Get or create a GeneLocusGroup/GeneLocusType row via in-memory cache."""
        if not name or isinstance(name, float):
            return None
        name = str(name).strip()
        if not name or name == "nan":
            return None
        if name in cache:
            return cache[name]
        obj = (
            self.session.query(model_class).filter_by(name=name).one_or_none()
        )
        if obj is None:
            obj = model_class(
                name=name,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(obj)
            self.session.flush()
        cache[name] = obj.id
        return obj.id

    def _get_or_create_gene_group(
        self, model_class, cache: dict, name: str
    ) -> int:
        """Get or create a GeneGroup row via in-memory cache."""
        if name in cache:
            return cache[name]
        obj = (
            self.session.query(model_class).filter_by(name=name).one_or_none()
        )
        if obj is None:
            obj = model_class(
                name=name,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(obj)
            self.session.flush()
        cache[name] = obj.id
        return obj.id
