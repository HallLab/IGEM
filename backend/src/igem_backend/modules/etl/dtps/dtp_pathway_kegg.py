"""
KEGG Pathway DTP.

Pipeline role:
- Standalone master DTP for KEGG human pathways.
- No relationship DTP — protein↔KEGG links come from dtp_protein_uniprot_relationships.

Source:
- KEGG REST API: https://rest.kegg.jp/list/pathway/hsa
- Returns tab-separated: path:hsaXXXXX\tdescription

What is loaded:
- Entity (type=Pathways) + EntityAlias (code=pathway_id, preferred=description)
- PathwayMaster with source_db="KEGG"
"""

from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_FILE = "kegg_pathways.txt"


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_pathway_kegg"
    DTP_VERSION = "1.0.0"
    DTP_TYPE = "master"
    ROLLBACK_STRATEGY = "deactivate"

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
            dest = landing / _FILE
            url = self.data_source.source_url

            self.logger.log(f"Fetching KEGG pathway list from {url}", "INFO")
            resp = requests.get(url, timeout=120, headers={"Accept": "text/plain"})
            resp.raise_for_status()

            with open(dest, "w", encoding="utf-8") as fh:
                fh.write(resp.text)

            file_hash = self._hash_file(dest)
            file_size = dest.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = f"[{self.DTP_NAME}] KEGG data downloaded ({file_size:,} bytes)"
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
            input_file = self._dtp_dir(raw_dir) / _FILE
            if not input_file.exists():
                return (
                    False,
                    f"Input file not found: {input_file}",
                    ETLStepStats(errors=1),
                )

            rows: list[tuple[str, str]] = []
            with open(input_file, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split("\t")
                    if len(parts) < 2:
                        continue
                    # Strip "path:" prefix → "hsa00010"
                    pid = parts[0].replace("path:", "").strip()
                    desc = parts[1].strip()
                    if pid:
                        rows.append((pid, desc))

            df = pd.DataFrame(rows, columns=["pathway_id", "description"])

            out = self._dtp_dir(processed_dir)
            out_parquet = out / "master_data.parquet"
            df.to_parquet(out_parquet, index=False)
            if self.debug_mode:
                df.to_csv(out / "master_data.csv", index=False)

            stats = ETLStepStats(
                total=len(df),
                columns=len(df.columns),
                output_size_bytes=out_parquet.stat().st_size,
            )
            msg = f"[{self.DTP_NAME}] Transform complete: {len(df)} KEGG pathways"
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
            return (
                False,
                f"Processed file not found: {parquet_file}",
                ETLStepStats(errors=1),
            )

        try:
            df = pd.read_parquet(parquet_file, engine="pyarrow")
        except Exception as e:
            return False, f"Could not read parquet: {e}", ETLStepStats(errors=1)

        if df.empty:
            return True, "No KEGG pathway rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_pathways import PathwayMaster

        pathway_type_id = self.get_entity_type_id("Pathways")

        existing: dict[str, int] = {
            pm.pathway_id: pm.id
            for pm in self.session.query(PathwayMaster).all()
        }

        total = created = skipped = warnings = 0
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1
            pathway_id = str(row.get("pathway_id") or "").strip()
            description = str(row.get("description") or "").strip()

            if not pathway_id:
                warnings += 1
                continue

            if pathway_id in existing:
                skipped += 1
                continue

            entity_id, _ = self.get_or_create_entity(
                name=pathway_id,
                type_id=pathway_type_id,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                alias_type="code",
                xref_source="KEGG",
                alias_norm=pathway_id.lower(),
                auto_commit=False,
            )
            if entity_id is None:
                warnings += 1
                continue

            if description:
                self.add_aliases(
                    entity_id=entity_id,
                    type_id=pathway_type_id,
                    aliases=[{
                        "alias_value": description,
                        "alias_type": "preferred",
                        "xref_source": "KEGG",
                        "alias_norm": self._normalize(description),
                        "locale": "en",
                    }],
                    data_source_id=self.data_source.id,
                    package_id=self.package.id,
                    auto_commit=False,
                )

            pm = PathwayMaster(
                entity_id=entity_id,
                pathway_id=pathway_id,
                description=self.guard_short(description),
                source_db="KEGG",
                organism="Homo sapiens",
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(pm)
            existing[pathway_id] = -1
            created += 1

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    self.logger.log(
                        f"[{self.DTP_NAME}] Committed batch {i + 1}/{total}",
                        "DEBUG",
                    )
                except Exception as e:
                    self.session.rollback()
                    return (
                        False,
                        f"Batch commit failed at row {i + 1}: {e}",
                        ETLStepStats(errors=1),
                    )

        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            return False, f"Final commit failed: {e}", ETLStepStats(errors=1)

        self._log_trunc_summary()
        stats = ETLStepStats(
            total=total,
            created=created,
            skipped=skipped,
            warnings=warnings,
        )
        msg = (
            f"[{self.DTP_NAME}] Load complete: "
            f"total={total} created={created} skipped={skipped} warnings={warnings}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats
