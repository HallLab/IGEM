"""
Reactome Pathway DTP.

Pipeline role:
- Master DTP for human Reactome pathways.
- Must run BEFORE dtp_pathway_reactome_relationships.
- No dependency on Gene/Protein DTPs (pathway master records are standalone).

What is extracted (5 files from Reactome download server):
- ReactomePathways.txt          → pathway name index (filtered to Homo sapiens)
- ReactomePathwaysRelation.txt  → parent-child pathway hierarchy
- ReactomePathways.gmt.zip      → gene symbol membership per pathway
- Ensembl2Reactome.txt          → Ensembl gene/protein ↔ pathway
- UniProt2Reactome.txt          → UniProt protein ↔ pathway

What is transformed:
- master_data.parquet       → one row per human Reactome pathway
- relationship_data.parquet → consumed by dtp_pathway_reactome_relationships

What is loaded (master_data only):
- Entity (type=Pathways) + EntityAlias (code=reactome_id, preferred=pathway_name)
- PathwayMaster with source_db="Reactome"
"""

import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_FILES = [
    "ReactomePathways.txt",
    "ReactomePathwaysRelation.txt",
    "ReactomePathways.gmt.zip",
    "Ensembl2Reactome.txt",
    "UniProt2Reactome.txt",
]
_HUMAN = "Homo sapiens"


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_pathway_reactome"
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
            base_url = self.data_source.source_url
            if not base_url.endswith("/"):
                base_url += "/"

            total_bytes = 0
            for fname in _FILES:
                url = base_url + fname
                self.logger.log(f"Downloading {fname} ...", "INFO")
                resp = requests.get(url, stream=True, timeout=300)
                resp.raise_for_status()
                dest = landing / fname
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            fh.write(chunk)
                total_bytes += dest.stat().st_size
                self.logger.log(f"  {dest.stat().st_size:,} bytes", "DEBUG")

            ref_file = landing / _FILES[0]
            file_hash = self._hash_file(ref_file)
            stats = ETLStepStats(file_size_bytes=total_bytes)
            msg = f"[{self.DTP_NAME}] {len(_FILES)} files downloaded ({total_bytes:,} bytes)"
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
            raw = self._dtp_dir(raw_dir)
            out = self._dtp_dir(processed_dir)

            # ---- Master data: ReactomePathways.txt ----
            pathways_file = raw / "ReactomePathways.txt"
            if not pathways_file.exists():
                return (
                    False,
                    f"Missing {pathways_file}",
                    ETLStepStats(errors=1),
                )

            df_master = pd.read_csv(
                pathways_file,
                sep="\t",
                header=None,
                names=["reactome_id", "pathway_name", "species"],
                dtype=str,
                na_filter=False,
            )
            df_master = df_master[df_master["species"] == _HUMAN].copy()
            df_master = df_master.drop(columns=["species"])
            valid_ids: set[str] = set(df_master["reactome_id"])

            master_parquet = out / "master_data.parquet"
            df_master.to_parquet(master_parquet, index=False)
            if self.debug_mode:
                df_master.to_csv(out / "master_data.csv", index=False)
            self.logger.log(
                f"  master_data: {len(df_master)} human pathways", "INFO"
            )

            # ---- Relationship records ----
            records: list[dict] = []

            # 1. Pathway hierarchy (parent→child is reversed: child part_of parent)
            rel_file = raw / "ReactomePathwaysRelation.txt"
            if rel_file.exists():
                with open(rel_file, encoding="utf-8") as fh:
                    for line in fh:
                        if line.startswith("#"):
                            continue
                        parts = line.rstrip("\n").split("\t")
                        if len(parts) < 2:
                            continue
                        parent_id, child_id = parts[0].strip(), parts[1].strip()
                        if parent_id in valid_ids and child_id in valid_ids:
                            records.append({
                                "source_id": child_id,
                                "target_id": parent_id,
                                "source_type": "Pathways",
                                "target_type": "Pathways",
                                "relation_type": "part_of",
                            })

            # 2. Gene symbols from GMT file
            gmt_zip = raw / "ReactomePathways.gmt.zip"
            name_to_id = (
                df_master.set_index("pathway_name")["reactome_id"].to_dict()
            )
            if gmt_zip.exists():
                with zipfile.ZipFile(gmt_zip, "r") as zf:
                    for info in zf.infolist():
                        if not info.filename.endswith(".gmt"):
                            continue
                        with zf.open(info.filename) as fh:
                            for line in fh:
                                parts = line.decode("utf-8").rstrip("\n").split("\t")
                                if len(parts) < 3:
                                    continue
                                rid = name_to_id.get(parts[0])
                                if rid is None:
                                    continue
                                for sym in parts[2:]:
                                    sym = sym.strip()
                                    if sym:
                                        records.append({
                                            "source_id": rid,
                                            "target_id": sym,
                                            "source_type": "Pathways",
                                            "target_type": "Genes",
                                            "relation_type": "in_pathway",
                                        })

            # 3. Ensembl IDs (ENSG → Genes, ENSP → Proteins)
            ensembl_file = raw / "Ensembl2Reactome.txt"
            if ensembl_file.exists():
                with open(ensembl_file, encoding="utf-8") as fh:
                    for line in fh:
                        if line.startswith("#"):
                            continue
                        parts = line.rstrip("\n").split("\t")
                        if len(parts) < 6:
                            continue
                        enid, rid = parts[0].strip(), parts[1].strip()
                        species = parts[5].strip()
                        if species != _HUMAN or rid not in valid_ids:
                            continue
                        if enid.startswith("ENSG"):
                            ttype = "Genes"
                        elif enid.startswith("ENSP"):
                            ttype = "Proteins"
                        else:
                            continue
                        records.append({
                            "source_id": rid,
                            "target_id": enid,
                            "source_type": "Pathways",
                            "target_type": ttype,
                            "relation_type": "in_pathway",
                        })

            # 4. UniProt proteins
            uniprot_file = raw / "UniProt2Reactome.txt"
            if uniprot_file.exists():
                with open(uniprot_file, encoding="utf-8") as fh:
                    for line in fh:
                        if line.startswith("#"):
                            continue
                        parts = line.rstrip("\n").split("\t")
                        if len(parts) < 6:
                            continue
                        uid, rid = parts[0].strip(), parts[1].strip()
                        species = parts[5].strip()
                        if species != _HUMAN or rid not in valid_ids:
                            continue
                        records.append({
                            "source_id": rid,
                            "target_id": uid,
                            "source_type": "Pathways",
                            "target_type": "Proteins",
                            "relation_type": "in_pathway",
                        })

            df_rel = pd.DataFrame(
                records,
                columns=["source_id", "target_id", "source_type", "target_type", "relation_type"],
            )
            rel_parquet = out / "relationship_data.parquet"
            df_rel.to_parquet(rel_parquet, index=False)
            if self.debug_mode:
                df_rel.to_csv(out / "relationship_data.csv", index=False)
            self.logger.log(
                f"  relationship_data: {len(df_rel)} rows", "INFO"
            )

            stats = ETLStepStats(
                total=len(df_master),
                columns=len(df_master.columns),
                output_size_bytes=master_parquet.stat().st_size,
                extras={"relationship_rows": len(df_rel)},
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df_master)} pathways, {len(df_rel)} relationship rows"
            )
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
            return True, "No pathway rows to load.", ETLStepStats()

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
            reactome_id = str(row.get("reactome_id") or "").strip()
            pathway_name = str(row.get("pathway_name") or "").strip()

            if not reactome_id:
                warnings += 1
                continue

            if reactome_id in existing:
                skipped += 1
                continue

            entity_id, _ = self.get_or_create_entity(
                name=reactome_id,
                type_id=pathway_type_id,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                alias_type="code",
                xref_source="REACTOME",
                alias_norm=reactome_id.lower(),
                auto_commit=False,
            )
            if entity_id is None:
                warnings += 1
                continue

            if pathway_name:
                self.add_aliases(
                    entity_id=entity_id,
                    type_id=pathway_type_id,
                    aliases=[{
                        "alias_value": pathway_name,
                        "alias_type": "preferred",
                        "xref_source": "REACTOME",
                        "alias_norm": self._normalize(pathway_name),
                        "locale": "en",
                    }],
                    data_source_id=self.data_source.id,
                    package_id=self.package.id,
                    auto_commit=False,
                )

            pm = PathwayMaster(
                entity_id=entity_id,
                pathway_id=reactome_id,
                description=self.guard_short(pathway_name),
                source_db="Reactome",
                organism=_HUMAN,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(pm)
            existing[reactome_id] = -1
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
