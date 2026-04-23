"""
HPO Gene-Phenotype Relationship DTP.

Pipeline role:
- Runs AFTER dtp_phenotype_hpo (master).
- Loads Gene → HP phenotype associations from the HPO annotation file.
- Source file is independent from dtp_phenotype_hpo; has its own extract.

Source:
- genes_to_phenotype.txt from HPO HPOA annotations
  https://purl.obolibrary.org/obo/hp/hpoa/genes_to_phenotype.txt

  Columns: ncbi_gene_id, gene_symbol, hpo_id, hpo_name, frequency, disease_count

What is loaded:
- EntityRelationship (Gene → HP, relation_type=has_phenotype)
  Gene is resolved by symbol (alias_norm = lowercase symbol).
  HP is resolved by hp_id (alias_norm = lowercase hp_id).
  Both endpoints must exist; pairs not found are silently skipped.
  Gene is always entity_1 (enforced by get_or_create_relationship canonical ordering).
"""

from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_FILE = "genes_to_phenotype.txt"
_URL = "https://purl.obolibrary.org/obo/hp/hpoa/genes_to_phenotype.txt"


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_phenotype_hpo_genes"
    DTP_VERSION = "1.0.0"
    DTP_TYPE = "relationship"
    ROLLBACK_STRATEGY = "delete"

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

            self.logger.log(f"Downloading {url}", "INFO")
            with requests.get(url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            fh.write(chunk)

            file_hash = self._hash_file(dest)
            file_size = dest.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = (
                f"[{self.DTP_NAME}] genes_to_phenotype.txt downloaded "
                f"({file_size:,} bytes)"
            )
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

            # HPO annotation file has a comment header line starting with '#'
            df = pd.read_csv(
                input_file,
                sep="\t",
                comment="#",
                header=None,
                names=[
                    "ncbi_gene_id", "gene_symbol", "hpo_id",
                    "hpo_name", "frequency", "disease_count",
                ],
                dtype=str,
            )

            # Normalise
            df = df.dropna(subset=["gene_symbol", "hpo_id"])
            df["gene_symbol"] = df["gene_symbol"].str.strip()
            df["hpo_id"] = df["hpo_id"].str.strip()
            df["ncbi_gene_id"] = df["ncbi_gene_id"].str.strip()

            # Keep only HP: prefixed IDs
            df = df[df["hpo_id"].str.startswith("HP:")]

            # Deduplicate Gene×HP pairs (keep first occurrence)
            df = df.drop_duplicates(subset=["gene_symbol", "hpo_id"])

            out = self._dtp_dir(processed_dir)
            rel_parquet = out / "relationship_data.parquet"
            df[["ncbi_gene_id", "gene_symbol", "hpo_id"]].to_parquet(
                rel_parquet, index=False
            )
            if self.debug_mode:
                df[["ncbi_gene_id", "gene_symbol", "hpo_id"]].to_csv(
                    out / "relationship_data.csv", index=False
                )

            stats = ETLStepStats(
                total=len(df),
                columns=3,
                output_size_bytes=rel_parquet.stat().st_size,
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df):,} unique Gene×HP pairs"
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

        rel_file = self._dtp_dir(processed_dir) / "relationship_data.parquet"
        if not rel_file.exists():
            return (
                False,
                f"relationship_data.parquet not found at {rel_file} — "
                "run extract + transform first.",
                ETLStepStats(errors=1),
            )

        try:
            df = pd.read_parquet(rel_file, engine="pyarrow")
        except Exception as e:
            return (
                False, f"Could not read parquet: {e}", ETLStepStats(errors=1)
            )

        if df.empty:
            return True, "No Gene×HP rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_entities import (
            EntityAlias,
            EntityRelationshipType,
            EntityType,
        )

        # --- Lookup tables ---
        type_id_map: dict[str, int] = {
            et.name: et.id for et in self.session.query(EntityType).all()
        }
        rel_type_map: dict[str, int] = {
            rt.code: rt.id
            for rt in self.session.query(EntityRelationshipType).all()
        }

        gene_type_id = type_id_map.get("Genes")
        pheno_type_id = type_id_map.get("Phenotypes")
        rel_type_id = rel_type_map.get("has_phenotype")

        if gene_type_id is None:
            return (
                False, "EntityType 'Genes' not found.", ETLStepStats(errors=1)
            )
        if pheno_type_id is None:
            return (
                False,
                "EntityType 'Phenotypes' not found.",
                ETLStepStats(errors=1),
            )
        if rel_type_id is None:
            return (
                False,
                "EntityRelationshipType 'has_phenotype' not found — "
                "run db upgrade first.",
                ETLStepStats(errors=1),
            )

        # --- Pre-load gene symbol → entity_id (preferred alias, lowercase norm) ---
        gene_map: dict[str, int] = {
            a.alias_norm: a.entity_id
            for a in self.session.query(EntityAlias)
            .filter(
                EntityAlias.type_id == gene_type_id,
                EntityAlias.alias_type == "preferred",
            )
            .all()
        }

        # --- Lazy HP entity cache: hp_id.lower() → entity_id ---
        _hp_cache: dict[str, Optional[int]] = {}

        def _resolve_hp(hp_id: str) -> Optional[int]:
            norm = hp_id.lower()
            if norm not in _hp_cache:
                a = (
                    self.session.query(EntityAlias)
                    .filter(
                        EntityAlias.type_id == pheno_type_id,
                        EntityAlias.alias_norm == norm,
                    )
                    .first()
                )
                _hp_cache[norm] = a.entity_id if a else None
            return _hp_cache[norm]

        total = created = skipped = warnings = 0
        BATCH = 500
        skipped_rows: list[dict] = []

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1

            symbol = str(row.get("gene_symbol") or "").strip()
            hp_id = str(row.get("hpo_id") or "").strip()

            if not symbol or not hp_id:
                warnings += 1
                continue

            gene_entity_id = gene_map.get(symbol.lower())
            if gene_entity_id is None:
                skipped += 1
                skipped_rows.append({
                    "gene_symbol": symbol, "hp_id": hp_id,
                    "skip_reason": "gene_not_found",
                })
                continue

            hp_entity_id = _resolve_hp(hp_id)
            if hp_entity_id is None:
                skipped += 1
                skipped_rows.append({
                    "gene_symbol": symbol, "hp_id": hp_id,
                    "skip_reason": "hp_not_found",
                })
                continue

            self.get_or_create_relationship(
                entity_1_id=gene_entity_id,
                entity_2_id=hp_entity_id,
                relationship_type_id=rel_type_id,
                data_source_id=self.data_source.id,
                entity_1_type_id=gene_type_id,
                entity_2_type_id=pheno_type_id,
                package_id=self.package.id,
                auto_commit=False,
            )
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

        if skipped_rows:
            skip_file = self._dtp_dir(processed_dir) / "skipped_relationships.csv"
            pd.DataFrame(skipped_rows).to_csv(skip_file, index=False)
            self.logger.log(
                f"[{self.DTP_NAME}] {skipped} skipped rows written to "
                f"{skip_file}",
                "INFO",
            )

        stats = ETLStepStats(
            total=total,
            created=created,
            skipped=skipped,
            warnings=warnings,
        )
        msg = (
            f"[{self.DTP_NAME}] Load complete: "
            f"total={total} created={created} "
            f"skipped={skipped} warnings={warnings}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats
