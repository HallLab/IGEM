"""
CTD Chemical-Gene Interaction DTP.

Pipeline role:
- Pure relationship DTP — no master data.
- Runs AFTER dtp_chemical_chebi (Chemicals) and dtp_gene_hgnc (Genes).
- Downloads CTD_chem_gene_ixns.tsv.gz directly from CTD.

Source:
- https://ctdbase.org/reports/CTD_chem_gene_ixns.tsv.gz
  Columns: ChemicalName, ChemicalID, CasRN, GeneSymbol, GeneID, GeneForms,
           Organism, OrganismID, Interaction, InteractionActions, PubMedIDs

What is loaded:
- EntityRelationship (Chemical → Gene, relation_type=regulates)
  Filtered to human interactions only (OrganismID = 9606).
  Deduplicated on (ChemicalID, GeneID).

  Chemical resolved by MESH ID from CTD (e.g. MESH:C007309).
    Lookup tries both "mesh:c007309" and "c007309" as alias_norm, since
    ChEBI stores MESH accessions without prefix.
  Gene resolved by NCBI GeneID (alias_type=code, xref_source=NCBI).
  Unresolved pairs are written to skipped_relationships.csv.
"""

from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_FILE = "CTD_chem_gene_ixns.tsv.gz"

# CTD column order (header line is a comment, so we provide names)
_COLS = [
    "ChemicalName", "ChemicalID", "CasRN",
    "GeneSymbol", "GeneID", "GeneForms",
    "Organism", "OrganismID",
    "Interaction", "InteractionActions", "PubMedIDs",
]

_HUMAN_ORG_ID = "9606"


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "relationship_ctd_chem_gene"
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
            dest = self._dtp_dir(raw_dir) / _FILE
            url = self.data_source.source_url
            self.logger.log(f"Downloading {url}", "INFO")
            with requests.get(url, stream=True, timeout=600) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            fh.write(chunk)
            file_hash = self._hash_file(dest)
            file_size = dest.stat().st_size
            msg = (
                f"[{self.DTP_NAME}] {_FILE} downloaded ({file_size:,} bytes)"
            )
            self.logger.log(msg, "INFO")
            return (
                True, msg, file_hash, ETLStepStats(file_size_bytes=file_size)
            )
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

            self.logger.log(
                "Parsing CTD_chem_gene_ixns.tsv.gz (stream, human only)...",
                "INFO",
            )
            kept = []
            total_rows = 0

            for chunk in pd.read_csv(
                input_file,
                sep="\t",
                comment="#",
                header=None,
                names=_COLS,
                dtype=str,
                chunksize=100_000,
            ):
                total_rows += len(chunk)
                # Human only
                chunk = chunk[chunk["OrganismID"] == _HUMAN_ORG_ID]
                # Valid ChemicalID and GeneID
                # CTD chem IDs have no prefix ("C534883", not "MESH:C534883")
                chunk = chunk[
                    chunk["ChemicalID"].notna()
                    & chunk["GeneID"].notna()
                ]
                if not chunk.empty:
                    kept.append(
                        chunk[[
                            "ChemicalName", "ChemicalID", "GeneSymbol",
                            "GeneID", "InteractionActions", "PubMedIDs",
                        ]]
                    )

            if kept:
                df = pd.concat(kept, ignore_index=True)
            else:
                df = pd.DataFrame(
                    columns=[
                        "ChemicalName", "ChemicalID", "GeneSymbol",
                        "GeneID", "InteractionActions", "PubMedIDs",
                    ]
                )

            # Count raw interaction records per pair before dedup
            ev_counts = (
                df.groupby(["ChemicalID", "GeneID"])
                .size()
                .reset_index(name="evidence_count")
            )
            df = df.drop_duplicates(subset=["ChemicalID", "GeneID"])
            df = df.merge(ev_counts, on=["ChemicalID", "GeneID"])
            df["relation_type"] = "regulates"

            df = df.rename(
                columns={
                    "ChemicalID": "source_id",
                    "GeneID": "target_id",
                }
            )
            df["source_type"] = "Chemicals"
            df["target_type"] = "Genes"

            out = self._dtp_dir(processed_dir)
            rel_parquet = out / "relationship_data.parquet"
            df[[
                "source_id", "source_type", "target_id", "target_type",
                "relation_type", "evidence_count",
                "ChemicalName", "GeneSymbol",
                "InteractionActions", "PubMedIDs",
            ]].to_parquet(rel_parquet, index=False)

            if self.debug_mode:
                df.to_csv(out / "relationship_data.csv", index=False)

            stats = ETLStepStats(
                total=total_rows,
                output_size_bytes=rel_parquet.stat().st_size,
                extras={"human_pairs": len(df)},
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{total_rows:,} raw rows → {len(df):,} unique "
                f"human Chemical×Gene pairs"
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
                "relationship_data.parquet not found — "
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
            return (
                True, "No CTD chem-gene rows to load.", ETLStepStats()
            )

        from igem_backend.modules.db.models.model_entities import (
            EntityAlias,
            EntityRelationshipType,
            EntityType,
        )

        type_id_map: dict[str, int] = {
            et.name: et.id for et in self.session.query(EntityType).all()
        }
        rel_type_map: dict[str, int] = {
            rt.code: rt.id
            for rt in self.session.query(EntityRelationshipType).all()
        }

        chem_type_id = type_id_map.get("Chemicals")
        gene_type_id = type_id_map.get("Genes")
        rel_type_id = rel_type_map.get("regulates")

        if chem_type_id is None:
            return (
                False,
                "EntityType 'Chemicals' not found.",
                ETLStepStats(errors=1),
            )
        if gene_type_id is None:
            return (
                False, "EntityType 'Genes' not found.", ETLStepStats(errors=1)
            )
        if rel_type_id is None:
            return (
                False,
                "EntityRelationshipType 'regulates' not found.",
                ETLStepStats(errors=1),
            )

        # Chemical map: alias_norm → entity_id (all code aliases)
        # CTD uses "MESH:C007309"; ChEBI may store "C007309" without prefix.
        # We try both alias_norm forms for each lookup.
        self.logger.log(
            "Pre-loading chemical map (all code aliases)...", "INFO"
        )
        chem_map: dict[str, int] = {
            a.alias_norm: a.entity_id
            for a in self.session.query(EntityAlias).filter_by(
                type_id=chem_type_id,
                alias_type="code",
            ).all()
            if a.alias_norm
        }

        def _resolve_chemical(ctd_id: str) -> Optional[int]:
            norm = ctd_id.lower()
            # Try bare form ("c534883")
            hit = chem_map.get(norm)
            if hit is not None:
                return hit
            # IGEM stores as CURIE ("mesh:c534883") — try with prefix
            hit = chem_map.get(f"mesh:{norm}")
            if hit is not None:
                return hit
            # If input already has a prefix, also try without it
            if ":" in norm:
                return chem_map.get(norm.split(":", 1)[1])
            return None

        # Gene map: NCBI GeneID string → entity_id
        self.logger.log("Pre-loading gene map (NCBI)...", "INFO")
        gene_map: dict[str, int] = {
            a.alias_value: a.entity_id
            for a in self.session.query(EntityAlias).filter_by(
                type_id=gene_type_id,
                alias_type="code",
                xref_source="NCBI",
            ).all()
        }

        self.logger.log(
            f"  chemicals={len(chem_map):,} genes={len(gene_map):,}",
            "INFO",
        )

        total = created = skipped = warnings = 0
        BATCH = 500
        skipped_rows: list[dict] = []

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1

            source_id = str(row.get("source_id") or "").strip()
            target_id = str(row.get("target_id") or "").strip()

            if not (source_id and target_id):
                warnings += 1
                continue

            chem_entity_id = _resolve_chemical(source_id)
            if chem_entity_id is None:
                skipped += 1
                skipped_rows.append({
                    "source_id": source_id,
                    "target_id": target_id,
                    "chemical_name": str(row.get("ChemicalName") or ""),
                    "gene_symbol": str(row.get("GeneSymbol") or ""),
                    "skip_reason": "chemical_not_found",
                })
                continue

            gene_entity_id = gene_map.get(target_id)
            if gene_entity_id is None:
                skipped += 1
                skipped_rows.append({
                    "source_id": source_id,
                    "target_id": target_id,
                    "chemical_name": str(row.get("ChemicalName") or ""),
                    "gene_symbol": str(row.get("GeneSymbol") or ""),
                    "skip_reason": "gene_not_found",
                })
                continue

            ev_count = int(row.get("evidence_count") or 1)
            self.get_or_create_relationship(
                entity_1_id=chem_entity_id,
                entity_2_id=gene_entity_id,
                relationship_type_id=rel_type_id,
                data_source_id=self.data_source.id,
                entity_1_type_id=chem_type_id,
                entity_2_type_id=gene_type_id,
                package_id=self.package.id,
                evidence_count=ev_count,
                auto_commit=False,
            )
            created += 1

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    self.logger.log(
                        f"[{self.DTP_NAME}] Committed batch "
                        f"{i + 1}/{total}",
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
            skip_file = (
                self._dtp_dir(processed_dir) / "skipped_relationships.csv"
            )
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
