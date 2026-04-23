"""
CTD Gene-Disease DTP.

Pipeline role:
- Pure relationship DTP — no master data.
- Runs AFTER dtp_gene_hgnc (Genes) and dtp_disease_mondo (Diseases).
- Downloads CTD_genes_diseases.tsv.gz directly from CTD.

Source:
- https://ctdbase.org/reports/CTD_genes_diseases.tsv.gz
  Columns: GeneSymbol, GeneID, DiseaseName, DiseaseID, DirectEvidence,
           InferenceChemicalName, InferenceScore, OmimIDs, PubMedIDs

What is loaded:
- EntityRelationship (Gene → Disease), curated direct evidence only.
  Inference-only rows (DirectEvidence empty) are excluded.

  DirectEvidence mapping:
    marker/mechanism → associated_with
    therapeutic      → responds_to

  Gene resolved by NCBI GeneID (alias_type=code, xref_source=NCBI).
  Disease resolved by MESH:Dxxxxxx or OMIM:xxxxxx (xref_source=MESH/OMIM).
  Unresolved pairs are written to skipped_relationships.csv.
"""

from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_FILE = "CTD_genes_diseases.tsv.gz"

# CTD column order (the header line is a comment, so we provide names)
_COLS = [
    "GeneSymbol", "GeneID", "DiseaseName", "DiseaseID",
    "DirectEvidence", "InferenceChemicalName", "InferenceScore",
    "OmimIDs", "PubMedIDs",
]

_EVIDENCE_MAP = {
    "marker/mechanism": "associated_with",
    "therapeutic":      "responds_to",
}


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "relationship_ctd_gene_disease"
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
            return True, msg, file_hash, ETLStepStats(file_size_bytes=file_size)
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
                "Parsing CTD_genes_diseases.tsv.gz (stream)...", "INFO"
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
                # Direct evidence only (excludes inference-only rows)
                mask = (
                    chunk["DirectEvidence"].notna()
                    & chunk["DirectEvidence"].ne("")
                    & chunk["DirectEvidence"].ne("nan")
                )
                chunk = chunk[mask]
                # Valid DiseaseID (MESH or OMIM)
                chunk = chunk[
                    chunk["DiseaseID"].notna()
                    & chunk["DiseaseID"].str.startswith(("MESH:", "OMIM:"))
                ]
                if not chunk.empty:
                    kept.append(
                        chunk[[
                            "GeneSymbol", "GeneID", "DiseaseID",
                            "DirectEvidence", "PubMedIDs",
                        ]]
                    )

            if kept:
                df = pd.concat(kept, ignore_index=True)
            else:
                df = pd.DataFrame(
                    columns=[
                        "GeneSymbol", "GeneID", "DiseaseID",
                        "DirectEvidence", "PubMedIDs",
                    ]
                )

            # Count raw occurrences per (gene, disease, evidence) before dedup
            ev_counts = (
                df.groupby(["GeneID", "DiseaseID", "DirectEvidence"])
                .size()
                .reset_index(name="evidence_count")
            )
            df = df.drop_duplicates(
                subset=["GeneID", "DiseaseID", "DirectEvidence"]
            )
            df = df.merge(
                ev_counts, on=["GeneID", "DiseaseID", "DirectEvidence"]
            )
            df["relation_type"] = df["DirectEvidence"].map(_EVIDENCE_MAP)
            df = df.dropna(subset=["relation_type"])

            df = df.rename(
                columns={"GeneID": "source_id", "DiseaseID": "target_id"}
            )
            df["source_type"] = "Genes"
            df["target_type"] = "Diseases"

            out = self._dtp_dir(processed_dir)
            rel_parquet = out / "relationship_data.parquet"
            df[[
                "source_id", "source_type", "target_id", "target_type",
                "relation_type", "evidence_count",
                "GeneSymbol", "DirectEvidence", "PubMedIDs",
            ]].to_parquet(rel_parquet, index=False)

            if self.debug_mode:
                df.to_csv(out / "relationship_data.csv", index=False)

            evidence_counts = (
                df["DirectEvidence"].value_counts().to_dict()
                if not df.empty else {}
            )
            ev_str = ", ".join(
                f"{k}:{v}" for k, v in sorted(evidence_counts.items())
            )
            self.logger.log(
                f"  evidence distribution: {ev_str}", "INFO"
            )

            stats = ETLStepStats(
                total=total_rows,
                output_size_bytes=rel_parquet.stat().st_size,
                extras={"direct_evidence_pairs": len(df)},
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{total_rows:,} raw rows → {len(df):,} direct-evidence pairs"
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
                f"relationship_data.parquet not found — "
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
            return True, "No CTD gene-disease rows to load.", ETLStepStats()

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

        gene_type_id = type_id_map.get("Genes")
        disease_type_id = type_id_map.get("Diseases")

        if gene_type_id is None:
            return (
                False, "EntityType 'Genes' not found.", ETLStepStats(errors=1)
            )
        if disease_type_id is None:
            return (
                False,
                "EntityType 'Diseases' not found.",
                ETLStepStats(errors=1),
            )

        # Gene map: NCBI GeneID (string) → entity_id
        self.logger.log("Pre-loading gene map (NCBI)...", "INFO")
        gene_map: dict[str, int] = {
            a.alias_value: a.entity_id
            for a in self.session.query(EntityAlias).filter_by(
                type_id=gene_type_id,
                alias_type="code",
                xref_source="NCBI",
            ).all()
        }

        # Disease map: "MESH:D015673" / "OMIM:12345" → entity_id
        self.logger.log("Pre-loading disease map (MESH + OMIM)...", "INFO")
        disease_map: dict[str, int] = {}
        for src in ("MESH", "OMIM"):
            for a in self.session.query(EntityAlias).filter_by(
                type_id=disease_type_id,
                alias_type="code",
                xref_source=src,
            ).all():
                disease_map[a.alias_value] = a.entity_id

        self.logger.log(
            f"  genes={len(gene_map):,} diseases={len(disease_map):,}",
            "INFO",
        )

        total = created = skipped = warnings = 0
        BATCH = 500
        skipped_rows: list[dict] = []

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1

            source_id = str(row.get("source_id") or "").strip()
            target_id = str(row.get("target_id") or "").strip()
            relation_type = str(row.get("relation_type") or "").strip()

            if not (source_id and target_id and relation_type):
                warnings += 1
                continue

            gene_entity_id = gene_map.get(source_id)
            if gene_entity_id is None:
                skipped += 1
                skipped_rows.append({
                    "source_id": source_id,
                    "target_id": target_id,
                    "relation_type": relation_type,
                    "gene_symbol": str(row.get("GeneSymbol") or ""),
                    "skip_reason": "gene_not_found",
                })
                continue

            disease_entity_id = disease_map.get(target_id)
            if disease_entity_id is None:
                skipped += 1
                skipped_rows.append({
                    "source_id": source_id,
                    "target_id": target_id,
                    "relation_type": relation_type,
                    "gene_symbol": str(row.get("GeneSymbol") or ""),
                    "skip_reason": "disease_not_found",
                })
                continue

            rel_type_id = rel_type_map.get(relation_type)
            if rel_type_id is None:
                warnings += 1
                continue

            ev_count = int(row.get("evidence_count") or 1)
            self.get_or_create_relationship(
                entity_1_id=gene_entity_id,
                entity_2_id=disease_entity_id,
                relationship_type_id=rel_type_id,
                data_source_id=self.data_source.id,
                entity_1_type_id=gene_type_id,
                entity_2_type_id=disease_type_id,
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
