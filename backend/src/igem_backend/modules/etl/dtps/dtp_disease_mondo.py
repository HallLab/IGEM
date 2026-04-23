"""
MONDO Disease DTP.

Pipeline role:
- Master DTP for human disease entities from the Monarch Disease Ontology.
- Must run BEFORE dtp_disease_mondo_relationships.
- No dependency on Gene/Protein DTPs.

Source:
- mondo.json (OBOGraph JSON format) from https://purl.obolibrary.org/obo/mondo.json

What is transformed:
- master_data.parquet       → one row per active MONDO term (mondo_id, label,
                              description, synonyms, xrefs, subsets, omim_id,
                              mesh_id, icd10, orphanet_id)
- relationship_data.parquet → MONDO→MONDO is_a / part_of DAG edges consumed
                              by dtp_disease_mondo_relationships

What is loaded (master_data only):
Phase 1 — DiseaseGroup entries from MONDO subset tags (get-or-create)

Phase 2 — per active term:
  Entity (type=Diseases) + EntityAlias:
    - code/MONDO     → MONDO:xxxxxxx  (primary)
    - preferred/MONDO → label
    - synonym/MONDO  → each synonym
    - code/<PREFIX>  → OMIM:xxxxxx, MESH:Dxxxxxx, ICD10:Xxx, Orphanet:xxx, ...
  DiseaseMaster (disease_id=mondo_id, cross-ref columns populated)
  DiseaseGroupMembership links
"""

import json
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_FILE = "mondo.json"
_ROOT_ID = "MONDO:0000001"

# Disease→Disease predicates (both endpoints must be valid MONDO terms)
_PRED_MAP: dict[str, str] = {
    "is_a": "is_a",
    "http://purl.obolibrary.org/obo/BFO_0000050":  "part_of",  # part of
    "http://purl.obolibrary.org/obo/RO_0002473":   "part_of",  # part of (disease axis)
    "http://purl.obolibrary.org/obo/RO_0001025":   "part_of",  # located in
}

# Disease → cross-ontology predicates
_CROSS_PRED_MAP: dict[str, str] = {
    # Gene-causal relationships
    "http://purl.obolibrary.org/obo/RO_0004003": "has_genetic_basis",  # gain-of-function germline mutation
    "http://purl.obolibrary.org/obo/RO_0004001": "has_genetic_basis",  # loss-of-function germline mutation
    "http://purl.obolibrary.org/obo/RO_0004004": "has_genetic_basis",  # somatic mutation
    "http://purl.obolibrary.org/obo/RO_0004020": "has_genetic_basis",  # disease has basis in dysfunction of
    # GO process / function disruption
    "http://purl.obolibrary.org/obo/RO_0004021": "has_disruption_in",  # disease has basis in disruption of
    "http://purl.obolibrary.org/obo/RO_0004024": "has_disruption_in",  # disease has basis in dysfunction of process
    # Phenotype associations (HP)
    "http://purl.obolibrary.org/obo/RO_0004029":                "has_phenotype",
    "http://purl.obolibrary.org/obo/RO_0000053":                "has_phenotype",  # has characteristic
    "http://purl.obolibrary.org/obo/mondo#disease_has_major_feature": "has_phenotype",
    "http://purl.obolibrary.org/obo/RO_0002573":                "has_phenotype",  # has modifier
    # Anatomical location (UBERON)
    "http://purl.obolibrary.org/obo/RO_0004026": "has_location",  # disease has location
    "http://purl.obolibrary.org/obo/RO_0004027": "has_location",  # disease has basis in development of
    "http://purl.obolibrary.org/obo/RO_0004030": "has_location",  # disease has basis in dysfunction of cell type
    # Chemical response (ChEBI)
    "http://purl.obolibrary.org/obo/RO_0004028":                    "responds_to",
    "http://purl.obolibrary.org/obo/mondo#disease_responds_to":      "responds_to",
}

# Target IRI → (normalized_id, entity_type_name)
def _normalize_cross_target(obj_iri: str) -> Optional[tuple[str, str]]:
    """
    Normalises a cross-ontology object IRI to (target_id, target_type).
    Returns None for unrecognised namespaces (silently skipped at load time).
    """
    if not obj_iri:
        return None

    # HGNC gene: http://identifiers.org/hgnc/28403 → HGNC:28403
    if "identifiers.org/hgnc/" in obj_iri:
        num = obj_iri.rsplit("/", 1)[-1]
        if num.isdigit():
            return f"HGNC:{num}", "Genes"

    # NCBIGene: http://identifiers.org/ncbigene/xxx → NCBIGene:xxx
    if "identifiers.org/ncbigene/" in obj_iri:
        num = obj_iri.rsplit("/", 1)[-1]
        if num.isdigit():
            return f"NCBIGene:{num}", "Genes"

    local = obj_iri.rsplit("/", 1)[-1]
    if local.startswith("HP_"):
        return local.replace("_", ":", 1), "Phenotypes"
    if local.startswith("UBERON_"):
        return local.replace("_", ":", 1), "Anatomy"
    if local.startswith("GO_"):
        return local.replace("_", ":", 1), "Gene Ontology"
    if local.startswith("CHEBI_"):
        return local.replace("_", ":", 1), "Chemicals"

    return None


# xref prefix → DiseaseMaster column name (first match wins per column)
_XREF_COL: dict[str, str] = {
    "OMIM":      "omim_id",
    "MESH":      "mesh_id",
    "MSH":       "mesh_id",
    "ICD10":     "icd10",
    "ICD10CM":   "icd10",
    "ORPHANET":  "orphanet_id",
    "ORPHA":     "orphanet_id",
}

_SUBSET_PREFIX = "http://purl.obolibrary.org/obo/mondo#"


def _iri_to_mondo(iri: str) -> Optional[str]:
    """http://purl.obolibrary.org/obo/MONDO_0000001 → MONDO:0000001"""
    if iri and "MONDO_" in iri:
        local = iri.rsplit("/", 1)[-1]
        return local.replace("_", ":", 1)
    return None


def _parse_xrefs(xrefs: list[str]) -> tuple[dict[str, Optional[str]], list[dict]]:
    """
    Returns (col_values, alias_list).
    col_values: {"omim_id": ..., "mesh_id": ..., "icd10": ..., "orphanet_id": ...}
    alias_list: [{alias_value, xref_source}, ...] for all parseable xrefs
    """
    cols: dict[str, Optional[str]] = {
        "omim_id": None, "mesh_id": None, "icd10": None, "orphanet_id": None
    }
    aliases: list[dict] = []
    for x in xrefs:
        if not x or ":" not in x:
            continue
        prefix, code = x.split(":", 1)
        prefix_up = prefix.upper()
        aliases.append({"alias_value": x, "xref_source": prefix_up})
        col = _XREF_COL.get(prefix_up) or _XREF_COL.get(prefix)
        if col and not cols[col]:
            cols[col] = x  # store as PREFIX:code
    return cols, aliases


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_disease_mondo"
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

            self.logger.log(f"Streaming MONDO JSON from {url}", "INFO")
            with requests.get(url, stream=True, timeout=600) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            fh.write(chunk)

            file_hash = self._hash_file(dest)
            file_size = dest.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = f"[{self.DTP_NAME}] mondo.json downloaded ({file_size:,} bytes)"
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

            self.logger.log("Loading MONDO JSON...", "INFO")
            with open(input_file, encoding="utf-8") as fh:
                mondo = json.load(fh)

            graph = mondo.get("graphs", [{}])[0]
            nodes = graph.get("nodes", [])
            edges = graph.get("edges", [])

            # ---- Master records ----
            master_records: list[dict] = []
            valid_ids: set[str] = set()

            for n in nodes:
                raw_id = n.get("id", "")
                mondo_id = _iri_to_mondo(raw_id)
                if not mondo_id or mondo_id == _ROOT_ID:
                    continue

                meta = n.get("meta", {})
                if meta.get("deprecated", False):
                    continue

                label = n.get("lbl") or ""
                if not label:
                    continue

                definition = ""
                def_block = meta.get("definition")
                if isinstance(def_block, dict):
                    definition = def_block.get("val", "")

                synonyms = [
                    s["val"]
                    for s in meta.get("synonyms", [])
                    if s.get("val")
                ]

                xref_vals = [x["val"] for x in meta.get("xrefs", []) if x.get("val")]
                xref_cols, _ = _parse_xrefs(xref_vals)

                subsets = [
                    s.replace(_SUBSET_PREFIX, "")
                    for s in meta.get("subsets", [])
                    if isinstance(s, str)
                ]

                master_records.append({
                    "mondo_id":    mondo_id,
                    "label":       label,
                    "description": definition,
                    "synonyms":    json.dumps(synonyms),
                    "xrefs":       json.dumps(xref_vals),
                    "subsets":     json.dumps(subsets),
                    "omim_id":     xref_cols["omim_id"],
                    "mesh_id":     xref_cols["mesh_id"],
                    "icd10":       xref_cols["icd10"],
                    "orphanet_id": xref_cols["orphanet_id"],
                })
                valid_ids.add(mondo_id)

            df_master = pd.DataFrame(master_records)
            out = self._dtp_dir(processed_dir)
            master_parquet = out / "master_data.parquet"
            df_master.to_parquet(master_parquet, index=False)
            if self.debug_mode:
                df_master.to_csv(out / "master_data.csv", index=False)
            self.logger.log(
                f"  master_data: {len(df_master)} active MONDO terms", "INFO"
            )

            # ---- Relationship records ----
            rel_records: list[dict] = []
            cross_counts: dict[str, int] = {}

            for e in edges:
                pred = e.get("pred", "")
                sub_iri = e.get("sub", "")
                obj_iri = e.get("obj", "")

                src = _iri_to_mondo(sub_iri)
                if not src or src not in valid_ids:
                    continue  # subject must be a loaded MONDO disease

                # Disease → Disease (is_a / part_of)
                rel_type = _PRED_MAP.get(pred)
                if rel_type:
                    tgt = _iri_to_mondo(obj_iri)
                    if tgt and tgt in valid_ids:
                        rel_records.append({
                            "source_id":     src,
                            "target_id":     tgt,
                            "source_type":   "Diseases",
                            "target_type":   "Diseases",
                            "relation_type": rel_type,
                        })
                    continue

                # Disease → cross-ontology (Gene, GO, HP, UBERON, ChEBI)
                cross_rel = _CROSS_PRED_MAP.get(pred)
                if not cross_rel:
                    continue

                target_info = _normalize_cross_target(obj_iri)
                if not target_info:
                    continue

                target_id, target_type = target_info
                rel_records.append({
                    "source_id":     src,
                    "target_id":     target_id,
                    "source_type":   "Diseases",
                    "target_type":   target_type,
                    "relation_type": cross_rel,
                })
                cross_counts[target_type] = cross_counts.get(target_type, 0) + 1

            df_rel = pd.DataFrame(
                rel_records,
                columns=["source_id", "target_id", "source_type",
                         "target_type", "relation_type"],
            )
            rel_parquet = out / "relationship_data.parquet"
            df_rel.to_parquet(rel_parquet, index=False)
            if self.debug_mode:
                df_rel.to_csv(out / "relationship_data.csv", index=False)

            disease_disease = sum(1 for r in rel_records if r["target_type"] == "Diseases")
            cross_summary = ", ".join(f"{t}:{c}" for t, c in sorted(cross_counts.items()))
            self.logger.log(
                f"  relationship_data: {len(df_rel)} total "
                f"({disease_disease} disease-disease; {cross_summary})",
                "INFO",
            )

            stats = ETLStepStats(
                total=len(df_master),
                columns=len(df_master.columns),
                output_size_bytes=master_parquet.stat().st_size,
                extras={
                    "relationship_rows": len(df_rel),
                    "disease_disease_edges": disease_disease,
                    "cross_ontology_edges": len(df_rel) - disease_disease,
                },
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df_master)} terms, {len(df_rel)} relationship edges"
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
            return True, "No disease rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_diseases import (
            DiseaseGroup,
            DiseaseGroupMembership,
            DiseaseMaster,
        )

        disease_type_id = self.get_entity_type_id("Diseases")

        # ---- Phase 1: DiseaseGroup entries from subsets ----
        self.logger.log(f"[{self.DTP_NAME}] Building DiseaseGroup map...", "INFO")
        all_subsets: set[str] = set()
        for raw in df["subsets"].dropna():
            for s in json.loads(raw):
                if s:
                    all_subsets.add(s)

        subset_map: dict[str, int] = {
            g.name: g.id
            for g in self.session.query(DiseaseGroup).all()
        }
        for subset in sorted(all_subsets):
            if subset not in subset_map:
                grp = DiseaseGroup(
                    name=subset,
                    description=f"MONDO subset: {subset}",
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(grp)
                self.session.flush()
                subset_map[subset] = grp.id
        self.session.commit()
        self.logger.log(
            f"  {len(subset_map)} DiseaseGroup entries ready", "INFO"
        )

        # ---- Phase 2: DiseaseMaster terms ----
        existing: dict[str, int] = {
            dm.disease_id: dm.id
            for dm in self.session.query(DiseaseMaster.disease_id, DiseaseMaster.id).all()
        }

        from igem_backend.modules.db.models.model_entities import Entity, EntityAlias

        total = created = skipped = warnings = 0
        # Larger batch is safe now — no per-entity SELECT overhead
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1
            mondo_id = str(row.get("mondo_id") or "").strip()
            label = str(row.get("label") or "").strip()

            if not mondo_id or not label:
                warnings += 1
                continue

            if mondo_id in existing:
                skipped += 1
                continue

            # --- Fast path: entity is definitely new (pre-checked via existing dict) ---
            # Bypass the mixin SELECTs and write directly to avoid 56k wasted round-trips.

            entity = Entity(
                type_id=disease_type_id,
                is_active=True,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(entity)
            self.session.flush()  # populate entity.id

            # Build all aliases for this term, deduplicated by (value, type, source)
            seen_keys: set[tuple[str, str, str]] = set()

            def _add_alias(
                val: str,
                atype: str,
                xsrc: str,
                norm: Optional[str],
                primary: bool = False,
            ) -> None:
                val = self.guard_alias(val.strip()) or ""
                if not val:
                    return
                key = (val, atype, xsrc)
                if key in seen_keys:
                    return
                seen_keys.add(key)
                self.session.add(EntityAlias(
                    entity_id=entity.id,
                    type_id=disease_type_id,
                    alias_value=val,
                    alias_type=atype,
                    xref_source=xsrc,
                    alias_norm=self.guard_alias_norm(norm or val),
                    is_primary=primary,
                    is_active=True,
                    locale="en",
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                ))

            # Primary alias
            _add_alias(mondo_id, "code", "MONDO", mondo_id.lower(), primary=True)
            # Label
            _add_alias(label, "preferred", "MONDO", self._normalize(label))
            # Synonyms
            for syn in json.loads(row.get("synonyms") or "[]"):
                if syn:
                    _add_alias(syn, "synonym", "MONDO", self._normalize(syn))
            # Xrefs
            for xref in json.loads(row.get("xrefs") or "[]"):
                if xref and ":" in xref:
                    prefix, _ = xref.split(":", 1)
                    _add_alias(xref, "code", prefix.upper(), xref.lower())

            # DiseaseMaster
            dm = DiseaseMaster(
                entity_id=entity.id,
                disease_id=mondo_id,
                label=self.guard_short(label),
                description=str(row.get("description") or "") or None,
                mondo_id=mondo_id,
                omim_id=self.guard_short(row.get("omim_id") or None),
                mesh_id=self.guard_short(row.get("mesh_id") or None),
                icd10=self.guard_short(row.get("icd10") or None),
                orphanet_id=self.guard_short(row.get("orphanet_id") or None),
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(dm)
            self.session.flush()  # populate dm.id for memberships
            existing[mondo_id] = dm.id
            created += 1

            # DiseaseGroupMembership
            for subset in json.loads(row.get("subsets") or "[]"):
                gid = subset_map.get(subset)
                if gid:
                    self.session.add(DiseaseGroupMembership(
                        disease_id=dm.id,
                        group_id=gid,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    ))

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
