"""
Gene Ontology DTP.

Pipeline role:
- Standalone master DTP; no dependency on Gene or Protein DTPs.
- Must run BEFORE dtp_protein_uniprot_relationships and any gene→GO
  relationship DTPs so that GO term entities exist for resolution.

What is extracted:
- go.obo from the GO Consortium (OBO flat-file format)

What is transformed:
- master_data.parquet   → one row per active GO term (go_id, name, namespace,
                          definition, synonyms, alt_ids)
- relations_data.parquet → GO internal DAG (child → parent, relation_type)
                           covers is_a, part_of, regulates,
                           positively_regulates, negatively_regulates

What is loaded:
Phase 1 — GO terms:
  Entity (type="Gene Ontology") + EntityAlias (code=go_id, preferred=name,
  synonym=synonyms, code=alt_ids) + GOMaster

Phase 2 — GO DAG:
  GORelation rows (uses GOMaster.id, not entity_id)
  Loaded after all terms are committed so FK constraints are satisfied.

Note: Gene↔GO and Protein↔GO EntityRelationship rows are created by
dtp_protein_uniprot_relationships and gene-specific relationship DTPs,
not here.
"""

import re
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_FILE = "go.obo"

_NS_MAP = {
    "biological_process": "BP",
    "molecular_function": "MF",
    "cellular_component": "CC",
}

_RELATION_TYPES = {
    "is_a",
    "part_of",
    "regulates",
    "positively_regulates",
    "negatively_regulates",
}


def _parse_obo(path) -> tuple[list[dict], list[dict]]:
    """
    Stream-parse an OBO file and return (terms, relations).

    terms: one dict per active [Term] block
    relations: {child_go, parent_go, relation_type} — child IS_A/part_of parent
    """
    terms: list[dict] = []
    relations: list[dict] = []
    current: dict = {}

    def _flush(cur: dict) -> None:
        if cur.get("go_id") and not cur.get("is_obsolete"):
            terms.append(cur)

    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()

            if line == "[Term]":
                _flush(current)
                current = {}
                continue

            if line.startswith("[") and line != "[Term]":
                # [Typedef] or end of terms section
                break

            if not current and not line.startswith("id:"):
                continue

            if line.startswith("id: "):
                current["go_id"] = line[4:].strip()

            elif line.startswith("name: "):
                current["name"] = line[6:].strip()

            elif line.startswith("namespace: "):
                raw_ns = line[11:].strip()
                current["namespace"] = _NS_MAP.get(raw_ns, raw_ns)

            elif line.startswith("def: "):
                # def: "definition text" [ref1, ref2]
                m = re.match(r'def:\s+"(.*?)"', line)
                current["definition"] = m.group(1) if m else ""

            elif line.startswith("is_obsolete: "):
                current["is_obsolete"] = line[13:].strip() == "true"

            elif line.startswith("alt_id: "):
                current.setdefault("alt_ids", []).append(line[8:].strip())

            elif line.startswith("synonym: "):
                m = re.search(r'"(.+?)"', line)
                if m:
                    current.setdefault("synonyms", []).append(m.group(1))

            # is_a: GO:xxxxxxx ! label  →  current IS_A target (current=child)
            elif line.startswith("is_a: "):
                parent_go = line[6:].split(" !")[0].strip()
                if current.get("go_id") and parent_go:
                    relations.append({
                        "child_go": current["go_id"],
                        "parent_go": parent_go,
                        "relation_type": "is_a",
                    })

            # relationship: part_of GO:xxxxxxx ! label
            elif line.startswith("relationship: "):
                parts = line[14:].split(None, 1)
                if len(parts) == 2:
                    rel_type = parts[0].strip()
                    parent_go = parts[1].split(" !")[0].strip()
                    if rel_type in _RELATION_TYPES and current.get("go_id") and parent_go:
                        relations.append({
                            "child_go": current["go_id"],
                            "parent_go": parent_go,
                            "relation_type": rel_type,
                        })

    _flush(current)
    return terms, relations


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_go"
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

            self.logger.log(f"Downloading GO OBO from {url}", "INFO")
            resp = requests.get(url, stream=True, timeout=300)
            resp.raise_for_status()

            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        fh.write(chunk)

            file_hash = self._hash_file(dest)
            file_size = dest.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = f"[{self.DTP_NAME}] GO OBO downloaded ({file_size:,} bytes)"
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

            self.logger.log("Parsing OBO file...", "INFO")
            terms, relations = _parse_obo(input_file)

            out = self._dtp_dir(processed_dir)

            # ---- master_data ----
            df_terms = pd.DataFrame(terms)

            # Normalise list columns to JSON strings for parquet compatibility
            for col in ("alt_ids", "synonyms"):
                if col in df_terms.columns:
                    import json
                    df_terms[col] = df_terms[col].apply(
                        lambda v: json.dumps(v) if isinstance(v, list) else "[]"
                    )
                else:
                    df_terms[col] = "[]"

            for col in ("definition", "namespace", "name"):
                if col not in df_terms.columns:
                    df_terms[col] = ""

            master_parquet = out / "master_data.parquet"
            df_terms.to_parquet(master_parquet, index=False)
            if self.debug_mode:
                df_terms.to_csv(out / "master_data.csv", index=False)
            self.logger.log(f"  master_data: {len(df_terms)} active GO terms", "INFO")

            # ---- relations_data ----
            df_rel = pd.DataFrame(
                relations,
                columns=["child_go", "parent_go", "relation_type"],
            )
            rel_parquet = out / "relations_data.parquet"
            df_rel.to_parquet(rel_parquet, index=False)
            if self.debug_mode:
                df_rel.to_csv(out / "relations_data.csv", index=False)
            self.logger.log(
                f"  relations_data: {len(df_rel)} GO DAG edges", "INFO"
            )

            stats = ETLStepStats(
                total=len(df_terms),
                columns=len(df_terms.columns),
                output_size_bytes=master_parquet.stat().st_size,
                extras={"relation_rows": len(df_rel)},
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df_terms)} terms, {len(df_rel)} DAG edges"
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

        out = self._dtp_dir(processed_dir)
        master_file = out / "master_data.parquet"
        rel_file = out / "relations_data.parquet"

        if not master_file.exists():
            return (
                False,
                f"Processed file not found: {master_file}",
                ETLStepStats(errors=1),
            )

        try:
            df = pd.read_parquet(master_file, engine="pyarrow")
        except Exception as e:
            return False, f"Could not read parquet: {e}", ETLStepStats(errors=1)

        if df.empty:
            return True, "No GO term rows to load.", ETLStepStats()

        import json

        from igem_backend.modules.db.models.model_go import GOMaster, GORelation

        go_type_id = self.get_entity_type_id("Gene Ontology")

        # ---- Phase 1: GO terms ----
        existing_go: dict[str, int] = {
            gm.go_id: gm.id
            for gm in self.session.query(GOMaster.go_id, GOMaster.id).all()
        }

        total = created = skipped = warnings = 0
        # go_id → GOMaster.id (for phase 2)
        go_id_to_master_id: dict[str, int] = dict(existing_go)

        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1
            go_id = str(row.get("go_id") or "").strip()
            name = str(row.get("name") or "").strip()
            namespace = str(row.get("namespace") or "").strip()

            if not go_id or not name:
                warnings += 1
                continue

            if namespace not in ("BP", "MF", "CC"):
                warnings += 1
                continue

            if go_id in existing_go:
                skipped += 1
                continue

            # Entity + primary alias (go_id as code)
            entity_id, _ = self.get_or_create_entity(
                name=go_id,
                type_id=go_type_id,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                alias_type="code",
                xref_source="GO",
                alias_norm=go_id.lower(),
                auto_commit=False,
            )
            if entity_id is None:
                warnings += 1
                continue

            # Additional aliases: name, synonyms, alt_ids
            extra: list[dict] = []
            if name:
                extra.append({
                    "alias_value": name,
                    "alias_type": "preferred",
                    "xref_source": "GO",
                    "alias_norm": self._normalize(name),
                    "locale": "en",
                })

            for syn in json.loads(row.get("synonyms") or "[]"):
                if syn:
                    extra.append({
                        "alias_value": syn,
                        "alias_type": "synonym",
                        "xref_source": "GO",
                        "alias_norm": self._normalize(syn),
                        "locale": "en",
                    })

            for alt in json.loads(row.get("alt_ids") or "[]"):
                if alt:
                    extra.append({
                        "alias_value": alt,
                        "alias_type": "code",
                        "xref_source": "GO",
                        "alias_norm": alt.lower(),
                        "locale": "en",
                    })

            if extra:
                self.add_aliases(
                    entity_id=entity_id,
                    type_id=go_type_id,
                    aliases=extra,
                    data_source_id=self.data_source.id,
                    package_id=self.package.id,
                    auto_commit=False,
                )

            gm = GOMaster(
                entity_id=entity_id,
                go_id=go_id,
                name=self.guard_short(name),
                namespace=namespace,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(gm)
            self.session.flush()
            existing_go[go_id] = gm.id
            go_id_to_master_id[go_id] = gm.id
            created += 1

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    self.logger.log(
                        f"[{self.DTP_NAME}] Committed term batch {i + 1}/{total}",
                        "DEBUG",
                    )
                except Exception as e:
                    self.session.rollback()
                    return (
                        False,
                        f"Term batch commit failed at row {i + 1}: {e}",
                        ETLStepStats(errors=1),
                    )

        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            return False, f"Final term commit failed: {e}", ETLStepStats(errors=1)

        self.logger.log(
            f"[{self.DTP_NAME}] Phase 1 complete: "
            f"total={total} created={created} skipped={skipped} warnings={warnings}",
            "INFO",
        )

        # ---- Phase 2: GO DAG relations ----
        rel_created = rel_skipped = rel_warnings = 0

        if not rel_file.exists():
            self.logger.log(
                f"[{self.DTP_NAME}] relations_data.parquet not found — skipping DAG load",
                "WARNING",
            )
        else:
            try:
                df_rel = pd.read_parquet(rel_file, engine="pyarrow")
            except Exception as e:
                self.logger.log(f"Could not read relations parquet: {e}", "WARNING")
                df_rel = pd.DataFrame()

            if not df_rel.empty:
                # Pre-load existing relations to avoid duplicates
                existing_rels: set[tuple] = {
                    (r.child_id, r.parent_id, r.relation_type)
                    for r in self.session.query(GORelation).all()
                }

                for j, (_, row) in enumerate(df_rel.iterrows()):
                    child_go = str(row.get("child_go") or "").strip()
                    parent_go = str(row.get("parent_go") or "").strip()
                    rel_type = str(row.get("relation_type") or "").strip()

                    if not (child_go and parent_go and rel_type):
                        rel_warnings += 1
                        continue

                    child_mid = go_id_to_master_id.get(child_go)
                    parent_mid = go_id_to_master_id.get(parent_go)

                    if child_mid is None or parent_mid is None:
                        rel_skipped += 1
                        continue

                    key = (child_mid, parent_mid, rel_type)
                    if key in existing_rels:
                        rel_skipped += 1
                        continue

                    self.session.add(GORelation(
                        child_id=child_mid,
                        parent_id=parent_mid,
                        relation_type=rel_type,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    ))
                    existing_rels.add(key)
                    rel_created += 1

                    if (j + 1) % BATCH == 0:
                        try:
                            self.session.commit()
                            self.logger.log(
                                f"[{self.DTP_NAME}] Committed relation batch {j + 1}",
                                "DEBUG",
                            )
                        except Exception as e:
                            self.session.rollback()
                            return (
                                False,
                                f"Relation batch commit failed at row {j + 1}: {e}",
                                ETLStepStats(errors=1),
                            )

                try:
                    self.session.commit()
                except Exception as e:
                    self.session.rollback()
                    return (
                        False,
                        f"Final relation commit failed: {e}",
                        ETLStepStats(errors=1),
                    )

        self._log_trunc_summary()
        stats = ETLStepStats(
            total=total,
            created=created,
            skipped=skipped,
            warnings=warnings,
            extras={
                "relations_created": rel_created,
                "relations_skipped": rel_skipped,
            },
        )
        msg = (
            f"[{self.DTP_NAME}] Load complete: "
            f"terms: total={total} created={created} skipped={skipped} | "
            f"DAG: created={rel_created} skipped={rel_skipped}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats
