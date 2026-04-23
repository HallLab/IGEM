"""
HPO Phenotype DTP.

Pipeline role:
- Master DTP for human phenotype entities from the Human Phenotype Ontology.
- Must run BEFORE dtp_phenotype_hpo_genes.
- No dependency on Gene/Disease DTPs.

Source:
- hp.obo (OBO format) from https://purl.obolibrary.org/obo/hp.obo

What is transformed:
- master_data.parquet   → one row per active HP term (hp_id, name, definition,
                          synonyms as JSON, alt_ids as JSON)
- relations_data.parquet → HP→HP hierarchy (child_hp, parent_hp, relation_type)

What is loaded:
Phase 1 — per active HP term:
  Entity (type=Phenotypes) + EntityAlias:
    - code/HPO      → HP:xxxxxxx  (primary)
    - preferred/HPO → term name
    - synonym/HPO   → each exact/broad/narrow synonym
    - code/HPO      → alt_ids (deprecated HP IDs redirecting here)
  PhenotypeMaster (hp_id, name, definition)

Phase 2 — PhenotypeRelation rows from relations_data.parquet
  Both endpoints must already exist; edges with unknown terms are skipped.
"""

import json
import re
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_FILE = "hp.obo"
_ROOT_ID = "HP:0000001"
_RELATION_TYPES = {"is_a", "part_of"}

# Regex to extract the quoted text from HPO def: lines
# e.g.: def: "A deviation from..." [HPO:probinson]  →  A deviation from...
_DEF_RE = re.compile(r'^"(.*?)"\s*\[', re.DOTALL)


def _parse_obo(path: Path) -> tuple[list[dict], list[dict]]:
    """
    Stream-parse hp.obo and return (terms, relations).

    terms: [{hp_id, name, definition, synonyms, alt_ids}, ...]
    relations: [{child_hp, parent_hp, relation_type}, ...]

    Stops processing stanzas once a non-[Term] stanza is encountered
    (e.g. [Typedef]) to avoid picking up meta-relations.
    """
    terms: list[dict] = []
    relations: list[dict] = []

    current: dict = {}

    def _flush():
        if not current:
            return
        hp_id = current.get("hp_id")
        name = current.get("name")
        if not hp_id or not name:
            return
        if current.get("obsolete"):
            return
        if hp_id == _ROOT_ID:
            return
        terms.append({
            "hp_id":      hp_id,
            "name":       name,
            "definition": current.get("definition"),
            "synonyms":   json.dumps(current.get("synonyms", [])),
            "alt_ids":    json.dumps(current.get("alt_ids", [])),
        })
        for parent_hp, rel_type in current.get("parents", []):
            relations.append({
                "child_hp":     hp_id,
                "parent_hp":    parent_hp,
                "relation_type": rel_type,
            })

    in_term = False

    with open(path, encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.rstrip()

            if line == "[Term]":
                _flush()
                current = {"synonyms": [], "alt_ids": [], "parents": []}
                in_term = True
                continue

            if line.startswith("[") and line != "[Term]":
                # [Typedef] or any other stanza — stop term processing
                _flush()
                in_term = False
                current = {}
                continue

            if not in_term or not line or line.startswith("!"):
                continue

            if ":" not in line:
                continue

            tag, _, value = line.partition(": ")
            value = value.split(" ! ")[0].strip()  # strip inline comments

            if tag == "id":
                current["hp_id"] = value
            elif tag == "name":
                current["name"] = value
            elif tag == "def":
                m = _DEF_RE.match(value)
                current["definition"] = m.group(1) if m else value.strip('"')
            elif tag == "alt_id":
                current["alt_ids"].append(value)
            elif tag == "synonym":
                # synonym: "text" EXACT/BROAD/NARROW [...]
                m = re.match(r'^"(.*?)"', value)
                if m:
                    current["synonyms"].append(m.group(1))
            elif tag == "is_a":
                # is_a: HP:0000001 ! Root
                parent = value.split(" ")[0].strip()
                if parent.startswith("HP:"):
                    current["parents"].append((parent, "is_a"))
            elif tag == "relationship":
                # relationship: part_of HP:0000118 ! Phenotypic abnormality
                parts = value.split(" ")
                if len(parts) >= 2 and parts[0] in _RELATION_TYPES:
                    parent = parts[1].strip()
                    if parent.startswith("HP:"):
                        current["parents"].append((parent, parts[0]))
            elif tag == "is_obsolete" and value == "true":
                current["obsolete"] = True

    _flush()
    return terms, relations


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_phenotype_hpo"
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

            self.logger.log(f"Streaming HPO OBO from {url}", "INFO")
            with requests.get(url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            fh.write(chunk)

            file_hash = self._hash_file(dest)
            file_size = dest.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = f"[{self.DTP_NAME}] hp.obo downloaded ({file_size:,} bytes)"
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

            self.logger.log("Parsing hp.obo...", "INFO")
            terms, relations = _parse_obo(input_file)

            out = self._dtp_dir(processed_dir)

            df_master = pd.DataFrame(terms)
            master_parquet = out / "master_data.parquet"
            df_master.to_parquet(master_parquet, index=False)
            if self.debug_mode:
                df_master.to_csv(out / "master_data.csv", index=False)
            self.logger.log(
                f"  master_data: {len(df_master):,} active HP terms", "INFO"
            )

            df_rel = pd.DataFrame(
                relations,
                columns=["child_hp", "parent_hp", "relation_type"],
            )
            rel_parquet = out / "relations_data.parquet"
            df_rel.to_parquet(rel_parquet, index=False)
            if self.debug_mode:
                df_rel.to_csv(out / "relations_data.csv", index=False)
            self.logger.log(
                f"  relations_data: {len(df_rel):,} HP→HP edges", "INFO"
            )

            stats = ETLStepStats(
                total=len(df_master),
                columns=len(df_master.columns),
                output_size_bytes=master_parquet.stat().st_size,
                extras={"relation_rows": len(df_rel)},
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df_master):,} terms, {len(df_rel):,} hierarchy edges"
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
        master_parquet = out / "master_data.parquet"
        rel_parquet = out / "relations_data.parquet"

        if not master_parquet.exists():
            return (
                False,
                f"Processed file not found: {master_parquet}",
                ETLStepStats(errors=1),
            )

        try:
            df = pd.read_parquet(master_parquet, engine="pyarrow")
        except Exception as e:
            return False, f"Could not read parquet: {e}", ETLStepStats(errors=1)

        if df.empty:
            return True, "No HP term rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_entities import Entity, EntityAlias
        from igem_backend.modules.db.models.model_phenotypes import (
            PhenotypeMaster,
            PhenotypeRelation,
        )

        pheno_type_id = self.get_entity_type_id("Phenotypes")

        # Pre-load existing HP IDs to skip re-loads
        existing: set[str] = {
            row[0]
            for row in self.session.query(PhenotypeMaster.hp_id).all()
            if row[0]
        }

        total = created = skipped = warnings = 0
        hp_id_to_master_id: dict[str, int] = {
            row[0]: row[1]
            for row in self.session.query(
                PhenotypeMaster.hp_id, PhenotypeMaster.id
            ).all()
        }
        BATCH = 500

        # ---- Phase 1: PhenotypeMaster ----
        self.logger.log(f"[{self.DTP_NAME}] Phase 1: loading HP terms...", "INFO")

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1

            hp_id = str(row.get("hp_id") or "").strip()
            name = str(row.get("name") or "").strip()

            if not hp_id or not name:
                warnings += 1
                continue

            if hp_id in existing:
                skipped += 1
                continue

            entity = Entity(
                type_id=pheno_type_id,
                is_active=True,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(entity)
            self.session.flush()

            seen_keys: set[tuple[str, str, str]] = set()

            def _add_alias(
                val: str,
                atype: str,
                xsrc: str,
                norm: Optional[str] = None,
                primary: bool = False,
            ) -> None:
                val = (self.guard_alias(val) or "").strip()
                if not val:
                    return
                key = (val, atype, xsrc)
                if key in seen_keys:
                    return
                seen_keys.add(key)
                self.session.add(EntityAlias(
                    entity_id=entity.id,
                    type_id=pheno_type_id,
                    alias_value=val,
                    alias_type=atype,
                    xref_source=xsrc,
                    alias_norm=self.guard_alias_norm(norm or val.lower()),
                    is_primary=primary,
                    is_active=True,
                    locale="en",
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                ))

            _add_alias(hp_id, "code", "HPO", hp_id.lower(), primary=True)
            _add_alias(name, "preferred", "HPO", self._normalize(name))

            for syn in json.loads(row.get("synonyms") or "[]"):
                if syn:
                    _add_alias(syn, "synonym", "HPO", self._normalize(syn))

            for alt_id in json.loads(row.get("alt_ids") or "[]"):
                if alt_id:
                    _add_alias(alt_id, "code", "HPO", alt_id.lower())

            definition = str(row.get("definition") or "").strip() or None
            pm = PhenotypeMaster(
                entity_id=entity.id,
                hp_id=hp_id,
                name=self.guard_short(name),
                definition=definition,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(pm)
            self.session.flush()
            hp_id_to_master_id[hp_id] = pm.id
            existing.add(hp_id)
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
            return False, f"Phase 1 final commit failed: {e}", ETLStepStats(errors=1)

        self.logger.log(
            f"[{self.DTP_NAME}] Phase 1 complete: "
            f"created={created} skipped={skipped} warnings={warnings}",
            "INFO",
        )

        # ---- Phase 2: PhenotypeRelation hierarchy ----
        if not rel_parquet.exists():
            self.logger.log(
                f"[{self.DTP_NAME}] relations_data.parquet not found — skipping hierarchy.",
                "WARNING",
            )
            return True, f"Phase 1 only (no relations_data.parquet)", ETLStepStats(
                total=total, created=created, skipped=skipped, warnings=warnings
            )

        try:
            df_rel = pd.read_parquet(rel_parquet, engine="pyarrow")
        except Exception as e:
            return False, f"Could not read relations_data.parquet: {e}", ETLStepStats(errors=1)

        self.logger.log(
            f"[{self.DTP_NAME}] Phase 2: loading {len(df_rel):,} hierarchy edges...",
            "INFO",
        )

        # Dedup existing relations in memory
        existing_rels: set[tuple[int, int, str]] = {
            (r.child_id, r.parent_id, r.relation_type)
            for r in self.session.query(PhenotypeRelation).all()
        }

        rel_created = rel_skipped = 0
        for i, (_, row) in enumerate(df_rel.iterrows()):
            child_hp  = str(row.get("child_hp")  or "").strip()
            parent_hp = str(row.get("parent_hp") or "").strip()
            rel_type  = str(row.get("relation_type") or "").strip()

            if not (child_hp and parent_hp and rel_type):
                continue

            child_mid  = hp_id_to_master_id.get(child_hp)
            parent_mid = hp_id_to_master_id.get(parent_hp)

            if child_mid is None or parent_mid is None:
                rel_skipped += 1
                continue

            key = (child_mid, parent_mid, rel_type)
            if key in existing_rels:
                rel_skipped += 1
                continue

            self.session.add(PhenotypeRelation(
                child_id=child_mid,
                parent_id=parent_mid,
                relation_type=rel_type,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            ))
            existing_rels.add(key)
            rel_created += 1

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    self.logger.log(
                        f"[{self.DTP_NAME}] Relations batch {i + 1}", "DEBUG"
                    )
                except Exception as e:
                    self.session.rollback()
                    return (
                        False,
                        f"Relations batch commit failed at {i + 1}: {e}",
                        ETLStepStats(errors=1),
                    )

        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            return False, f"Phase 2 final commit failed: {e}", ETLStepStats(errors=1)

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
            f"terms created={created} skipped={skipped}; "
            f"relations created={rel_created} skipped={rel_skipped}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats
