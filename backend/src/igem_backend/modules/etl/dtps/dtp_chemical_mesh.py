"""
MeSH Chemical DTP — enricher for UniChem/ChEBI/HMDB-seeded entities.

Pipeline role
-------------
Runs AFTER dtp_chemical_unichem, dtp_chemical_chebi, and dtp_chemical_hmdb.
Matches each MeSH chemical term against an existing Entity and enriches
it with MeSH-specific nomenclature (MESH codes, alternative names).

Match strategy (first that wins)
--------------------------------
  1. MeSH UI match (bare OR CURIE) against existing code/MESH alias
                                                                 (strong)
     — typically lands on entities where ChEBI added a MeSH xref
  2. CAS number match against existing ChemicalMaster.cas_number   (medium)
  3. Unique normalized name match against existing preferred
     aliases (only when exactly ONE entity shares the normalized
     name — ambiguous names fall through)                          (weak)
  4. Fallback — create a new Entity. MeSH descriptors of chemical
     categories (e.g. "Antineoplastic Agents") typically fall here.

Primary promotion rule
----------------------
MeSH names tend to be generic ("Glucose" vs. ChEBI's "D-glucose").
`preferred/MESH` is only promoted to is_primary=True when the entity
has NO preferred primary yet (newly created, or never enriched by a
higher-priority source). For matches against entities already carrying
a ChEBI/HMDB preferred-primary alias, MeSH adds its preferred name as
a regular alias (is_primary=False), leaving the display name unchanged.

Idempotency
-----------
Entities that already carry a `preferred/MESH` alias are skipped on
re-run.

Sources (NLM, year-stamped)
---------------------------
  supp{year}.xml — Supplementary Concept Records (SCRs): specific
                   compounds, e.g. C534883 = 10074-G5 (MYC inhibitor)
  desc{year}.xml — Main Heading descriptors filtered to D-tree:
                   drug/chemical classes, e.g. D007854 = Lead

Base URL: https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/

What is added
-------------
  EntityAlias:
    code/MESH      → MESH:<UI>                           (CURIE form)
    preferred/MESH → term name
    synonym/MESH   → entry terms / synonyms
  ChemicalMaster (fill-null only):
    ctd_id     = bare MESH UI (e.g. "C534883")
    cas_number (when MeSH provides one and field was NULL)
"""

import datetime
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin
from igem_backend.modules.nlp.normalizer import normalize

_CAS_RE = re.compile(r"^\d+-\d+-\d+")


def _current_year() -> int:
    return datetime.datetime.now().year


def _parse_supp_xml(path: Path) -> list[dict]:
    """Parse MeSH SupplementalRecord XML (supp{year}.xml)."""
    records: list[dict] = []
    with open(path, "rb") as fh:
        for _event, elem in ET.iterparse(fh, events=("end",)):
            if elem.tag != "SupplementalRecord":
                continue

            ui = (elem.findtext("SupplementalRecordUI") or "").strip()
            name_el = elem.find("SupplementalRecordName/String")
            name = (name_el.text if name_el is not None else "").strip()
            if not ui or not name:
                elem.clear()
                continue

            note = (elem.findtext("Note") or "").strip()

            cas = ""
            for rn in elem.findall(".//RelatedRegistryNumber"):
                text = (rn.text or "").strip()
                m = _CAS_RE.match(text)
                if m:
                    cas = m.group()
                    break

            syns: list[str] = []
            seen: set[str] = {name.lower()}
            for term in elem.findall(".//TermList/Term"):
                s = (term.findtext("String") or "").strip()
                if s and s.lower() not in seen:
                    seen.add(s.lower())
                    if term.get("RecordPreferredTermYN") != "Y":
                        syns.append(s)

            records.append({
                "ui": ui,
                "name": name,
                "definition": note,
                "cas_number": cas,
                "synonyms": "|".join(syns),
                "mesh_source": "SCR",
            })
            elem.clear()

    return records


def _parse_desc_xml_d_tree(path: Path) -> list[dict]:
    """Parse MeSH DescriptorRecord XML, keeping only D-tree entries."""
    records: list[dict] = []
    with open(path, "rb") as fh:
        for _event, elem in ET.iterparse(fh, events=("end",)):
            if elem.tag != "DescriptorRecord":
                elem.clear()
                continue

            tree_nums = [
                tn.text for tn in elem.findall("TreeNumberList/TreeNumber")
                if tn.text
            ]
            if not any(t.startswith("D") for t in tree_nums):
                elem.clear()
                continue

            ui = (elem.findtext("DescriptorUI") or "").strip()
            name_el = elem.find("DescriptorName/String")
            name = (name_el.text if name_el is not None else "").strip()
            if not ui or not name:
                elem.clear()
                continue

            scope_note = ""
            for concept in elem.findall("ConceptList/Concept"):
                if concept.get("PreferredConceptYN") == "Y":
                    scope_note = (concept.findtext("ScopeNote") or "").strip()
                    break

            syns: list[str] = []
            seen: set[str] = {name.lower()}
            for term in elem.findall(".//TermList/Term"):
                s = (term.findtext("String") or "").strip()
                if s and s.lower() not in seen:
                    seen.add(s.lower())
                    if term.get("RecordPreferredTermYN") != "Y":
                        syns.append(s)

            records.append({
                "ui": ui,
                "name": name,
                "definition": scope_note,
                "cas_number": "",
                "synonyms": "|".join(syns),
                "mesh_source": "descriptor",
            })
            elem.clear()

    return records


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_chemical_mesh"
    DTP_VERSION = "1.1.0"
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

    def _file_urls(self) -> tuple[str, str]:
        base = self.data_source.source_url.rstrip("/")
        year = _current_year()
        return f"{base}/supp{year}.xml", f"{base}/desc{year}.xml"

    # -------------------------------------------------------------------------
    # EXTRACT
    # -------------------------------------------------------------------------
    def extract(
        self, raw_dir: str
    ) -> tuple[bool, str, Optional[str], ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Extract starting...", "INFO")
        try:
            landing = self._dtp_dir(raw_dir)
            year = _current_year()
            supp_url, desc_url = self._file_urls()
            total_bytes = 0

            for url, fname in [
                (supp_url, f"supp{year}.xml"),
                (desc_url, f"desc{year}.xml"),
            ]:
                dest = landing / fname
                self.logger.log(f"Downloading {url}", "INFO")
                with requests.get(url, stream=True, timeout=600) as resp:
                    resp.raise_for_status()
                    with open(dest, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=65536):
                            if chunk:
                                fh.write(chunk)
                sz = dest.stat().st_size
                total_bytes += sz
                self.logger.log(f"  {fname}: {sz:,} bytes", "INFO")

            file_hash = self._hash_file(landing / f"supp{year}.xml")
            msg = (
                f"[{self.DTP_NAME}] MeSH XML files downloaded "
                f"({total_bytes:,} bytes total)"
            )
            self.logger.log(msg, "INFO")
            return (
                True, msg, file_hash, ETLStepStats(file_size_bytes=total_bytes)
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
            year = _current_year()
            landing = self._dtp_dir(raw_dir)
            supp_file = landing / f"supp{year}.xml"
            desc_file = landing / f"desc{year}.xml"

            records: list[dict] = []

            if supp_file.exists():
                self.logger.log(
                    f"Parsing {supp_file.name} (SCRs)...", "INFO"
                )
                scr_records = _parse_supp_xml(supp_file)
                records.extend(scr_records)
                self.logger.log(
                    f"  {len(scr_records):,} SCR chemical terms", "INFO"
                )

            if desc_file.exists():
                self.logger.log(
                    f"Parsing {desc_file.name} (D-tree descriptors)...", "INFO"
                )
                desc_records = _parse_desc_xml_d_tree(desc_file)
                records.extend(desc_records)
                self.logger.log(
                    f"  {len(desc_records):,} descriptor chemical terms",
                    "INFO",
                )

            df = pd.DataFrame(records).drop_duplicates(subset=["ui"])
            out = self._dtp_dir(processed_dir)
            master_parquet = out / "master_data.parquet"
            df.to_parquet(master_parquet, index=False)
            if self.debug_mode:
                df.to_csv(out / "master_data.csv", index=False)

            if not df.empty:
                src_counts = df["mesh_source"].value_counts().to_dict()
                src_str = ", ".join(
                    f"{k}:{v}" for k, v in sorted(src_counts.items())
                )
                self.logger.log(
                    f"  mesh_source distribution: {src_str}", "INFO"
                )

            stats = ETLStepStats(
                total=len(df),
                columns=len(df.columns),
                output_size_bytes=master_parquet.stat().st_size,
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df):,} MeSH chemical terms"
            )
            self.logger.log(msg, "INFO")
            return True, msg, stats

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Transform failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, ETLStepStats(errors=1)

    # -------------------------------------------------------------------------
    # LOAD — enrich matched entities; fallback creates new entities
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
            return (
                False,
                f"Could not read parquet: {e}",
                ETLStepStats(errors=1),
            )

        if df.empty:
            return True, "No MeSH chemical rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_chemicals import (
            ChemicalMaster,
        )
        from igem_backend.modules.db.models.model_entities import (
            Entity,
            EntityAlias,
        )

        chem_type_id = self.get_entity_type_id("Chemicals")

        # --- Pre-load match maps ---
        # 1. MESH xref → entity. Store BOTH bare and CURIE forms to match
        #    however each upstream source stored the xref.
        self.logger.log("  Loading MESH xref → entity map...", "INFO")
        mesh_xref_to_entity: dict[str, int] = {}
        for val, eid in self.session.query(
            EntityAlias.alias_value, EntityAlias.entity_id
        ).filter(
            EntityAlias.type_id == chem_type_id,
            EntityAlias.alias_type == "code",
            EntityAlias.xref_source.in_(["MESH", "MeSH"]),
        ).all():
            if not val:
                continue
            mesh_xref_to_entity[val] = eid
            mesh_xref_to_entity[val.lower()] = eid
            if ":" in val:
                bare = val.split(":", 1)[1]
                mesh_xref_to_entity[bare] = eid
                mesh_xref_to_entity[bare.lower()] = eid

        # 2. CAS → entity (from ChemicalMaster.cas_number)
        self.logger.log("  Loading CAS → entity map...", "INFO")
        cas_to_entity: dict[str, int] = {
            row[0]: row[1]
            for row in self.session.query(
                ChemicalMaster.cas_number, ChemicalMaster.entity_id
            ).filter(ChemicalMaster.cas_number.isnot(None)).all()
            if row[0]
        }

        # 3. Normalized name → entities (LIST — ambiguous names kept separate)
        self.logger.log(
            "  Loading normalized preferred-name → entities...", "INFO"
        )
        name_to_entities: dict[str, list[int]] = defaultdict(list)
        for norm, eid in self.session.query(
            EntityAlias.alias_norm, EntityAlias.entity_id
        ).filter(
            EntityAlias.type_id == chem_type_id,
            EntityAlias.alias_type == "preferred",
            EntityAlias.alias_norm.isnot(None),
        ).all():
            if norm:
                name_to_entities[norm].append(eid)

        # 4. Already enriched by MeSH (idempotency)
        already_enriched: set[int] = {
            row[0]
            for row in self.session.query(
                EntityAlias.entity_id
            ).filter(
                EntityAlias.xref_source.in_(["MESH", "MeSH"]),
                EntityAlias.alias_type == "preferred",
            ).distinct().all()
        }

        self.logger.log(
            f"  Lookups: mesh_xref={len(mesh_xref_to_entity):,} "
            f"cas={len(cas_to_entity):,} "
            f"preferred_names={len(name_to_entities):,} "
            f"already={len(already_enriched):,}",
            "INFO",
        )

        total = 0
        matched_mesh = 0
        matched_cas = 0
        matched_name = 0
        created_new = 0
        already_done = 0
        warnings = 0
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1
            ui = str(row.get("ui") or "").strip()
            name = str(row.get("name") or "").strip()
            if not ui or not name:
                warnings += 1
                continue

            mesh_curie = f"MESH:{ui}"
            cas_raw = str(row.get("cas_number") or "").strip()
            cas = cas_raw if cas_raw and cas_raw != "0" else None

            # --- Match strategy ---
            entity_id: Optional[int] = None
            match_type: Optional[str] = None

            if mesh_curie in mesh_xref_to_entity:
                entity_id = mesh_xref_to_entity[mesh_curie]
                match_type = "mesh"
            elif ui in mesh_xref_to_entity:
                entity_id = mesh_xref_to_entity[ui]
                match_type = "mesh"
            elif cas and cas in cas_to_entity:
                entity_id = cas_to_entity[cas]
                match_type = "cas"
            else:
                norm_name = normalize(name)
                candidates = name_to_entities.get(norm_name, [])
                if len(candidates) == 1:
                    entity_id = candidates[0]
                    match_type = "name"
                # Ambiguous names (>1 candidate) → fall through to create

            # Idempotency guard
            if entity_id is not None and entity_id in already_enriched:
                already_done += 1
                continue

            # Stats
            if match_type == "mesh":
                matched_mesh += 1
            elif match_type == "cas":
                matched_cas += 1
            elif match_type == "name":
                matched_name += 1

            # Fallback: create new entity
            if entity_id is None:
                entity = Entity(
                    type_id=chem_type_id,
                    is_active=True,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(entity)
                self.session.flush()
                entity_id = entity.id
                created_new += 1

            # --- Decide if MeSH promotes preferred to is_primary ---
            # MeSH is the LOWEST priority source for display names.
            # Only promote when no preferred-primary exists yet.
            if match_type is not None:
                existing_alias_keys = {
                    (av, at, xs)
                    for av, at, xs in self.session.query(
                        EntityAlias.alias_value,
                        EntityAlias.alias_type,
                        EntityAlias.xref_source,
                    ).filter_by(entity_id=entity_id).all()
                }
                has_preferred_primary = self.session.query(
                    EntityAlias.id
                ).filter_by(
                    entity_id=entity_id,
                    alias_type="preferred",
                    is_primary=True,
                ).first() is not None
                promote_primary = not has_preferred_primary
                if promote_primary:
                    # Demote UniChem UCI (non-preferred primary) so MeSH
                    # preferred name can take the slot.
                    self.session.query(EntityAlias).filter_by(
                        entity_id=entity_id, is_primary=True
                    ).update({"is_primary": False})
            else:
                existing_alias_keys = set()
                promote_primary = True  # freshly created entity

            seen_keys: set[tuple[str, str, str]] = set(existing_alias_keys)

            def _add_alias(
                val: str,
                atype: str,
                primary: bool = False,
            ) -> None:
                val = (self.guard_alias(val) or "").strip()
                if not val:
                    return
                key = (val, atype, "MESH")
                if key in seen_keys:
                    return
                seen_keys.add(key)
                self.session.add(EntityAlias(
                    entity_id=entity_id,
                    type_id=chem_type_id,
                    alias_value=val,
                    alias_type=atype,
                    xref_source="MESH",
                    alias_norm=self.guard_alias_norm(normalize(val)),
                    is_primary=primary,
                    is_active=True,
                    locale="en",
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                ))

            _add_alias(mesh_curie, "code", primary=False)
            _add_alias(name, "preferred", primary=promote_primary)
            for syn in str(row.get("synonyms") or "").split("|"):
                syn = syn.strip()
                if syn and syn != name:
                    _add_alias(syn, "synonym")

            # --- ChemicalMaster: fill-null-only or create ---
            cm = self.session.query(ChemicalMaster).filter_by(
                entity_id=entity_id
            ).one_or_none()

            if cm is None:
                cm = ChemicalMaster(
                    entity_id=entity_id,
                    ctd_id=ui,
                    cas_number=cas,
                    chebi_id=None,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(cm)
            else:
                if cm.ctd_id is None:
                    cm.ctd_id = ui
                if cm.cas_number is None and cas:
                    cm.cas_number = cas

            already_enriched.add(entity_id)
            # Keep lookup maps fresh within this run
            mesh_xref_to_entity[mesh_curie] = entity_id
            mesh_xref_to_entity[ui] = entity_id
            if cas and cas not in cas_to_entity:
                cas_to_entity[cas] = entity_id

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    if (i + 1) % (BATCH * 20) == 0:
                        self.logger.log(
                            f"  Committed {i + 1:,}/{total:,} "
                            f"(mesh={matched_mesh:,} "
                            f"cas={matched_cas:,} "
                            f"name={matched_name:,} "
                            f"new={created_new:,})",
                            "INFO",
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
            return (
                False,
                f"Final commit failed: {e}",
                ETLStepStats(errors=1),
            )

        stats = ETLStepStats(
            total=total,
            created=matched_mesh + matched_cas + matched_name + created_new,
            skipped=already_done,
            warnings=warnings,
            extras={
                "matched_mesh_xref": matched_mesh,
                "matched_cas":       matched_cas,
                "matched_name":      matched_name,
                "created_new":       created_new,
                "already_enriched":  already_done,
            },
        )
        msg = (
            f"[{self.DTP_NAME}] Enrich complete: "
            f"total={total:,} "
            f"mesh={matched_mesh:,} "
            f"cas={matched_cas:,} "
            f"name={matched_name:,} "
            f"new={created_new:,} "
            f"already={already_done:,} "
            f"warn={warnings:,}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats
