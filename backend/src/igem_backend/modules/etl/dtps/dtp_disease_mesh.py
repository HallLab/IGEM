"""
MeSH Disease Master DTP.

Pipeline role:
- Supplement DTP for disease entities — runs AFTER dtp_disease_mondo.
- Only creates Disease entities for MESH IDs not already in the database.
  Diseases already covered by MONDO (via MESH xrefs) are silently skipped.

Source (downloaded from NLM, year-stamped filename):
  desc{year}.xml — Main Heading descriptors filtered to disease tree sections:
    C*   → diseases and conditions (C01–C26)
    F03* → mental disorders

Base URL: https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/

Deduplication:
  Pre-loads all MESH code aliases on Disease entities (xref_source=MESH).
  MONDO stores them as CURIE ("MESH:D000309" → alias_norm "mesh:d000309"),
  so we check against that normalised form.

What is loaded:
  Entity (type=Diseases) + EntityAlias:
    code/MESH     → MESH:D000309  (primary, CURIE format)
    preferred/MESH → term name
    synonym/MESH  → non-preferred terms
  DiseaseMaster:
    disease_id = "MESH:D000309"  (primary identifier for MeSH-only diseases)
    mesh_id    = "D000309"       (bare ID cross-reference column)
    label      = term name
    description from ScopeNote when present
    mondo_id / omim_id remain null
"""

import datetime
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_DISEASE_TREES = ("C", "F03")


def _current_year() -> int:
    return datetime.datetime.now().year


def _parse_desc_xml_disease(path: Path) -> list[dict]:
    """Parse MeSH DescriptorRecord XML, keeping C-tree and F03 entries."""
    records: list[dict] = []
    with open(path, "rb") as fh:
        for _event, elem in ET.iterparse(fh, events=("end",)):
            if elem.tag != "DescriptorRecord":
                continue

            tree_nums = [
                tn.text for tn in elem.findall("TreeNumberList/TreeNumber")
                if tn.text
            ]
            if not any(
                t.startswith(p) for t in tree_nums for p in _DISEASE_TREES
            ):
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
                "synonyms": "|".join(syns),
            })
            elem.clear()

    return records


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_disease_mesh"
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

    def _file_url(self) -> str:
        base = self.data_source.source_url.rstrip("/")
        year = _current_year()
        return f"{base}/desc{year}.xml"

    # -------------------------------------------------------------------------
    # EXTRACT
    # -------------------------------------------------------------------------
    def extract(
        self, raw_dir: str
    ) -> tuple[bool, str, Optional[str], ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Extract starting...", "INFO")
        try:
            year = _current_year()
            fname = f"desc{year}.xml"
            dest = self._dtp_dir(raw_dir) / fname
            url = self._file_url()

            self.logger.log(f"Downloading {url}", "INFO")
            with requests.get(url, stream=True, timeout=600) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            fh.write(chunk)

            file_size = dest.stat().st_size
            file_hash = self._hash_file(dest)
            msg = (
                f"[{self.DTP_NAME}] {fname} downloaded ({file_size:,} bytes)"
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
            year = _current_year()
            desc_file = self._dtp_dir(raw_dir) / f"desc{year}.xml"

            if not desc_file.exists():
                return (
                    False,
                    f"Input file not found: {desc_file}",
                    ETLStepStats(errors=1),
                )

            self.logger.log(
                f"Parsing {desc_file.name} (C-tree + F03 filter)...", "INFO"
            )
            records = _parse_desc_xml_disease(desc_file)

            df = pd.DataFrame(records).drop_duplicates(subset=["ui"])
            out = self._dtp_dir(processed_dir)
            master_parquet = out / "master_data.parquet"
            df.to_parquet(master_parquet, index=False)
            if self.debug_mode:
                df.to_csv(out / "master_data.csv", index=False)

            self.logger.log(
                f"  {len(df):,} disease terms extracted", "INFO"
            )

            stats = ETLStepStats(
                total=len(df),
                columns=len(df.columns),
                output_size_bytes=master_parquet.stat().st_size,
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df):,} MeSH disease terms"
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
            return (
                False, f"Could not read parquet: {e}", ETLStepStats(errors=1)
            )

        if df.empty:
            return True, "No MeSH disease rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_diseases import DiseaseMaster
        from igem_backend.modules.db.models.model_entities import (
            Entity,
            EntityAlias,
        )

        disease_type_id = self.get_entity_type_id("Diseases")

        # Pre-load existing MESH disease aliases (MONDO stores as CURIE)
        # alias_norm = "mesh:d000309"
        self.logger.log(
            "Pre-loading existing MESH disease aliases...", "INFO"
        )
        existing: set[str] = {
            (a.alias_norm or "").strip()
            for a in self.session.query(EntityAlias).filter(
                EntityAlias.type_id == disease_type_id,
                EntityAlias.alias_type == "code",
                EntityAlias.xref_source == "MESH",
            ).all()
        }

        self.logger.log(
            f"  {len(existing):,} existing MESH disease alias norms", "INFO"
        )

        total = created = skipped = warnings = 0
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1

            ui = str(row.get("ui") or "").strip()
            name = str(row.get("name") or "").strip()
            if not ui or not name:
                warnings += 1
                continue

            mesh_curie = f"MESH:{ui}"
            if mesh_curie.lower() in existing:
                skipped += 1
                continue

            entity = Entity(
                type_id=disease_type_id,
                is_active=True,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(entity)
            self.session.flush()

            seen: set[tuple] = set()

            def _add_alias(
                val: str,
                atype: str,
                norm: Optional[str] = None,
                primary: bool = False,
            ) -> None:
                val = (self.guard_alias(val) or "").strip()
                if not val:
                    return
                key = (val, atype)
                if key in seen:
                    return
                seen.add(key)
                self.session.add(EntityAlias(
                    entity_id=entity.id,
                    type_id=disease_type_id,
                    alias_value=val,
                    alias_type=atype,
                    xref_source="MESH",
                    alias_norm=self.guard_alias_norm(norm or val.lower()),
                    is_primary=primary,
                    is_active=True,
                    locale="en",
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                ))

            _add_alias(mesh_curie, "code", mesh_curie.lower(), primary=True)
            _add_alias(name, "preferred", self._normalize(name))
            for syn in str(row.get("synonyms") or "").split("|"):
                syn = syn.strip()
                if syn and syn != name:
                    _add_alias(syn, "synonym", self._normalize(syn))

            definition = str(row.get("definition") or "").strip() or None

            self.session.add(DiseaseMaster(
                entity_id=entity.id,
                disease_id=mesh_curie,
                label=self.guard_short(name),
                description=definition,
                mesh_id=ui,
                mondo_id=None,
                omim_id=None,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            ))

            existing.add(mesh_curie.lower())
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
