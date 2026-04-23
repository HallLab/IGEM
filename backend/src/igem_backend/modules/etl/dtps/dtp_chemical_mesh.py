"""
MeSH Chemical Master DTP.

Pipeline role:
- Supplement DTP for chemical entities — runs AFTER dtp_chemical_chebi.
- Only creates Chemical entities for MESH IDs not already in the database.
  Chemicals already covered by ChEBI (via MESH xrefs) are silently skipped.

Sources (downloaded from NLM, year-stamped filenames):
  supp{year}.xml — Supplementary Concept Records (SCRs): specific compounds
                   e.g. C534883 = 10074-G5 (a MYC inhibitor)
  desc{year}.xml — Main Heading descriptors filtered to D-section tree numbers:
                   drug/chemical classes, e.g. D007854 = Lead

Base URL: https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/

Deduplication:
  Pre-loads all MESH code aliases on Chemical entities (xref_source IN
  ["MESH","MeSH"]). Checks both with and without "MESH:" prefix since
  ChEBI stores bare IDs ("c534883") while IGEM-native aliases use CURIE
  ("mesh:c534883").

What is loaded:
  Entity (type=Chemicals) + EntityAlias:
    code/MESH     → MESH:C534883 or MESH:D007854  (primary, CURIE format)
    preferred/MESH → term name
    synonym/MESH  → entry terms / synonyms
  ChemicalMaster:
    ctd_id   = bare MESH ID without prefix (e.g. "C534883")
    cas_number from RelatedRegistryNumberList when present
    chebi_id remains null (MeSH-only chemicals)
    structure fields (formula, MW, SMILES) remain null
"""

import datetime
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

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
            return True, "No MeSH chemical rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_chemicals import (
            ChemicalMaster,
        )
        from igem_backend.modules.db.models.model_entities import (
            Entity,
            EntityAlias,
        )

        chem_type_id = self.get_entity_type_id("Chemicals")

        # Pre-load existing MESH chemical aliases — check both CURIE and bare.
        # ChEBI stores bare ("c534883"); IGEM-native uses CURIE ("mesh:c534883")
        self.logger.log(
            "Pre-loading existing MESH chemical aliases...", "INFO"
        )
        existing: set[str] = set()
        for a in self.session.query(EntityAlias).filter(
            EntityAlias.type_id == chem_type_id,
            EntityAlias.alias_type == "code",
            EntityAlias.xref_source.in_(["MESH", "MeSH"]),
        ).all():
            norm = (a.alias_norm or "").strip()
            if norm:
                existing.add(norm)
                if ":" in norm:
                    existing.add(norm.split(":", 1)[1])

        self.logger.log(
            f"  {len(existing):,} existing MESH chemical alias norms", "INFO"
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

            ui_lower = ui.lower()
            if ui_lower in existing or f"mesh:{ui_lower}" in existing:
                skipped += 1
                continue

            entity = Entity(
                type_id=chem_type_id,
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
                    type_id=chem_type_id,
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

            mesh_curie = f"MESH:{ui}"
            _add_alias(mesh_curie, "code", mesh_curie.lower(), primary=True)
            _add_alias(name, "preferred", self._normalize(name))
            for syn in str(row.get("synonyms") or "").split("|"):
                syn = syn.strip()
                if syn and syn != name:
                    _add_alias(syn, "synonym", self._normalize(syn))

            cas_raw = str(row.get("cas_number") or "").strip()
            cas = cas_raw if cas_raw and cas_raw != "0" else None

            self.session.add(ChemicalMaster(
                entity_id=entity.id,
                ctd_id=ui,
                cas_number=cas,
                chebi_id=None,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            ))

            existing.add(ui_lower)
            existing.add(f"mesh:{ui_lower}")
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
