"""
HMDB Chemical DTP — enricher for UniChem-seeded entities.

Pipeline role
-------------
Runs AFTER dtp_chemical_unichem (and optionally after dtp_chemical_chebi).
Matches each HMDB metabolite against an existing Entity (usually created
by UniChem) and enriches it with:

  - common name (preferred/HMDB alias, promoted to is_primary)
  - secondary HMDB accessions (old 7-digit format)
  - up to 50 synonyms (synonym/HMDB)
  - CAS / PubChem / ChEBI / KEGG xrefs
  - ChemicalMaster fields that were NULL: smiles, formula, molecular_weight,
    cas_number, pubchem_cid, chebi_id (inchi_key only when missing)

Match strategy (first that wins)
--------------------------------
  1. InChIKey match against existing ChemicalMaster.inchi_key      (strong)
  2. HMDB accession match against existing EntityAlias
     (xref_source=HMDB, alias_type=code)                            (medium)
  3. Fallback — create new Entity (for the ~5-10% HMDB records
     without InChIKey and without an HMDB xref loaded by UniChem)

Fill-null-only semantics
------------------------
When enriching, HMDB never OVERWRITES fields that are already populated.
UniChem/ChEBI's structural values (InChIKey, ChEBI ID) take precedence.

Idempotency
-----------
Entities that already carry a `preferred/HMDB` alias are skipped on
re-run. To force re-enrichment, delete those aliases first.

Source file
-----------
  hmdb_metabolites.zip  →  hmdb_metabolites.xml
  (~4.7 GB unzipped, ~220k records)
  https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip

NLP consumption
---------------
The description field is preserved in master_data.parquet for use by
dtp_nlp_hmdb (which runs the entity resolver over each description). It
is NOT persisted to the database — the DB schema has no description
column on ChemicalMaster.

Operational: manual download
----------------------------
HMDB sits behind a Cloudflare challenge that blocks Python HTTP clients.
The extract() guard supports skip-if-exists for manually placed files.

1. Download from browser: https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip
2. Place at:
   backend/igem_data/downloads/HMDB/chemical_hmdb/hmdb_metabolites.zip
3. Run: poetry run igem-server etl run --source chemical_hmdb
"""

import json
import zipfile
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin
from igem_backend.modules.nlp.normalizer import normalize

_ZIP_FILE = "hmdb_metabolites.zip"
_XML_FILE = "hmdb_metabolites.xml"
_METABOLITE_TAG = "metabolite"

# Synonyms cap — HMDB records can have hundreds; keep the most common
_MAX_SYNONYMS = 50


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "chemical_hmdb"
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
            zip_path = landing / _ZIP_FILE
            xml_path = landing / _XML_FILE

            # HMDB sits behind a Cloudflare challenge that blocks non-browser
            # clients. If the operator has pre-placed the files (manual
            # download), reuse them instead of attempting the HTTP fetch.
            if xml_path.exists():
                xml_size = xml_path.stat().st_size
                file_hash = (
                    self._hash_file(zip_path) if zip_path.exists() else None
                )
                msg = (
                    f"[{self.DTP_NAME}] Extract skipped: reusing existing "
                    f"{_XML_FILE} ({xml_size:,} bytes) at {landing}"
                )
                self.logger.log(msg, "INFO")
                return (
                    True, msg, file_hash,
                    ETLStepStats(file_size_bytes=xml_size),
                )

            if zip_path.exists():
                self.logger.log(
                    f"Reusing existing {_ZIP_FILE} at {landing} "
                    "(skip download)", "INFO"
                )
            else:
                url = self.data_source.source_url
                self.logger.log(f"Downloading {url}", "INFO")
                try:
                    with requests.get(url, stream=True, timeout=1800) as resp:
                        resp.raise_for_status()
                        with open(zip_path, "wb") as fh:
                            for chunk in resp.iter_content(chunk_size=65536):
                                if chunk:
                                    fh.write(chunk)
                except requests.HTTPError as e:
                    # Clean up partial file; report actionable message.
                    if zip_path.exists():
                        zip_path.unlink()
                    msg = (
                        f"[{self.DTP_NAME}] Download failed ({e}). "
                        f"HMDB is behind a Cloudflare challenge. "
                        f"Download {_ZIP_FILE} manually from {url} "
                        f"and place it at {zip_path}, then re-run."
                    )
                    self.logger.log(msg, "ERROR")
                    return False, msg, None, ETLStepStats(errors=1)

            zip_size = zip_path.stat().st_size
            self.logger.log(
                f"  {_ZIP_FILE}: {zip_size:,} bytes — extracting...", "INFO"
            )

            with zipfile.ZipFile(zip_path, "r") as zf:
                # The zip may contain hmdb_metabolites.xml directly
                names = zf.namelist()
                xml_member = next(
                    (n for n in names if n.endswith(".xml")), None
                )
                if xml_member is None:
                    return (
                        False,
                        f"No .xml file found in {_ZIP_FILE} (members: {names})",
                        ETLStepStats(errors=1),
                    )
                with zf.open(xml_member) as src, open(xml_path, "wb") as dst:
                    for chunk in iter(lambda: src.read(65536), b""):
                        dst.write(chunk)

            xml_size = xml_path.stat().st_size
            file_hash = self._hash_file(zip_path)
            stats = ETLStepStats(file_size_bytes=xml_size)
            msg = (
                f"[{self.DTP_NAME}] Extract complete: "
                f"zip={zip_size:,} bytes, xml={xml_size:,} bytes"
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
            xml_path = self._dtp_dir(raw_dir) / _XML_FILE
            if not xml_path.exists():
                return (
                    False,
                    f"XML not found: {xml_path} — run extract first.",
                    ETLStepStats(errors=1),
                )

            records = []
            n_parsed = 0

            for _event, elem in ET.iterparse(
                xml_path, events=("end",)
            ):
                local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                if local != _METABOLITE_TAG:
                    continue

                rec = self._parse_metabolite(elem)
                if rec.get("accession"):
                    records.append(rec)
                    n_parsed += 1

                elem.clear()

                # if n_parsed % 10_000 == 0:
                #     self.logger.log(
                #         f"  Parsed {n_parsed:,} metabolites...", "INFO"
                #     )

            self.logger.log(
                f"  Parsed {n_parsed:,} metabolites total.", "INFO"
            )

            df = pd.DataFrame(records)
            df["molecular_weight"] = pd.to_numeric(
                df["molecular_weight"], errors="coerce"
            )

            out = self._dtp_dir(processed_dir)
            parquet_path = out / "master_data.parquet"
            df.to_parquet(parquet_path, index=False)
            if self.debug_mode:
                df.to_csv(out / "master_data.csv", index=False)

            stats = ETLStepStats(
                total=len(df),
                columns=len(df.columns),
                output_size_bytes=parquet_path.stat().st_size,
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df):,} metabolites"
            )
            self.logger.log(msg, "INFO")
            return True, msg, stats

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Transform failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, ETLStepStats(errors=1)

    # -------------------------------------------------------------------------
    # LOAD — enrich UniChem-seeded entities; fallback creates new entities
    # -------------------------------------------------------------------------
    def load(self, processed_dir: str) -> tuple[bool, str, ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Load starting...", "INFO")

        parquet_path = self._dtp_dir(processed_dir) / "master_data.parquet"
        if not parquet_path.exists():
            return (
                False,
                f"Processed file not found: {parquet_path}",
                ETLStepStats(errors=1),
            )

        try:
            df = pd.read_parquet(parquet_path, engine="pyarrow")
        except Exception as e:
            return (
                False,
                f"Could not read parquet: {e}",
                ETLStepStats(errors=1),
            )

        if df.empty:
            return True, "No metabolite rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_chemicals import ChemicalMaster
        from igem_backend.modules.db.models.model_entities import Entity, EntityAlias

        chem_type_id = self.get_entity_type_id("Chemicals")

        # --- Pre-load match + idempotency maps ---
        self.logger.log("  Loading InChIKey → entity map...", "INFO")
        inchi_to_entity: dict[str, int] = {
            row[0]: row[1]
            for row in self.session.query(
                ChemicalMaster.inchi_key, ChemicalMaster.entity_id
            ).filter(ChemicalMaster.inchi_key.isnot(None)).all()
            if row[0]
        }

        self.logger.log("  Loading HMDB xref → entity map...", "INFO")
        hmdb_xref_to_entity: dict[str, int] = {
            row[0]: row[1]
            for row in self.session.query(
                EntityAlias.alias_value, EntityAlias.entity_id
            ).filter(
                EntityAlias.xref_source == "HMDB",
                EntityAlias.alias_type == "code",
            ).all()
            if row[0]
        }

        self.logger.log("  Loading already-enriched entity set...", "INFO")
        already_enriched: set[int] = {
            row[0]
            for row in self.session.query(
                EntityAlias.entity_id
            ).filter(
                EntityAlias.xref_source == "HMDB",
                EntityAlias.alias_type == "preferred",
            ).distinct().all()
        }

        self.logger.log(
            f"  Lookups ready: inchi={len(inchi_to_entity):,} "
            f"hmdb_xref={len(hmdb_xref_to_entity):,} "
            f"already_enriched={len(already_enriched):,}",
            "INFO",
        )

        total = 0
        matched_inchi = 0
        matched_hmdb = 0
        created_new = 0
        already_done = 0
        warnings = 0
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1
            accession = str(row.get("accession") or "").strip()
            name = str(row.get("name") or "").strip()
            if not accession or not name:
                warnings += 1
                continue

            raw_inchi = str(row.get("inchi_key") or "").strip() or None

            # --- Match strategy ---
            entity_id: Optional[int] = None
            match_type: Optional[str] = None
            if raw_inchi and raw_inchi in inchi_to_entity:
                entity_id = inchi_to_entity[raw_inchi]
                match_type = "inchi"
            elif accession in hmdb_xref_to_entity:
                entity_id = hmdb_xref_to_entity[accession]
                match_type = "hmdb"

            # Idempotency guard
            if entity_id is not None and entity_id in already_enriched:
                already_done += 1
                continue

            # Stats for successful matches (only count after idempotency)
            if match_type == "inchi":
                matched_inchi += 1
            elif match_type == "hmdb":
                matched_hmdb += 1

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

            # --- Pre-load existing aliases for MATCHED entities to avoid
            #     unique-constraint collisions (e.g. UniChem already
            #     added code/HMDB for this accession).
            if match_type is not None:
                existing_alias_keys = {
                    (av, at, xs)
                    for av, at, xs in self.session.query(
                        EntityAlias.alias_value,
                        EntityAlias.alias_type,
                        EntityAlias.xref_source,
                    ).filter_by(entity_id=entity_id).all()
                }
                # Demote existing primary so HMDB preferred name takes slot
                self.session.query(EntityAlias).filter_by(
                    entity_id=entity_id, is_primary=True
                ).update({"is_primary": False})
            else:
                existing_alias_keys = set()

            seen_keys: set[tuple[str, str, str]] = set(existing_alias_keys)

            def _add(
                val: str,
                atype: str,
                xsrc: str,
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
                    entity_id=entity_id,
                    type_id=chem_type_id,
                    alias_value=val,
                    alias_type=atype,
                    xref_source=xsrc,
                    alias_norm=self.guard_alias_norm(normalize(val)),
                    is_primary=primary,
                    is_active=True,
                    locale="en",
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                ))

            # HMDB preferred name → primary (only one is_primary per entity)
            _add(name, "preferred", "HMDB", primary=True)
            # HMDB accession (code, not primary — preferred name wins)
            _add(accession, "code", "HMDB")
            # Secondary accessions (old 7-digit format)
            for sec in json.loads(row.get("secondary_accessions") or "[]"):
                _add(str(sec), "code", "HMDB")
            # Synonyms
            for syn in json.loads(row.get("synonyms") or "[]"):
                _add(str(syn), "synonym", "HMDB")
            # Cross-reference codes
            cas = str(row.get("cas_number") or "").strip()
            if cas:
                _add(cas, "code", "CAS")
            pubchem = str(row.get("pubchem_cid") or "").strip()
            if pubchem:
                _add(pubchem, "code", "PubChem")
            raw_chebi = str(row.get("chebi_id") or "").strip()
            chebi_full = (
                f"CHEBI:{raw_chebi}"
                if raw_chebi and not raw_chebi.upper().startswith("CHEBI:")
                else raw_chebi
            )
            if chebi_full:
                _add(chebi_full, "code", "ChEBI")
            kegg = str(row.get("kegg_id") or "").strip()
            if kegg:
                _add(kegg, "code", "KEGG")

            # --- ChemicalMaster: fill-null-only or create ---
            mol_w: Optional[float] = None
            try:
                raw_w = row.get("molecular_weight")
                if raw_w is not None and \
                        str(raw_w).strip() not in ("", "nan"):
                    mol_w = float(raw_w)
            except (ValueError, TypeError):
                pass

            smiles_raw = str(row.get("smiles") or "").strip() or None
            formula_raw = str(row.get("formula") or "").strip() or None

            cm = self.session.query(ChemicalMaster).filter_by(
                entity_id=entity_id
            ).one_or_none()

            if cm is None:
                cm = ChemicalMaster(
                    entity_id=entity_id,
                    chebi_id=chebi_full[:20] if chebi_full else None,
                    cas_number=cas[:20] if cas else None,
                    pubchem_cid=pubchem[:20] if pubchem else None,
                    inchi_key=raw_inchi[:27] if raw_inchi else None,
                    smiles=smiles_raw[:4000] if smiles_raw else None,
                    formula=formula_raw[:100] if formula_raw else None,
                    molecular_weight=mol_w,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(cm)
            else:
                if cm.chebi_id is None and chebi_full:
                    cm.chebi_id = chebi_full[:20]
                if cm.cas_number is None and cas:
                    cm.cas_number = cas[:20]
                if cm.pubchem_cid is None and pubchem:
                    cm.pubchem_cid = pubchem[:20]
                if cm.inchi_key is None and raw_inchi:
                    cm.inchi_key = raw_inchi[:27]
                if cm.smiles is None and smiles_raw:
                    cm.smiles = smiles_raw[:4000]
                if cm.formula is None and formula_raw:
                    cm.formula = formula_raw[:100]
                if cm.molecular_weight is None and mol_w is not None:
                    cm.molecular_weight = mol_w

            already_enriched.add(entity_id)
            # Keep lookup maps fresh for subsequent rows sharing the
            # same InChIKey or accession
            if raw_inchi and raw_inchi not in inchi_to_entity:
                inchi_to_entity[raw_inchi] = entity_id
            if accession not in hmdb_xref_to_entity:
                hmdb_xref_to_entity[accession] = entity_id

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    if (i + 1) % (BATCH * 20) == 0:
                        self.logger.log(
                            f"  Committed {i + 1:,}/{total:,} "
                            f"(inchi={matched_inchi:,} "
                            f"hmdb={matched_hmdb:,} "
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

        self._log_trunc_summary()
        stats = ETLStepStats(
            total=total,
            created=matched_inchi + matched_hmdb + created_new,
            skipped=already_done,
            warnings=warnings,
            extras={
                "matched_inchi":     matched_inchi,
                "matched_hmdb_xref": matched_hmdb,
                "created_new":       created_new,
                "already_enriched":  already_done,
            },
        )
        msg = (
            f"[{self.DTP_NAME}] Enrich complete: "
            f"total={total:,} "
            f"inchi={matched_inchi:,} "
            f"hmdb_xref={matched_hmdb:,} "
            f"new={created_new:,} "
            f"already={already_done:,} "
            f"warn={warnings:,}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats

    # -------------------------------------------------------------------------
    # XML parsing helper
    # -------------------------------------------------------------------------
    @staticmethod
    def _parse_metabolite(elem: ET.Element) -> dict:
        """Extract fields from a single <metabolite> element.

        HMDB XML declares a default namespace (xmlns="http://www.hmdb.ca"),
        so every element tag carries that namespace prefix after parsing.
        The {*} wildcard matches any namespace without requiring an
        explicit {...} map — works cleanly whether HMDB ships a
        namespaced or namespace-free XML dump.
        """

        def _text(tag: str) -> str:
            node = elem.find(f"{{*}}{tag}")
            return (node.text or "").strip() if node is not None else ""

        accession = _text("accession")

        secondary = [
            n.text.strip()
            for n in elem.findall(
                "{*}secondary_accessions/{*}accession"
            )
            if n.text and n.text.strip()
        ]

        synonyms = [
            n.text.strip()
            for n in elem.findall("{*}synonyms/{*}synonym")
            if n.text and n.text.strip()
        ][:_MAX_SYNONYMS]

        # Strip "InChIKey=" prefix present in some HMDB versions
        raw_inchi = _text("inchikey")
        inchi_key = raw_inchi.replace("InChIKey=", "").strip() or None

        return {
            "accession":            accession,
            "name":                 _text("name"),
            "description":          _text("description"),
            "synonyms":             json.dumps(synonyms),
            "secondary_accessions": json.dumps(secondary),
            "formula":              _text("chemical_formula") or None,
            "molecular_weight":     _text("average_molecular_weight") or None,
            "smiles":               _text("smiles") or None,
            "inchi_key":            inchi_key,
            "cas_number":           _text("cas_registry_number") or None,
            "pubchem_cid":          _text("pubchem_compound_id") or None,
            "chebi_id":             _text("chebi_id") or None,
            "kegg_id":              _text("kegg_id") or None,
            "state":                _text("state") or None,
        }
