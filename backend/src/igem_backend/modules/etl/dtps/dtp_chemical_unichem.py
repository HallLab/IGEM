"""
UniChem Chemical DTP — master source of chemical identity.

Pipeline role
-------------
First DTP in the chemical master chain. Creates one Entity per unique
molecular structure (keyed by Standard InChIKey) and populates
EntityAlias rows with cross-references to all biomedical databases
that UniChem maps.

Downstream DTPs (chemical_chebi, chemical_hmdb, chemical_mesh) are
"enrichers": they match existing entities by InChIKey (or cross-reference
code) and attach names, synonyms, SMILES, descriptions, etc.

Source
------
EMBL-EBI UniChem FTP dumps:
  source.tsv.gz     ~2.5 KB   — 22 source databases (ChEBI, HMDB, DrugBank, …)
  reference.tsv.gz  ~1.4 GB   — UCI → (src_id, src_compound_id) mapping
  structure.tsv.gz  ~12 GB    — UCI → StandardInChI + StandardInChIKey

https://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/table_dumps/

What is loaded
--------------
  Entity  (type=Chemicals)
  ChemicalMaster (inchi_key populated; smiles/formula/mw filled later by ChEBI)
  EntityAlias rows:
    - code/UniChem    → UCI                      (is_primary=True initially)
    - code/InChIKey   → StandardInChIKey         (queryable structural ID)
    - code/ChEBI      → CHEBI:<id>               (when present in xrefs)
    - code/HMDB       → HMDB<id>                 (when present in xrefs)
    - code/DrugBank   → DB<id>                   (…)
    - code/PubChem, LipidMaps, GtoPdb, … (any xref from UniChem is preserved)

The primary alias is moved from `code/UniChem` to the preferred name by
the ChEBI/HMDB enrichers as they attach human-readable names.

Filter preset
-------------
This DTP loads only UCIs that appear in AT LEAST ONE of the sources in
INCLUDED_SRC_IDS below (preset A — "minimal biomedical"):
    ChEBI, HMDB, LipidMaps, DrugBank, DrugCentral, GtoPdb,
    FDA-SRS, Rhea, Probes&Drugs

Expected output: ~400k–500k entities.

Xrefs from other sources (PubChem, SwissLipids, CompTox, etc.) ARE
preserved for any UCI that passes the filter — wider xref coverage is
cheap and useful.

Future expansion
----------------
  Preset B — add SwissLipids (41): +~1M lipid isomers for comprehensive
             lipid GWAS coverage
  Preset C — add CompTox (32):     +~900k environmental chemicals for
             exposome studies

To switch presets, edit INCLUDED_SRC_IDS below and re-run. Entities
already loaded are preserved; only new InChIKeys are added.

Operational notes
-----------------
  - `extract()` supports skip-if-exists: pre-placed files under
    <raw_dir>/UniChem/chemical_unichem/ are reused instead of re-downloaded.
  - `transform()` streams the 12GB structure file line-by-line (constant
    memory w.r.t. file size; peak RAM ~300MB for the xref index).
  - `load()` is idempotent: existing InChIKey-keyed entities are skipped.
"""

from __future__ import annotations

import gzip
import json
from collections import defaultdict
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin
from igem_backend.modules.nlp.normalizer import normalize

_BASE_URL = (
    "https://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/table_dumps/"
)
_SOURCE_FILE = "source.tsv.gz"
_REFERENCE_FILE = "reference.tsv.gz"
_STRUCTURE_FILE = "structure.tsv.gz"

# Filter preset A — minimal biomedical. Any UCI with ≥1 xref to one of
# these sources is loaded. See module docstring for presets B, C.
INCLUDED_SRC_IDS: set[int] = {
    7,   # ChEBI         — curated chemical ontology
    18,  # HMDB          — metabolites
    33,  # LIPID MAPS    — curated lipids
    2,   # DrugBank      — drugs
    34,  # DrugCentral   — approved drugs
    4,   # GtoPdb        — pharmacology ligands
    14,  # FDA-SRS       — FDA unique ingredient identifiers (UNII)
    38,  # Rhea          — enzyme reactions
    49,  # Probes&Drugs  — chemical probes
}

# Canonical xref_source names written to EntityAlias.xref_source.
# Covers all 22 UniChem sources so any xref on a loaded UCI is preserved
# with a consistent label, even for sources outside the filter.
_XREF_NAME: dict[int, str] = {
    1:  "ChEMBL",
    2:  "DrugBank",
    3:  "RCSB-PDB",
    4:  "GtoPdb",
    5:  "PDBe",
    7:  "ChEBI",
    14: "FDA-SRS",
    15: "SureChEMBL",
    18: "HMDB",
    22: "PubChem",
    24: "NMRShiftDB",
    28: "MolPort",
    31: "BindingDB",
    32: "CompTox",
    33: "LipidMaps",
    34: "DrugCentral",
    37: "Brenda",
    38: "Rhea",
    41: "SwissLipids",
    46: "ClinicalTrials",
    49: "ProbesAndDrugs",
    50: "CCDC",
}


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "chemical_unichem"
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
    # EXTRACT — download 3 files (source, reference, structure)
    # -------------------------------------------------------------------------
    def extract(
        self, raw_dir: str
    ) -> tuple[bool, str, Optional[str], ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Extract starting...", "INFO")
        try:
            landing = self._dtp_dir(raw_dir)
            files = [_SOURCE_FILE, _REFERENCE_FILE, _STRUCTURE_FILE]

            total_bytes = 0
            for fname in files:
                dst = landing / fname
                if dst.exists():
                    size = dst.stat().st_size
                    self.logger.log(
                        f"  Reusing existing {fname} ({size:,} bytes)",
                        "INFO",
                    )
                    total_bytes += size
                    continue

                url = _BASE_URL + fname
                self.logger.log(f"  Downloading {url}", "INFO")
                with requests.get(url, stream=True, timeout=3600) as resp:
                    resp.raise_for_status()
                    with open(dst, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=1 << 20):
                            if chunk:
                                fh.write(chunk)
                size = dst.stat().st_size
                self.logger.log(
                    f"  {fname}: {size:,} bytes", "INFO"
                )
                total_bytes += size

            # Hash the reference file (smaller, stable across structure
            # re-dumps that only add new UCIs)
            file_hash = self._hash_file(landing / _REFERENCE_FILE)
            stats = ETLStepStats(file_size_bytes=total_bytes)
            msg = (
                f"[{self.DTP_NAME}] Extract complete: "
                f"{total_bytes:,} bytes across 3 files"
            )
            self.logger.log(msg, "INFO")
            return True, msg, file_hash, stats

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Extract failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, None, ETLStepStats(errors=1)

    # -------------------------------------------------------------------------
    # TRANSFORM — stream reference + structure, emit one parquet row per UCI
    # -------------------------------------------------------------------------
    def transform(
        self, raw_dir: str, processed_dir: str
    ) -> tuple[bool, str, ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Transform starting...", "INFO")
        try:
            landing = self._dtp_dir(raw_dir)
            ref_path = landing / _REFERENCE_FILE
            struct_path = landing / _STRUCTURE_FILE
            if not ref_path.exists() or not struct_path.exists():
                return (
                    False,
                    f"Input files missing at {landing} — run extract first.",
                    ETLStepStats(errors=1),
                )

            # --- Pass 1a: find UCIs that have ≥1 xref in INCLUDED_SRC_IDS ---
            # Memory: a set of ints, O(passed UCIs) ~ ~500k entries = trivial.
            self.logger.log(
                "  Pass 1a/3: scanning reference for included UCIs...", "INFO"
            )
            passing: set[int] = set()
            n_lines = 0

            with gzip.open(ref_path, "rt", encoding="utf-8") as fh:
                fh.readline()  # skip header
                for line in fh:
                    n_lines += 1
                    parts = line.split("\t", 3)
                    if len(parts) < 3:
                        continue
                    try:
                        src_id = int(parts[1])
                    except ValueError:
                        continue
                    if src_id not in INCLUDED_SRC_IDS:
                        continue
                    try:
                        passing.add(int(parts[0]))
                    except ValueError:
                        continue

                    if n_lines % 20_000_000 == 0:
                        self.logger.log(
                            f"    {n_lines:,} rows scanned, "
                            f"{len(passing):,} UCIs matched filter",
                            "INFO",
                        )

            n_passed = len(passing)
            self.logger.log(
                f"  Pass 1a done: {n_lines:,} rows → "
                f"{n_passed:,} UCIs pass filter",
                "INFO",
            )
            if n_passed == 0:
                return (
                    True,
                    "No UniChem records matched filter — nothing to write.",
                    ETLStepStats(),
                )

            # --- Pass 1b: collect xrefs (all sources) for passing UCIs ---
            # Memory: ~500k UCIs × ~10 xrefs × ~40 B = ~200 MB.
            self.logger.log(
                "  Pass 1b/3: collecting xrefs for passing UCIs...", "INFO"
            )
            filtered: dict[int, list[tuple[int, str]]] = defaultdict(list)
            n_lines = 0

            with gzip.open(ref_path, "rt", encoding="utf-8") as fh:
                fh.readline()
                for line in fh:
                    n_lines += 1
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) < 3:
                        continue
                    try:
                        uci = int(parts[0])
                    except ValueError:
                        continue
                    if uci not in passing:
                        continue
                    try:
                        src_id = int(parts[1])
                    except ValueError:
                        continue
                    compound_id = parts[2].strip()
                    if not compound_id:
                        continue
                    filtered[uci].append((src_id, compound_id))

                    if n_lines % 20_000_000 == 0:
                        self.logger.log(
                            f"    {n_lines:,} rows re-scanned...", "INFO"
                        )
            del passing  # release memory

            self.logger.log(
                f"  Pass 1b done: xrefs indexed for {len(filtered):,} UCIs",
                "INFO",
            )

            # --- Pass 2: scan structure.tsv.gz, emit records per UCI ---
            self.logger.log(
                f"  Pass 2/3: extracting InChIKey for {n_passed:,} UCIs...",
                "INFO",
            )
            records: list[dict] = []
            seen: set[int] = set()
            n_struct = 0

            with gzip.open(struct_path, "rt", encoding="utf-8") as fh:
                header = fh.readline()  # skip header
                del header
                for line in fh:
                    n_struct += 1
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) < 3:
                        continue
                    try:
                        uci = int(parts[0])
                    except ValueError:
                        continue
                    if uci not in filtered or uci in seen:
                        continue
                    inchi = parts[1].strip()
                    inchi_key = parts[2].strip()
                    if not inchi_key:
                        continue

                    xrefs_list = [
                        {"src_id": sid, "compound_id": cid}
                        for sid, cid in filtered[uci]
                    ]
                    records.append({
                        "uci":         uci,
                        "inchi_key":   inchi_key,
                        "inchi":       inchi,
                        "xrefs_json":  json.dumps(xrefs_list),
                    })
                    seen.add(uci)

                    if n_struct % 10_000_000 == 0:
                        self.logger.log(
                            f"    {n_struct:,} structure rows scanned, "
                            f"{len(records):,} records emitted",
                            "INFO",
                        )

            self.logger.log(
                f"  Pass 2 done: {n_struct:,} structure rows → "
                f"{len(records):,} records",
                "INFO",
            )

            if not records:
                return (
                    True,
                    "No UniChem records matched filter — nothing to write.",
                    ETLStepStats(),
                )

            df = pd.DataFrame(records)

            out = self._dtp_dir(processed_dir)
            parquet_path = out / "master_data.parquet"
            df.to_parquet(parquet_path, index=False)
            if self.debug_mode:
                df.head(1000).to_csv(
                    out / "master_data_sample.csv", index=False
                )

            stats = ETLStepStats(
                total=len(df),
                columns=len(df.columns),
                output_size_bytes=parquet_path.stat().st_size,
                extras={
                    "ref_rows_scanned": n_lines,
                    "struct_rows_scanned": n_struct,
                    "uci_passed_filter": n_passed,
                    "entities_to_load": len(df),
                },
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df):,} UCIs → master_data.parquet"
            )
            self.logger.log(msg, "INFO")
            return True, msg, stats

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Transform failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, ETLStepStats(errors=1)

    # -------------------------------------------------------------------------
    # LOAD — create Entity + ChemicalMaster + EntityAlias rows
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
            return True, "No UniChem rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_chemicals import (
            ChemicalMaster,
        )
        from igem_backend.modules.db.models.model_entities import (
            Entity,
            EntityAlias,
        )

        chem_type_id = self.get_entity_type_id("Chemicals")

        # Pre-load existing InChIKey → entity_id map (idempotency)
        existing_by_inchi: dict[str, int] = {
            row[0]: row[1]
            for row in self.session.query(
                ChemicalMaster.inchi_key, ChemicalMaster.entity_id
            ).filter(ChemicalMaster.inchi_key.isnot(None)).all()
            if row[0]
        }
        self.logger.log(
            f"  {len(existing_by_inchi):,} entities already keyed by InChIKey",
            "INFO",
        )

        total = created = skipped = warnings = 0
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1
            inchi_key = str(row.get("inchi_key") or "").strip()
            if not inchi_key:
                warnings += 1
                continue

            if inchi_key in existing_by_inchi:
                skipped += 1
                continue

            uci = str(row.get("uci") or "").strip()
            xrefs_raw = row.get("xrefs_json") or "[]"
            try:
                xrefs = json.loads(xrefs_raw)
            except (ValueError, TypeError):
                xrefs = []

            # --- Entity ---
            entity = Entity(
                type_id=chem_type_id,
                is_active=True,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(entity)
            self.session.flush()

            # --- Aliases ---
            seen_keys: set[tuple[str, str, str]] = set()

            def _add(val: str, atype: str, xsrc: str, primary: bool = False):
                val = (self.guard_alias(val) or "").strip()
                if not val:
                    return
                key = (val, atype, xsrc)
                if key in seen_keys:
                    return
                seen_keys.add(key)
                self.session.add(EntityAlias(
                    entity_id=entity.id,
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

            # UCI as primary (enrichers promote preferred name later)
            _add(uci, "code", "UniChem", primary=True)
            # InChIKey as queryable structural alias
            _add(inchi_key, "code", "InChIKey")

            # Per-source xrefs
            for xr in xrefs:
                src_id = xr.get("src_id")
                cid = str(xr.get("compound_id") or "").strip()
                if not cid or src_id is None:
                    continue
                src_name = _XREF_NAME.get(int(src_id))
                if not src_name:
                    continue
                # ChEBI convention: prefix with "CHEBI:" when bare numeric
                if src_name == "ChEBI" and \
                        not cid.upper().startswith("CHEBI:"):
                    cid = f"CHEBI:{cid}"
                _add(cid, "code", src_name)

            # --- ChemicalMaster ---
            chebi_bare = None
            pubchem_cid = None
            for xr in xrefs:
                sid = xr.get("src_id")
                cv = str(xr.get("compound_id") or "").strip()
                if sid == 7 and cv and not chebi_bare:
                    chebi_bare = (
                        cv if cv.upper().startswith("CHEBI:")
                        else f"CHEBI:{cv}"
                    )
                if sid == 22 and cv and not pubchem_cid:
                    pubchem_cid = cv

            self.session.add(ChemicalMaster(
                entity_id=entity.id,
                chebi_id=chebi_bare[:20] if chebi_bare else None,
                pubchem_cid=pubchem_cid[:20] if pubchem_cid else None,
                inchi_key=inchi_key[:27],
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            ))
            existing_by_inchi[inchi_key] = entity.id
            created += 1

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    if (i + 1) % (BATCH * 20) == 0:
                        self.logger.log(
                            f"  Committed {i + 1:,}/{total:,} "
                            f"(created={created:,} skipped={skipped:,})",
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
            f"total={total:,} created={created:,} "
            f"skipped={skipped:,} warnings={warnings:,}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats
