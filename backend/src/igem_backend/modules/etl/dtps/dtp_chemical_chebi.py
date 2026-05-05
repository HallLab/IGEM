"""
ChEBI Chemical DTP — enricher for UniChem-seeded entities.

Pipeline role
-------------
Runs AFTER dtp_chemical_unichem. Matches each ChEBI compound against an
existing Entity (created by UniChem) and enriches it with:

  - compound name (preferred/ChEBI alias, promoted to is_primary)
  - secondary ChEBI codes
  - CAS / PubChem / KEGG / InChIKey and ~30 other xrefs
  - ChemicalMaster fields that UniChem left NULL: smiles, formula,
    molecular_weight, cas_number, pubchem_cid
  - inchi_key / chebi_id only when missing (UniChem usually already has them)

Match strategy (first that wins)
--------------------------------
  1. InChIKey match against existing ChemicalMaster.inchi_key  (strong)
  2. CHEBI:id match against existing EntityAlias
     (xref_source=ChEBI, alias_type=code)                        (medium)
  3. Fallback — create new Entity (for ChEBI compounds UniChem did not
     load because they fell outside the UniChem preset filter)

Fill-null-only semantics
------------------------
When enriching an existing ChemicalMaster, ChEBI never OVERWRITES fields
that are already populated. This preserves UniChem as the initial source
of truth for structural identity (InChIKey, chebi_id, pubchem_cid).

Idempotency
-----------
Entities that already carry a `preferred/ChEBI` alias are skipped on
re-run. To force re-enrichment, delete those aliases first.

Source files (ChEBI FTP flat files)
-----------------------------------
  compounds.tsv.gz          — primary compound records (ID, name, status)
  chemical_data.tsv.gz      — molecular properties (FORMULA, MASS,
                              MONOISOTOPIC MASS, SMILES, InChIKey)
  secondary_ids.tsv.gz      — deprecated ChEBI IDs → current compounds
  database_accession.tsv.gz — external cross-references
  source.tsv.gz             — lookup table for xref source names

Status mapping
--------------
  status 'C' / 'E' / 'S'    → entity marked active
  anything else             → entity marked is_active=False
"""

import json
from io import BytesIO
from pathlib import Path
from typing import Optional
from zipfile import BadZipFile, ZipFile

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin
from igem_backend.modules.nlp.normalizer import normalize

_BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/chebi/flat_files/"
_FILES = [
    "compounds.tsv.gz",
    "chemical_data.tsv.gz",
    "secondary_ids.tsv.gz",
    "database_accession.tsv.gz",
    "source.tsv.gz",
]

# ChEBI status codes that represent active/curated compounds
_ACTIVE_STATUSES = {"C", "E", "S"}

# Molecular property types in chemical_data.tsv (long format)
_PROP_TYPES = {
    "FORMULA":           "formula",
    "CHARGE":            "charge",
    "MASS":              "mass",
    "MONOISOTOPIC MASS": "monoisotopic_mass",
    "SMILES":            "smiles",
    "InChIKey":          "inchi_key",
}

# External source prefixes to store as EntityAlias.xref_source
# and (optionally) map to ChemicalMaster columns
_XREF_COL: dict[str, str] = {
    "CAS Registry Number": "cas_number",
    "PubChem":             "pubchem_cid",
}

# How many xref aliases to write per compound (cap to avoid bloat)
_MAX_XREF_ALIASES = 30


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_chemical_chebi"
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
            total_bytes = 0

            for fname in _FILES:
                url = _BASE_URL + fname
                dest = landing / fname
                self.logger.log(f"Downloading {url}", "INFO")
                with requests.get(url, stream=True, timeout=600) as resp:
                    resp.raise_for_status()
                    with open(dest, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=65536):
                            if chunk:
                                fh.write(chunk)
                total_bytes += dest.stat().st_size
                self.logger.log(
                    f"  {fname}: {dest.stat().st_size:,} bytes", "INFO"
                )

            # Hash compounds file as the canonical change fingerprint
            file_hash = self._hash_file(landing / "compounds.tsv.gz")
            stats = ETLStepStats(
                file_size_bytes=total_bytes,
                extras={"files": len(_FILES)},
            )
            msg = (
                f"[{self.DTP_NAME}] Extract complete: "
                f"{len(_FILES)} files, {total_bytes:,} bytes total"
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
            src = self._dtp_dir(raw_dir)
            out = self._dtp_dir(processed_dir)

            # ---- 1. compounds.tsv.gz ----
            compounds_file = src / "compounds.tsv.gz"
            if not compounds_file.exists():
                return (
                    False,
                    f"Missing: {compounds_file}",
                    ETLStepStats(errors=1),
                )
            df_cpd = pd.read_csv(
                compounds_file, sep="\t", compression="gzip", low_memory=False
            )
            # Normalise column names (ChEBI uses uppercase headers)
            df_cpd.columns = [c.strip().lower() for c in df_cpd.columns]
            # Rename canonical columns
            rename = {}
            for col in df_cpd.columns:
                if col in ("chebi_accession", "id"):
                    rename[col] = col  # keep as-is for now
                if col == "name":
                    rename[col] = "label"
            df_cpd = df_cpd.rename(columns=rename)

            # Ensure chebi_id column
            if "chebi_accession" in df_cpd.columns:
                df_cpd = df_cpd.rename(columns={"chebi_accession": "chebi_id"})

            # compound_id is the numeric ID used for joins
            # ChEBI uses 'id' as the numeric compound id in flat files
            if "id" in df_cpd.columns and "compound_id" not in df_cpd.columns:
                df_cpd = df_cpd.rename(columns={"id": "compound_id"})
            df_cpd["compound_id"] = pd.to_numeric(
                df_cpd["compound_id"], errors="coerce"
            ).astype("Int64")

            # Keep only compounds with a valid CHEBI id and label
            df_cpd = df_cpd.dropna(subset=["chebi_id", "label"])
            df_cpd = df_cpd[df_cpd["chebi_id"].str.startswith("CHEBI:")]

            # Status: 'C' checked, 'E' entry, 'S' starred
            if "status" not in df_cpd.columns:
                df_cpd["status"] = "E"

            self.logger.log(
                f"  compounds: {len(df_cpd):,} records after filter", "INFO"
            )

            # ---- 2. chemical_data.tsv.gz (long → wide pivot) ----
            chem_data_file = src / "chemical_data.tsv.gz"
            df_wide = pd.DataFrame({"compound_id": df_cpd["compound_id"].unique()})

            if chem_data_file.exists():
                df_cd = pd.read_csv(
                    chem_data_file, sep="\t", compression="gzip", low_memory=False
                )
                df_cd.columns = [c.strip() for c in df_cd.columns]

                # Detect long vs wide format
                if "TYPE" in df_cd.columns and "CHEMICAL_DATA" in df_cd.columns:
                    # Long format — pivot on TYPE
                    df_cd = df_cd[df_cd["TYPE"].isin(_PROP_TYPES)].copy()
                    df_cd["prop"] = df_cd["TYPE"].map(_PROP_TYPES)
                    df_cd["COMPOUND_ID"] = pd.to_numeric(
                        df_cd["COMPOUND_ID"], errors="coerce"
                    ).astype("Int64")
                    # Keep first occurrence per (compound_id, prop)
                    df_cd = df_cd.drop_duplicates(subset=["COMPOUND_ID", "prop"])
                    df_pivot = df_cd.pivot(
                        index="COMPOUND_ID", columns="prop", values="CHEMICAL_DATA"
                    ).reset_index()
                    df_pivot = df_pivot.rename(columns={"COMPOUND_ID": "compound_id"})
                    df_wide = df_wide.merge(df_pivot, on="compound_id", how="left")
                else:
                    # Already wide (older ChEBI export)
                    df_cd.columns = [c.lower() for c in df_cd.columns]
                    if "compound_id" not in df_cd.columns and "id" in df_cd.columns:
                        df_cd = df_cd.rename(columns={"id": "compound_id"})
                    df_cd["compound_id"] = pd.to_numeric(
                        df_cd["compound_id"], errors="coerce"
                    ).astype("Int64")
                    keep = [c for c in _PROP_TYPES.values() if c in df_cd.columns]
                    df_wide = df_wide.merge(
                        df_cd[["compound_id"] + keep], on="compound_id", how="left"
                    )

            # Merge compound props into main frame
            df = df_cpd.merge(df_wide, on="compound_id", how="left")

            # ---- 3. secondary_ids.tsv.gz ----
            sec_file = src / "secondary_ids.tsv.gz"
            secondary_by_cid: dict[int, list[str]] = {}
            if sec_file.exists():
                df_sec = pd.read_csv(
                    sec_file, sep="\t", compression="gzip", low_memory=False
                )
                df_sec.columns = [c.strip().lower() for c in df_sec.columns]
                if "compound_id" in df_sec.columns and "secondary_id" in df_sec.columns:
                    df_sec["compound_id"] = pd.to_numeric(
                        df_sec["compound_id"], errors="coerce"
                    )
                    df_sec["secondary_id"] = df_sec["secondary_id"].astype(str)
                    # Normalise to CHEBI:xxx format
                    df_sec["secondary_id"] = df_sec["secondary_id"].apply(
                        lambda x: x if x.startswith("CHEBI:") else f"CHEBI:{x}"
                    )
                    for cid, grp in df_sec.groupby("compound_id"):
                        secondary_by_cid[int(cid)] = grp["secondary_id"].tolist()

            # ---- 4. database_accession.tsv.gz + source.tsv.gz ----
            accession_file = src / "database_accession.tsv.gz"
            source_file = src / "source.tsv.gz"
            xrefs_by_cid: dict[int, list[dict]] = {}

            if accession_file.exists() and source_file.exists():
                df_src = pd.read_csv(
                    source_file, sep="\t", compression="gzip", low_memory=False
                )
                df_src.columns = [c.strip().lower() for c in df_src.columns]
                src_name_by_id: dict[int, str] = {}
                if "id" in df_src.columns and "name" in df_src.columns:
                    for _, r in df_src.iterrows():
                        try:
                            src_name_by_id[int(r["id"])] = str(r["name"])
                        except (ValueError, TypeError):
                            pass

                df_acc = pd.read_csv(
                    accession_file, sep="\t", compression="gzip", low_memory=False
                )
                df_acc.columns = [c.strip().lower() for c in df_acc.columns]
                if (
                    "compound_id" in df_acc.columns
                    and "accession_number" in df_acc.columns
                    and "source_id" in df_acc.columns
                ):
                    df_acc["compound_id"] = pd.to_numeric(
                        df_acc["compound_id"], errors="coerce"
                    )
                    df_acc["src_name"] = df_acc["source_id"].apply(
                        lambda x: src_name_by_id.get(int(x), "Unknown")
                        if pd.notna(x)
                        else "Unknown"
                    )
                    for cid, grp in df_acc.groupby("compound_id"):
                        entries = []
                        for _, r in grp.iterrows():
                            val = str(r.get("accession_number", "") or "").strip()
                            src_name = str(r.get("src_name", "") or "").strip()
                            if val and src_name and src_name != "Unknown":
                                entries.append(
                                    {"alias_value": val, "xref_source": src_name}
                                )
                        if entries:
                            xrefs_by_cid[int(cid)] = entries

            # ---- 5. Assemble master_data ----
            # Attach secondary ids and xrefs as JSON strings
            df["secondary_ids"] = df["compound_id"].apply(
                lambda x: json.dumps(secondary_by_cid.get(int(x), []))
                if pd.notna(x)
                else "[]"
            )
            df["xrefs_extra"] = df["compound_id"].apply(
                lambda x: json.dumps(xrefs_by_cid.get(int(x), []))
                if pd.notna(x)
                else "[]"
            )

            # Normalise numeric columns
            for col in ("mass", "monoisotopic_mass", "charge"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Select output columns (only those that exist)
            keep_cols = [
                "chebi_id", "label", "status", "compound_id",
                "formula", "charge", "mass", "monoisotopic_mass",
                "smiles", "inchi_key",
                "secondary_ids", "xrefs_extra",
            ]
            if "definition" in df.columns:
                keep_cols.insert(2, "definition")

            out_cols = [c for c in keep_cols if c in df.columns]
            df_out = df[out_cols].drop_duplicates(subset=["chebi_id"])

            master_parquet = out / "master_data.parquet"
            df_out.to_parquet(master_parquet, index=False)
            if self.debug_mode:
                df_out.to_csv(out / "master_data.csv", index=False)

            stats = ETLStepStats(
                total=len(df_out),
                columns=len(df_out.columns),
                output_size_bytes=master_parquet.stat().st_size,
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df_out):,} compounds"
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
            return True, "No chemical rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_chemicals import (
            ChemicalMaster,
        )
        from igem_backend.modules.db.models.model_entities import (
            Entity,
            EntityAlias,
        )

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

        self.logger.log("  Loading ChEBI xref → entity map...", "INFO")
        chebi_xref_to_entity: dict[str, int] = {
            row[0]: row[1]
            for row in self.session.query(
                EntityAlias.alias_value, EntityAlias.entity_id
            ).filter(
                EntityAlias.xref_source == "ChEBI",
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
                EntityAlias.xref_source == "ChEBI",
                EntityAlias.alias_type == "preferred",
            ).distinct().all()
        }

        self.logger.log(
            f"  Lookups ready: inchi={len(inchi_to_entity):,} "
            f"chebi_xref={len(chebi_xref_to_entity):,} "
            f"already_enriched={len(already_enriched):,}",
            "INFO",
        )

        total = 0
        matched_inchi = 0
        matched_chebi = 0
        created_new = 0
        already_done = 0
        warnings = 0
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1

            chebi_id = str(row.get("chebi_id") or "").strip()
            label = str(row.get("label") or "").strip()
            if not chebi_id or not label:
                warnings += 1
                continue

            status = str(row.get("status") or "E").strip().upper()
            is_active = status in _ACTIVE_STATUSES

            inchi_key_raw = (
                str(row.get("inchi_key") or "").strip() or None
            )
            smiles_raw = str(row.get("smiles") or "").strip() or None
            formula_raw = str(row.get("formula") or "").strip() or None

            mol_weight: Optional[float] = None
            raw_mass = row.get("mass")
            if raw_mass is not None and \
                    str(raw_mass).strip() not in ("", "nan"):
                try:
                    mol_weight = float(raw_mass)
                except (ValueError, TypeError):
                    pass

            # --- Match strategy ---
            entity_id: Optional[int] = None
            match_type: Optional[str] = None
            if inchi_key_raw and inchi_key_raw in inchi_to_entity:
                entity_id = inchi_to_entity[inchi_key_raw]
                match_type = "inchi"
            elif chebi_id in chebi_xref_to_entity:
                entity_id = chebi_xref_to_entity[chebi_id]
                match_type = "chebi"

            # Idempotency guard
            if entity_id is not None and entity_id in already_enriched:
                already_done += 1
                continue

            # Stats for successful matches (only count after idempotency)
            if match_type == "inchi":
                matched_inchi += 1
            elif match_type == "chebi":
                matched_chebi += 1

            # Fallback: create new entity
            if entity_id is None:
                entity = Entity(
                    type_id=chem_type_id,
                    is_active=is_active,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(entity)
                self.session.flush()
                entity_id = entity.id
                created_new += 1

            # --- Pre-load existing aliases on MATCHED entities only ---
            # Newly-created entities have no prior aliases, so we can skip
            # the lookup for them. This avoids unique-constraint collisions
            # when xrefs overlap across sources (e.g. UniChem already added
            # code/ChEBI for this compound).
            if match_type is not None:
                existing_alias_keys = {
                    (av, at, xs)
                    for av, at, xs in self.session.query(
                        EntityAlias.alias_value,
                        EntityAlias.alias_type,
                        EntityAlias.xref_source,
                    ).filter_by(entity_id=entity_id).all()
                }
                # Demote UniChem's primary so ChEBI's preferred name
                # can take the primary slot.
                self.session.query(EntityAlias).filter_by(
                    entity_id=entity_id, is_primary=True
                ).update({"is_primary": False})
            else:
                existing_alias_keys = set()

            seen_keys: set[tuple[str, str, str]] = set(existing_alias_keys)

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
                    entity_id=entity_id,
                    type_id=chem_type_id,
                    alias_value=val,
                    alias_type=atype,
                    xref_source=xsrc,
                    alias_norm=self.guard_alias_norm(
                        norm or normalize(val)
                    ),
                    is_primary=primary,
                    is_active=is_active,
                    locale="en",
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                ))

            # ChEBI's preferred name takes the is_primary slot
            _add_alias(label, "preferred", "ChEBI", primary=True)
            # ChEBI code (if UniChem already added it, the dedup set skips)
            _add_alias(chebi_id, "code", "ChEBI")
            # Secondary ChEBI IDs
            for sec_id in json.loads(row.get("secondary_ids") or "[]"):
                if sec_id:
                    _add_alias(sec_id, "code", "ChEBI")
            # External xrefs
            xrefs_extra = json.loads(row.get("xrefs_extra") or "[]")
            for xr in xrefs_extra[:_MAX_XREF_ALIASES]:
                val = str(xr.get("alias_value") or "").strip()
                src_name = str(xr.get("xref_source") or "").strip()
                if val and src_name:
                    _add_alias(val, "code", src_name)

            # --- Extract ChemicalMaster scalar values from xrefs ---
            cas_number: Optional[str] = None
            pubchem_cid: Optional[str] = None
            for xr in xrefs_extra:
                src_name = str(xr.get("xref_source") or "").strip()
                val = str(xr.get("alias_value") or "").strip()
                if not val:
                    continue
                if src_name == "CAS Registry Number" and not cas_number:
                    cas_number = val[:20]
                elif src_name == "PubChem" and not pubchem_cid:
                    pubchem_cid = val[:20]

            # --- ChemicalMaster: fill-null-only or create ---
            cm = self.session.query(ChemicalMaster).filter_by(
                entity_id=entity_id
            ).one_or_none()

            if cm is None:
                cm = ChemicalMaster(
                    entity_id=entity_id,
                    chebi_id=chebi_id,
                    inchi_key=inchi_key_raw[:27] if inchi_key_raw else None,
                    smiles=smiles_raw[:4000] if smiles_raw else None,
                    formula=formula_raw[:100] if formula_raw else None,
                    molecular_weight=mol_weight,
                    cas_number=cas_number,
                    pubchem_cid=pubchem_cid,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(cm)
            else:
                if cm.chebi_id is None:
                    cm.chebi_id = chebi_id
                if cm.inchi_key is None and inchi_key_raw:
                    cm.inchi_key = inchi_key_raw[:27]
                if cm.smiles is None and smiles_raw:
                    cm.smiles = smiles_raw[:4000]
                if cm.formula is None and formula_raw:
                    cm.formula = formula_raw[:100]
                if cm.molecular_weight is None and mol_weight is not None:
                    cm.molecular_weight = mol_weight
                if cm.cas_number is None and cas_number:
                    cm.cas_number = cas_number
                if cm.pubchem_cid is None and pubchem_cid:
                    cm.pubchem_cid = pubchem_cid

            already_enriched.add(entity_id)
            # Keep InChIKey → entity map fresh for next rows that share it
            if inchi_key_raw and inchi_key_raw not in inchi_to_entity:
                inchi_to_entity[inchi_key_raw] = entity_id
            if chebi_id not in chebi_xref_to_entity:
                chebi_xref_to_entity[chebi_id] = entity_id

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    if (i + 1) % (BATCH * 20) == 0:
                        self.logger.log(
                            f"  Committed {i + 1:,}/{total:,} "
                            f"(inchi={matched_inchi:,} "
                            f"chebi={matched_chebi:,} "
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
            created=matched_inchi + matched_chebi + created_new,
            skipped=already_done,
            warnings=warnings,
            extras={
                "matched_inchi":     matched_inchi,
                "matched_chebi_xref": matched_chebi,
                "created_new":        created_new,
                "already_enriched":   already_done,
            },
        )
        msg = (
            f"[{self.DTP_NAME}] Enrich complete: "
            f"total={total:,} "
            f"inchi={matched_inchi:,} "
            f"chebi_xref={matched_chebi:,} "
            f"new={created_new:,} "
            f"already={already_done:,} "
            f"warn={warnings:,}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats
