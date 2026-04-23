"""
ChEBI Chemical DTP.

Pipeline role:
- Master DTP for chemical entities from the Chemical Entities of Biological
  Interest (ChEBI) ontology published by EMBL-EBI.
- No dependency on Gene/Protein DTPs.
- CTD chemical-gene and chemical-disease relationships are handled by
  separate CTD DTPs (dtp_ctd_chem_gene, etc.).

Source files (ChEBI FTP flat files):
  compounds.tsv.gz        — primary compound records (ID, name, status)
  chemical_data.tsv.gz    — molecular properties in long format (FORMULA,
                            CHARGE, MASS, MONOISOTOPIC MASS, SMILES, InChIKey)
  secondary_ids.tsv.gz    — deprecated ChEBI IDs that map to current compounds
  database_accession.tsv.gz — external cross-references (CAS, PubChem, KEGG…)
  source.tsv.gz           — lookup table for source names in database_accession

What is loaded:
  Entity (type=Chemicals) + EntityAlias:
    - code/ChEBI      → CHEBI:xxxxxxx  (primary)
    - preferred/ChEBI → compound name
    - code/ChEBI      → secondary CHEBI IDs (is_primary=False)
    - code/<PREFIX>   → CAS, PubChem CID, KEGG, InChIKey, …
  ChemicalMaster (chebi_id, molecular_weight, formula, smiles, inchi_key,
                  cas_number, pubchem_cid)

Status mapping:
  status 'C' (checked) → active
  status 'E' (entry)   → active
  status 'S' (star)    → active
  anything else        → entity created but is_active=False
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
            return True, "No chemical rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_chemicals import ChemicalMaster
        from igem_backend.modules.db.models.model_entities import Entity, EntityAlias

        chem_type_id = self.get_entity_type_id("Chemicals")

        # Pre-load existing ChEBI ids to skip already-loaded compounds
        existing: set[str] = {
            cm.chebi_id
            for (cm,) in self.session.query(ChemicalMaster).with_entities(
                ChemicalMaster
            ).all()
            if cm.chebi_id
        }
        # Simpler: just fetch the chebi_id column
        existing = {
            row[0]
            for row in self.session.query(ChemicalMaster.chebi_id).all()
            if row[0]
        }

        total = created = skipped = warnings = 0
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1

            chebi_id = str(row.get("chebi_id") or "").strip()
            label = str(row.get("label") or "").strip()

            if not chebi_id or not label:
                warnings += 1
                continue

            if chebi_id in existing:
                skipped += 1
                continue

            status = str(row.get("status") or "E").strip().upper()
            is_active = status in _ACTIVE_STATUSES

            # --- Entity (direct add — compound is guaranteed new) ---
            entity = Entity(
                type_id=chem_type_id,
                is_active=is_active,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(entity)
            self.session.flush()

            # --- Aliases ---
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
                    type_id=chem_type_id,
                    alias_value=val,
                    alias_type=atype,
                    xref_source=xsrc,
                    alias_norm=self.guard_alias_norm(norm or val.lower()),
                    is_primary=primary,
                    is_active=is_active,
                    locale="en",
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                ))

            # Primary: CHEBI:xxxxxxx
            _add_alias(chebi_id, "code", "ChEBI", chebi_id.lower(), primary=True)
            # Preferred name
            _add_alias(label, "preferred", "ChEBI", self._normalize(label))
            # Secondary ChEBI IDs
            for sec_id in json.loads(row.get("secondary_ids") or "[]"):
                if sec_id:
                    _add_alias(sec_id, "code", "ChEBI", sec_id.lower())
            # External xrefs (CAS, PubChem, KEGG, InChIKey, …)
            xrefs_extra = json.loads(row.get("xrefs_extra") or "[]")
            for xr in xrefs_extra[:_MAX_XREF_ALIASES]:
                val = str(xr.get("alias_value") or "").strip()
                src_name = str(xr.get("xref_source") or "").strip()
                if val and src_name:
                    _add_alias(val, "code", src_name, val.lower())

            # --- Extract ChemicalMaster column values from xrefs ---
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

            # --- ChemicalMaster ---
            inchi_key_raw = str(row.get("inchi_key") or "").strip() or None
            smiles_raw = str(row.get("smiles") or "").strip() or None
            formula_raw = str(row.get("formula") or "").strip() or None

            mol_weight: Optional[float] = None
            raw_mass = row.get("mass")
            if raw_mass is not None and str(raw_mass).strip() not in ("", "nan"):
                try:
                    mol_weight = float(raw_mass)
                except (ValueError, TypeError):
                    pass

            cm = ChemicalMaster(
                entity_id=entity.id,
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
            existing.add(chebi_id)
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
            return False, f"Final commit failed: {e}", ETLStepStats(errors=1)

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
