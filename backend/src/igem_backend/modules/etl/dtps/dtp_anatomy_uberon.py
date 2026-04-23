"""
UBERON Anatomy DTP.

Pipeline role:
- Master DTP for anatomical entities from the UBERON basic ontology.
- No dependency on Gene/Disease/Protein DTPs.
- Once loaded, Disease→Anatomy edges from dtp_disease_mondo_relationships
  and future Gene→Tissue edges (GTEx, eQTL) resolve automatically.

Source:
- uberon-basic.obo (~5k curated terms) from
  https://purl.obolibrary.org/obo/uberon/basic.obo

  The basic subset is pre-filtered to terms with at least one subset tag,
  excluding obscure cross-species extensions from the full UBERON (~25k terms).
  Terms with 'upper_level' or 'non_informative' subsets are skipped as they
  represent abstract grouping classes (e.g. "anatomical entity"), not real
  anatomical structures.

anatomy_level classification strategy:
  1. Subset-based (reliable):
     organ_slim / major_organ          → "organ"
  2. Name-based heuristics (approximate):
     name contains 'system'/'apparatus' → "system"
     name contains tissue keywords      → "tissue"
     name contains structure keywords   → "structure"
  3. Default:
     everything else                    → "region"

  Heuristic terms should be reviewed over time. The column is nullable so
  the DTP can be re-run with improved rules without reloading entities.

What is loaded:
  Entity (type=Anatomy) + EntityAlias:
    - code/UBERON     → UBERON:xxxxxxx  (primary)
    - preferred/UBERON → term name
    - synonym/UBERON  → exact synonyms
    - code/<PREFIX>   → FMA, MESH, BTO cross-references
  AnatomyMaster (uberon_id, name, definition, anatomy_level)
"""

import json
import re
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_FILE = "uberon-basic.obo"

# Subsets that mark abstract grouping classes — skip entirely
_SKIP_SUBSETS = {"upper_level", "non_informative", "early_development"}

# Subsets that map directly to anatomy_level (checked first)
_SUBSET_LEVEL: dict[str, str] = {
    "organ_slim":  "organ",
    "major_organ": "organ",
}

# Name fragments for system-level anatomy (checked in order; word-boundary aware)
_SYSTEM_TOKENS = {
    "system", "apparatus",
}
# Broader patterns as substrings (more permissive)
_SYSTEM_SUBSTRINGS = {
    " system", " apparatus",
}

# Name fragments indicating tissue-level anatomy
_TISSUE_TOKENS = {
    "tissue", "parenchyma", "epithelium", "endothelium",
    "mucosa", "stroma", "adventitia", "mesothelium",
    "myelin", "periosteum", "perichondrium", "mesenchyme",
    "ectoderm", "endoderm", "mesoderm",
}

# Name fragments indicating cellular/subcellular structures
_STRUCTURE_TOKENS = {
    "synapse", "synaptic", "axon", "dendrite", "dendritic",
    "membrane", "junction", "channel", "receptor",
    "fiber tract", "nerve fiber", "filament", "granule cell",
    "vesicle", "tubule", "canaliculus", "cilium",
}

# Cross-reference prefixes to store as EntityAlias
_XREF_ALIAS_PREFIXES = {"FMA", "MESH", "BTO", "NCIT", "UMLS"}

# Regex to extract the quoted definition text
_DEF_RE = re.compile(r'^"(.*?)"\s*\[', re.DOTALL)


def _classify_level(name: str, subsets: set) -> Optional[str]:
    """
    Returns anatomy_level string or None if inconclusive.

    Priority: subset tags → system keywords → tissue keywords →
              structure keywords → region (default).
    """
    # 1. Subset-based (most reliable)
    for subset, level in _SUBSET_LEVEL.items():
        if subset in subsets:
            return level

    name_lower = name.lower()

    # 2. System: must appear as a standalone word or clear substring
    words = set(name_lower.split())
    if words & _SYSTEM_TOKENS:
        return "system"
    # also catch compound forms like "nervous system" (ends with system)
    if name_lower.endswith(" system") or name_lower.endswith(" apparatus"):
        return "system"

    # 3. Tissue
    if any(tok in name_lower for tok in _TISSUE_TOKENS):
        return "tissue"

    # 4. Cellular/subcellular structure
    if any(tok in name_lower for tok in _STRUCTURE_TOKENS):
        return "structure"

    # 5. Default
    return "region"


def _parse_obo(path: Path) -> tuple[list[dict], int]:
    """
    Stream-parse uberon-basic.obo.
    Returns (records, skipped_count).

    records: [{uberon_id, name, definition, synonyms, xrefs, subsets,
               anatomy_level}, ...]
    """
    records: list[dict] = []
    skipped = 0
    current: dict = {}
    in_term = False

    def _flush():
        nonlocal skipped
        if not current:
            return
        uberon_id = current.get("uberon_id")
        name = current.get("name")
        if not uberon_id or not name:
            return
        if current.get("obsolete"):
            return
        # Skip abstract grouping terms
        if current.get("subsets", set()) & _SKIP_SUBSETS:
            skipped += 1
            return
        level = _classify_level(name, current.get("subsets", set()))
        records.append({
            "uberon_id":     uberon_id,
            "name":          name,
            "definition":    current.get("definition"),
            "synonyms":      json.dumps(current.get("synonyms", [])),
            "xrefs":         json.dumps(current.get("xrefs", [])),
            "anatomy_level": level,
        })

    with open(path, encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.rstrip()

            if line == "[Term]":
                _flush()
                current = {"synonyms": [], "xrefs": [], "subsets": set()}
                in_term = True
                continue

            if line.startswith("[") and line != "[Term]":
                _flush()
                in_term = False
                current = {}
                continue

            if not in_term or not line or line.startswith("!"):
                continue
            if ":" not in line:
                continue

            tag, _, value = line.partition(": ")
            value = value.split(" ! ")[0].strip()

            if tag == "id":
                if value.startswith("UBERON:"):
                    current["uberon_id"] = value
                else:
                    # Non-UBERON term (BFO, RO, etc.) — skip
                    current["uberon_id"] = None
            elif tag == "name":
                current["name"] = value
            elif tag == "def":
                m = _DEF_RE.match(value)
                current["definition"] = m.group(1) if m else value.strip('"')
            elif tag == "subset":
                current["subsets"].add(value)
            elif tag == "synonym":
                # Only EXACT synonyms to keep noise low
                if "EXACT" in value:
                    m = re.match(r'^"(.*?)"', value)
                    if m:
                        current["synonyms"].append(m.group(1))
            elif tag == "xref":
                prefix = value.split(":")[0].upper() if ":" in value else ""
                if prefix in _XREF_ALIAS_PREFIXES:
                    current["xrefs"].append(value)
            elif tag == "is_obsolete" and value == "true":
                current["obsolete"] = True

    _flush()
    return records, skipped


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_anatomy_uberon"
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

            self.logger.log(f"Downloading UBERON basic from {url}", "INFO")
            with requests.get(url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            fh.write(chunk)

            file_hash = self._hash_file(dest)
            file_size = dest.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = f"[{self.DTP_NAME}] uberon-basic.obo downloaded ({file_size:,} bytes)"
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

            self.logger.log("Parsing uberon-basic.obo...", "INFO")
            records, skipped = _parse_obo(input_file)

            df = pd.DataFrame(records)
            out = self._dtp_dir(processed_dir)
            master_parquet = out / "master_data.parquet"
            df.to_parquet(master_parquet, index=False)
            if self.debug_mode:
                df.to_csv(out / "master_data.csv", index=False)

            # Log level distribution
            if not df.empty and "anatomy_level" in df.columns:
                level_counts = df["anatomy_level"].value_counts().to_dict()
                level_str = ", ".join(
                    f"{k}:{v}" for k, v in sorted(level_counts.items())
                )
                self.logger.log(f"  anatomy_level distribution: {level_str}", "INFO")

            self.logger.log(
                f"  master_data: {len(df):,} terms loaded, {skipped} skipped "
                f"(abstract/grouping)",
                "INFO",
            )

            stats = ETLStepStats(
                total=len(df),
                columns=len(df.columns),
                output_size_bytes=master_parquet.stat().st_size,
                extras={"skipped_abstract": skipped},
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df):,} anatomy terms"
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
            return True, "No anatomy rows to load.", ETLStepStats()

        from igem_backend.modules.db.models.model_anatomy import AnatomyMaster
        from igem_backend.modules.db.models.model_entities import Entity, EntityAlias

        anatomy_type_id = self.get_entity_type_id("Anatomy")

        # Pre-load existing UBERON IDs to skip re-loads
        existing: set[str] = {
            row[0]
            for row in self.session.query(AnatomyMaster.uberon_id).all()
            if row[0]
        }

        total = created = skipped = warnings = 0
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1

            uberon_id = str(row.get("uberon_id") or "").strip()
            name = str(row.get("name") or "").strip()

            if not uberon_id or not name:
                warnings += 1
                continue

            if uberon_id in existing:
                skipped += 1
                continue

            anatomy_level = row.get("anatomy_level") or None
            if anatomy_level and str(anatomy_level).strip().lower() in (
                "nan", "none", ""
            ):
                anatomy_level = None

            entity = Entity(
                type_id=anatomy_type_id,
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
                    type_id=anatomy_type_id,
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

            # Primary: UBERON:xxxxxxx
            _add_alias(uberon_id, "code", "UBERON", uberon_id.lower(), primary=True)
            # Preferred name
            _add_alias(name, "preferred", "UBERON", self._normalize(name))
            # Exact synonyms
            for syn in json.loads(row.get("synonyms") or "[]"):
                if syn and syn != name:
                    _add_alias(syn, "synonym", "UBERON", self._normalize(syn))
            # Cross-reference IDs (FMA, MESH, BTO, NCIT, UMLS)
            for xref in json.loads(row.get("xrefs") or "[]"):
                if xref and ":" in xref:
                    prefix = xref.split(":")[0].upper()
                    _add_alias(xref, "code", prefix, xref.lower())

            definition = str(row.get("definition") or "").strip() or None
            am = AnatomyMaster(
                entity_id=entity.id,
                uberon_id=uberon_id,
                name=self.guard_short(name),
                definition=definition,
                anatomy_level=anatomy_level,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(am)
            existing.add(uberon_id)
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
