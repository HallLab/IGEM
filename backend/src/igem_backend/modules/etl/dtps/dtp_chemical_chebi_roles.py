"""
ChEBI Roles Classifier DTP — populates ChemicalGroupMembership using
ChEBI's curated has_role / is_a hierarchy.

Pipeline role
-------------
Runs AFTER all chemical master DTPs and complements
dtp_chemical_groups by adding membership rows for the categories that
require curated role knowledge:

    Hormone   = CHEBI:24621 + descendants in the role is_a graph
    Toxin     = CHEBI:27026 + descendants
    Nutrient  = CHEBI:33284 + descendants
    Vitamin   = CHEBI:33229 + descendants

A compound qualifies for a category if any of its `has_role` targets
is in the descendant set of the corresponding root role. Memberships
are written to chemical_group_memberships with ON CONFLICT DO NOTHING,
so re-runs are idempotent and existing memberships from
dtp_chemical_groups (Vitamin via name-pattern, etc.) are preserved.

Source
------
relation.tsv.gz (~2.5 MB, ~380k rows)
https://ftp.ebi.ac.uk/pub/databases/chebi/flat_files/relation.tsv.gz

Columns: id, relation_type_id, init_id, final_id, status_id, ...
Encoded relation types (from observation):
    4 → has_role   (init compound has the role of final)
    5 → is_a       (init is_a final — used to walk role hierarchy)

Algorithm
---------
1. Parse relation.tsv, separate rows into:
   - is_a edges (child → parent)
   - has_role edges (compound → role)
2. For each root role, BFS down the reverse is_a graph (parent →
   children) to collect all descendant role IDs.
3. For each has_role edge, check the role against each category's
   descendant set; emit (compound_chebi_id, category) pairs.
4. Output parquet with one row per (chebi_id, category) pair.
5. Load: bulk INSERT into chemical_group_memberships, joining on the
   existing `code/ChEBI` aliases to resolve chebi_id → chemical_master.id.
"""

import gzip
from collections import defaultdict, deque
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from sqlalchemy import bindparam, text

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats


_BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/chebi/flat_files/"
_RELATION_FILE = "relation.tsv.gz"

_REL_HAS_ROLE = 4
_REL_IS_A = 5

# Root ChEBI IDs for each functional category we want to classify.
_ROLE_ROOTS: dict[str, int] = {
    "Hormone":  24621,
    "Toxin":    27026,
    "Nutrient": 33284,
    "Vitamin":  33229,
}


class DTP(DTPBase):

    DTP_NAME = "chemical_chebi_roles"
    DTP_VERSION = "1.0.0"
    DTP_TYPE = "relationship"
    ROLLBACK_STRATEGY = "delete"

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
            dst = landing / _RELATION_FILE

            if dst.exists():
                size = dst.stat().st_size
                self.logger.log(
                    f"  Reusing existing {_RELATION_FILE} "
                    f"({size:,} bytes)",
                    "INFO",
                )
            else:
                url = _BASE_URL + _RELATION_FILE
                self.logger.log(f"  Downloading {url}", "INFO")
                with requests.get(url, stream=True, timeout=600) as resp:
                    resp.raise_for_status()
                    with open(dst, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=1 << 16):
                            if chunk:
                                fh.write(chunk)
                size = dst.stat().st_size

            file_hash = self._hash_file(dst)
            stats = ETLStepStats(file_size_bytes=size)
            msg = (
                f"[{self.DTP_NAME}] Extract complete: "
                f"{_RELATION_FILE} ({size:,} bytes)"
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
            src = self._dtp_dir(raw_dir) / _RELATION_FILE
            if not src.exists():
                return (
                    False,
                    f"Input file missing: {src} — run extract first.",
                    ETLStepStats(errors=1),
                )

            # --- Pass 1: parse rows we care about ---
            self.logger.log("  Pass 1/3: parsing relation.tsv...", "INFO")
            is_a_children: dict[int, list[int]] = defaultdict(list)
            has_role_rows: list[tuple[int, int]] = []
            n_lines = 0

            with gzip.open(src, "rt", encoding="utf-8") as fh:
                fh.readline()  # header
                for line in fh:
                    n_lines += 1
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) < 4:
                        continue
                    try:
                        rtype = int(parts[1])
                        init_id = int(parts[2])
                        final_id = int(parts[3])
                    except ValueError:
                        continue
                    if rtype == _REL_IS_A:
                        # reverse edge for descendant traversal
                        is_a_children[final_id].append(init_id)
                    elif rtype == _REL_HAS_ROLE:
                        has_role_rows.append((init_id, final_id))

            self.logger.log(
                f"    {n_lines:,} rows scanned: "
                f"{sum(len(v) for v in is_a_children.values()):,} is_a, "
                f"{len(has_role_rows):,} has_role",
                "INFO",
            )

            # --- Pass 2: BFS descendants per category root ---
            self.logger.log(
                "  Pass 2/3: walking role hierarchy...", "INFO"
            )
            descendants: dict[str, set[int]] = {}
            for category, root in _ROLE_ROOTS.items():
                seen = {root}
                queue = deque([root])
                while queue:
                    node = queue.popleft()
                    for child in is_a_children.get(node, ()):
                        if child not in seen:
                            seen.add(child)
                            queue.append(child)
                descendants[category] = seen
                self.logger.log(
                    f"    {category:<10} root=CHEBI:{root}  "
                    f"descendants={len(seen):,}",
                    "INFO",
                )

            # --- Pass 3: classify compounds via has_role ---
            self.logger.log(
                "  Pass 3/3: matching compounds against role sets...",
                "INFO",
            )
            classifications: dict[str, set[int]] = {
                cat: set() for cat in _ROLE_ROOTS
            }
            for compound_id, role_id in has_role_rows:
                for category, role_set in descendants.items():
                    if role_id in role_set:
                        classifications[category].add(compound_id)

            # Build long-format dataframe (chebi_id, category)
            records: list[dict] = []
            for category, compound_ids in classifications.items():
                for cid in compound_ids:
                    records.append({
                        "chebi_id": f"CHEBI:{cid}",
                        "category": category,
                    })

            df = pd.DataFrame(records)
            out = self._dtp_dir(processed_dir)
            parquet_path = out / "role_classifications.parquet"
            df.to_parquet(parquet_path, index=False)
            if self.debug_mode:
                df.to_csv(out / "role_classifications.csv", index=False)

            stats = ETLStepStats(
                total=len(df),
                columns=len(df.columns),
                output_size_bytes=parquet_path.stat().st_size,
                extras={
                    cat: len(ids)
                    for cat, ids in classifications.items()
                },
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{len(df):,} (chebi_id, category) pairs"
            )
            self.logger.log(msg, "INFO")
            for cat in sorted(_ROLE_ROOTS):
                self.logger.log(
                    f"    {cat:<10}: {len(classifications[cat]):>8,} "
                    "compounds",
                    "INFO",
                )
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
        try:
            parquet_path = (
                self._dtp_dir(processed_dir) / "role_classifications.parquet"
            )
            if not parquet_path.exists():
                return (
                    False,
                    f"Processed file not found: {parquet_path}",
                    ETLStepStats(errors=1),
                )

            df = pd.read_parquet(parquet_path, engine="pyarrow")
            if df.empty:
                return True, "No role classifications to load.", \
                    ETLStepStats()

            from igem_backend.modules.db.models.model_chemicals import (
                ChemicalGroup,
            )
            groups: dict[str, int] = {
                g.name: g.id
                for g in self.session.query(ChemicalGroup).all()
            }
            missing = set(_ROLE_ROOTS) - set(groups.keys())
            if missing:
                return (
                    False,
                    f"ChemicalGroup rows missing: {sorted(missing)}",
                    ETLStepStats(errors=1),
                )

            per_group: dict[str, int] = {}
            for category in _ROLE_ROOTS:
                chebi_ids = (
                    df.loc[df["category"] == category, "chebi_id"]
                    .unique().tolist()
                )
                if not chebi_ids:
                    per_group[category] = 0
                    continue

                self.logger.log(
                    f"  [{category}] {len(chebi_ids):,} ChEBI ids → "
                    "matching entities",
                    "INFO",
                )
                sql = text("""
                    INSERT INTO chemical_group_memberships
                        (chemical_id, group_id, data_source_id,
                         etl_package_id)
                    SELECT DISTINCT cm.id, :gid, :ds, :pkg
                    FROM chemical_masters cm
                    JOIN entity_aliases a ON a.entity_id = cm.entity_id
                    WHERE a.xref_source = 'ChEBI'
                      AND a.alias_type = 'code'
                      AND a.alias_value IN :chebi_ids
                    ON CONFLICT DO NOTHING
                """).bindparams(bindparam("chebi_ids", expanding=True))

                result = self.session.execute(sql, {
                    "gid": groups[category],
                    "ds": self.data_source.id,
                    "pkg": self.package.id,
                    "chebi_ids": tuple(chebi_ids),
                })
                n = result.rowcount or 0
                per_group[category] = n
                self.logger.log(f"    → {n:,} memberships", "INFO")

            self.session.commit()

            total = sum(per_group.values())
            stats = ETLStepStats(
                total=total,
                created=total,
                extras=per_group,
            )
            lines = [
                f"[{self.DTP_NAME}] Classifier complete: "
                f"{total:,} memberships created",
            ]
            for cat in sorted(per_group, key=lambda k: -per_group[k]):
                lines.append(f"  {cat:<12}: {per_group[cat]:>10,}")
            for line in lines:
                self.logger.log(line, "INFO")
            return True, "\n".join(lines), stats

        except Exception as e:
            self.session.rollback()
            msg = f"[{self.DTP_NAME}] Load failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, ETLStepStats(errors=1)
