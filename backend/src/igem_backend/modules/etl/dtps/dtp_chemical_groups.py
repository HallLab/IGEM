"""
Chemical Groups Classifier DTP.

Pipeline role
-------------
Runs AFTER all chemical master DTPs (unichem → chebi → hmdb → mesh).
Applies declarative classification rules to populate
`chemical_group_memberships` for the curated groups:

    Metabolite, Drug, Pollutant, Nutrient, Toxin, Hormone,
    Vitamin, Lipid, Amino-Acid

A chemical can belong to multiple groups — caffeine = Metabolite + Drug +
Nutrient is a valid classification. Membership rows carry the full ETL
provenance (data_source_id + etl_package_id) of the classifier run that
produced them.

Architecture
------------
No extract, no transform — all inputs are already in the DB. The `load()`
step runs a small set of bulk `INSERT … SELECT … ON CONFLICT DO NOTHING`
statements against `entity_aliases` and `chemical_masters`. One SQL
statement per rule means classification for ~980k chemicals completes
in seconds rather than iterating per-row in Python.

Rules (v1)
----------
Strong source-based (xref_source on EntityAlias):
  Lipid      — LipidMaps, LIPID MAPS, SwissLipids
  Drug       — DrugBank, DrugCentral, GtoPdb, ProbesAndDrugs, FDA-SRS
  Pollutant  — CompTox             (zero matches until UniChem preset B/C)
  Metabolite — HMDB                (HMDB chemicals are metabolites by scope)

Name-based (alias_norm on EntityAlias):
  Vitamin    — alias_norm LIKE 'vitamin %' on any alias
  Amino-Acid — alias_norm matches one of the 20 proteinogenic amino acids
               plus L-/D- stereoisomer variants

Future (v2) — not yet implemented:
  Hormone, Toxin, Nutrient require ChEBI's role ontology (relation.tsv),
  which the ChEBI DTP does not currently load. Adding these groups is a
  follow-up once relation.tsv is ingested.

Idempotency
-----------
Each rule uses `ON CONFLICT DO NOTHING` on the (chemical_id, group_id)
primary key, so re-runs add only new classifications without disturbing
existing membership rows.
"""

from typing import Optional

from sqlalchemy import bindparam, text

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats


# 20 proteinogenic amino acids after normalize() (lowercase, hyphen→space).
_AA_BASE = [
    "alanine", "arginine", "asparagine", "aspartic acid", "cysteine",
    "glutamic acid", "glutamine", "glycine", "histidine", "isoleucine",
    "leucine", "lysine", "methionine", "phenylalanine", "proline",
    "serine", "threonine", "tryptophan", "tyrosine", "valine",
]
_AA_NORMS: list[str] = (
    _AA_BASE
    + [f"l {n}" for n in _AA_BASE]
    + [f"d {n}" for n in _AA_BASE]
)


# Source-based classification rules. Each group may have multiple xref
# sources — a chemical qualifies if any one matches.
_SOURCE_RULES: dict[str, list[str]] = {
    "Lipid":      ["LipidMaps", "LIPID MAPS", "SwissLipids"],
    "Drug":       ["DrugBank", "DrugCentral", "GtoPdb",
                   "ProbesAndDrugs", "FDA-SRS"],
    "Pollutant":  ["CompTox"],
    "Metabolite": ["HMDB"],
}


class DTP(DTPBase):

    DTP_NAME = "chemical_groups"
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
    # EXTRACT — no-op
    # -------------------------------------------------------------------------
    def extract(
        self, raw_dir: str
    ) -> tuple[bool, str, Optional[str], ETLStepStats]:
        msg = (
            f"[{self.DTP_NAME}] Extract not applicable — "
            "classification operates directly on DB state."
        )
        self.logger.log(msg, "INFO")
        return True, msg, None, ETLStepStats()

    # -------------------------------------------------------------------------
    # TRANSFORM — no-op
    # -------------------------------------------------------------------------
    def transform(
        self, raw_dir: str, processed_dir: str
    ) -> tuple[bool, str, ETLStepStats]:
        msg = (
            f"[{self.DTP_NAME}] Transform not applicable — "
            "no intermediate artefact is produced."
        )
        self.logger.log(msg, "INFO")
        return True, msg, ETLStepStats()

    # -------------------------------------------------------------------------
    # LOAD — apply classification rules in bulk
    # -------------------------------------------------------------------------
    def load(self, processed_dir: str) -> tuple[bool, str, ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Load starting...", "INFO")
        try:
            from igem_backend.modules.db.models.model_chemicals import (
                ChemicalGroup,
            )

            # Resolve group name → id
            groups: dict[str, int] = {
                g.name: g.id
                for g in self.session.query(ChemicalGroup).all()
            }

            required = set(_SOURCE_RULES.keys()) | {"Vitamin", "Amino-Acid"}
            missing = required - set(groups.keys())
            if missing:
                return (
                    False,
                    f"ChemicalGroup rows missing: {sorted(missing)}. "
                    "Run `igem-server db upgrade` to seed them.",
                    ETLStepStats(errors=1),
                )

            per_group: dict[str, int] = {g: 0 for g in groups}

            # --- Rules 1-4: source-based classification ---
            for group_name, sources in _SOURCE_RULES.items():
                n = self._apply_source_rule(
                    group_name, groups[group_name], sources
                )
                per_group[group_name] += n

            # --- Rule 5: Vitamin (name pattern) ---
            n = self._apply_name_like(
                "Vitamin", groups["Vitamin"], "vitamin %"
            )
            per_group["Vitamin"] += n

            # --- Rule 6: Amino-Acid (exact name match) ---
            n = self._apply_name_in(
                "Amino-Acid", groups["Amino-Acid"], _AA_NORMS
            )
            per_group["Amino-Acid"] += n

            self.session.commit()

            total_created = sum(per_group.values())
            stats = ETLStepStats(
                total=total_created,
                created=total_created,
                extras=per_group,
            )
            lines = [
                f"[{self.DTP_NAME}] Classifier complete: "
                f"{total_created:,} memberships created",
            ]
            for name in sorted(
                per_group, key=lambda k: -per_group[k]
            ):
                lines.append(
                    f"  {name:<12} : {per_group[name]:>10,}"
                )
            msg = "\n".join(lines)
            for line in lines:
                self.logger.log(line, "INFO")
            return True, msg, stats

        except Exception as e:
            self.session.rollback()
            msg = f"[{self.DTP_NAME}] Load failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, ETLStepStats(errors=1)

    # -------------------------------------------------------------------------
    # Rule helpers
    # -------------------------------------------------------------------------
    def _apply_source_rule(
        self, group_name: str, group_id: int, sources: list[str]
    ) -> int:
        self.logger.log(
            f"  [{group_name}] xref_source IN {sources}", "INFO"
        )
        sql = text("""
            INSERT INTO chemical_group_memberships
                (chemical_id, group_id, data_source_id, etl_package_id)
            SELECT DISTINCT cm.id, :gid, :ds, :pkg
            FROM chemical_masters cm
            JOIN entity_aliases a ON a.entity_id = cm.entity_id
            WHERE a.xref_source IN :sources
            ON CONFLICT DO NOTHING
        """).bindparams(bindparam("sources", expanding=True))

        result = self.session.execute(sql, {
            "gid": group_id,
            "ds": self.data_source.id,
            "pkg": self.package.id,
            "sources": tuple(sources),
        })
        n = result.rowcount or 0
        self.logger.log(f"    → {n:,} memberships", "INFO")
        return n

    def _apply_name_like(
        self, group_name: str, group_id: int, pattern: str
    ) -> int:
        self.logger.log(
            f"  [{group_name}] alias_norm LIKE {pattern!r}", "INFO"
        )
        sql = text("""
            INSERT INTO chemical_group_memberships
                (chemical_id, group_id, data_source_id, etl_package_id)
            SELECT DISTINCT cm.id, :gid, :ds, :pkg
            FROM chemical_masters cm
            JOIN entity_aliases a ON a.entity_id = cm.entity_id
            WHERE a.alias_norm LIKE :pat
            ON CONFLICT DO NOTHING
        """)
        result = self.session.execute(sql, {
            "gid": group_id,
            "ds": self.data_source.id,
            "pkg": self.package.id,
            "pat": pattern,
        })
        n = result.rowcount or 0
        self.logger.log(f"    → {n:,} memberships", "INFO")
        return n

    def _apply_name_in(
        self, group_name: str, group_id: int, norms: list[str]
    ) -> int:
        self.logger.log(
            f"  [{group_name}] alias_norm IN ({len(norms)} values)", "INFO"
        )
        sql = text("""
            INSERT INTO chemical_group_memberships
                (chemical_id, group_id, data_source_id, etl_package_id)
            SELECT DISTINCT cm.id, :gid, :ds, :pkg
            FROM chemical_masters cm
            JOIN entity_aliases a ON a.entity_id = cm.entity_id
            WHERE a.alias_norm IN :norms
            ON CONFLICT DO NOTHING
        """).bindparams(bindparam("norms", expanding=True))

        result = self.session.execute(sql, {
            "gid": group_id,
            "ds": self.data_source.id,
            "pkg": self.package.id,
            "norms": tuple(norms),
        })
        n = result.rowcount or 0
        self.logger.log(f"    → {n:,} memberships", "INFO")
        return n
