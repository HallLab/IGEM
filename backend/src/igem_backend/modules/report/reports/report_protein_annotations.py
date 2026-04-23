from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

import pandas as pd
from sqlalchemy import or_
from sqlalchemy.orm import Session

from igem_backend.modules.report.reports.base_report import ReportBase


class ProteinAnnotationsReport(ReportBase):
    """
    Master annotation report for human proteins.

    Accepts a list of protein identifiers (UniProt accessions, protein names,
    gene symbols, or any registered alias). Returns one row per matched entity
    with consolidated cross-references, Pfam domain summary, and relationship
    summary. Isoform inputs are resolved and annotated with their canonical
    counterpart. Input values with no match return a 'not_found' row.
    """

    REPORT_NAME = "protein_annotations"
    REPORT_VERSION = "1.0.0"
    REPORT_DESCRIPTION = (
        "Master protein annotation table: UniProt cross-references, function, "
        "subcellular location, Pfam domain summary, and relationship summary per protein."
    )

    def available_columns(self) -> list[str]:
        return [
            "input_value",
            "input_matched_alias",
            "entity_id",
            "canonical_entity_id",
            "protein_master_id",
            "protein_id",
            "input_is_isoform",
            "input_isoform_accession",
            "isoform_count",
            "function",
            "location",
            "tissue_expression",
            "protein_source_system",
            "protein_data_source",
            "pfam_total_count",
            "pfam_count_by_type",
            "pfam_ids_by_type",
            "entity_relationships_by_group",
            "total_entity_relationships",
            "other_aliases",
            "status",
            "note",
        ]

    def example_input(self) -> dict[str, Any]:
        return {
            "input_values": ["P04637", "P00533", "Q9Y6K9"],
            "include_pfam_summary": True,
            "include_pfam_details": False,
            "include_relationships": True,
        }

    # -------------------------------------------------------------------------
    # Main run
    # -------------------------------------------------------------------------
    def run(self, session: Session, **kwargs) -> pd.DataFrame:
        from igem_backend.modules.db.models.model_entities import (
            Entity,
            EntityAlias,
            EntityRelationship,
            EntityType,
        )
        from igem_backend.modules.db.models.model_proteins import (
            ProteinEntity,
            ProteinMaster,
            ProteinPfam,
            ProteinPfamLink,
        )

        inputs = self.resolve_input_list(self.param(kwargs, "input_values"))
        emit_not_found = self.param(kwargs, "emit_not_found_rows", True)
        include_pfam = self.param(kwargs, "include_pfam_summary", True)
        include_pfam_details = self.param(kwargs, "include_pfam_details", False)
        max_pfam_ids = self.param(kwargs, "max_pfam_ids_per_type", 10)
        include_rels = self.param(kwargs, "include_relationships", True)
        include_aliases = self.param(kwargs, "include_aliases", True)

        # --- Protein entity type ---
        protein_type = (
            session.query(EntityType).filter_by(name="Proteins").one_or_none()
        )
        if protein_type is None:
            return pd.DataFrame(columns=self.available_columns())

        # ---- 1. Match inputs ----
        all_mode = not inputs
        norm_to_original: dict[str, str] = {}
        if inputs:
            for v in inputs:
                norm = v.strip().lower()
                if norm:
                    norm_to_original[norm] = v

        # alias_norm → (entity_id, alias_value) — first match wins
        matched: dict[str, tuple[int, str]] = {}
        if norm_to_original:
            aliases_found = (
                session.query(EntityAlias)
                .filter(
                    EntityAlias.alias_norm.in_(norm_to_original.keys()),
                    EntityAlias.type_id == protein_type.id,
                )
                .all()
            )
            for a in aliases_found:
                if a.alias_norm not in matched:
                    matched[a.alias_norm] = (a.entity_id, a.alias_value)

        # Determine the set of entity_ids to materialise
        if all_mode:
            all_pe_ids = [
                pe.entity_id for pe in session.query(ProteinEntity).all()
            ]
            entity_ids_set: set[int] = set(all_pe_ids)
        else:
            entity_ids_set = {eid for eid, _ in matched.values()}

        if not entity_ids_set and not all_mode:
            # All inputs unmatched — produce not_found rows only
            rows: list[dict] = []
            if emit_not_found:
                for original in norm_to_original.values():
                    rows.append(self._not_found_row(original))
            return pd.DataFrame(rows, columns=self.available_columns())

        # ---- 2. Bulk load ProteinEntity for queried entities ----
        pe_by_entity: dict[int, ProteinEntity] = {}
        protein_master_ids: set[int] = set()

        if entity_ids_set:
            pe_rows = (
                session.query(ProteinEntity)
                .filter(ProteinEntity.entity_id.in_(entity_ids_set))
                .all()
            )
            for pe in pe_rows:
                pe_by_entity[pe.entity_id] = pe
                protein_master_ids.add(pe.protein_id)

        # Load ALL ProteinEntity rows for those masters (isoform siblings)
        pe_by_master: dict[int, list[ProteinEntity]] = defaultdict(list)
        if protein_master_ids:
            for pe in (
                session.query(ProteinEntity)
                .filter(ProteinEntity.protein_id.in_(protein_master_ids))
                .all()
            ):
                pe_by_master[pe.protein_id].append(pe)
                pe_by_entity.setdefault(pe.entity_id, pe)

        # ---- 3. Bulk load ProteinMaster ----
        pm_by_id: dict[int, ProteinMaster] = {}
        if protein_master_ids:
            for pm in (
                session.query(ProteinMaster)
                .filter(ProteinMaster.id.in_(protein_master_ids))
                .all()
            ):
                pm_by_id[pm.id] = pm

        # ---- 4. Bulk load Entity (data source metadata) ----
        entity_by_id: dict[int, Entity] = {}
        if entity_ids_set:
            for e in (
                session.query(Entity)
                .filter(Entity.id.in_(entity_ids_set))
                .all()
            ):
                entity_by_id[e.id] = e

        # ---- 5. Bulk load Pfam links ----
        pfam_by_master: dict[int, list[ProteinPfam]] = defaultdict(list)
        if include_pfam and protein_master_ids:
            for link, pfam in (
                session.query(ProteinPfamLink, ProteinPfam)
                .join(ProteinPfam, ProteinPfamLink.pfam_pk_id == ProteinPfam.id)
                .filter(ProteinPfamLink.protein_id.in_(protein_master_ids))
                .all()
            ):
                pfam_by_master[link.protein_id].append(pfam)

        # ---- 6. Bulk load EntityRelationship ----
        rels_by_entity: dict[int, list[EntityRelationship]] = defaultdict(list)
        if include_rels and entity_ids_set:
            for r in (
                session.query(EntityRelationship)
                .filter(
                    or_(
                        EntityRelationship.entity_1_id.in_(entity_ids_set),
                        EntityRelationship.entity_2_id.in_(entity_ids_set),
                    )
                )
                .all()
            ):
                if r.entity_1_id in entity_ids_set:
                    rels_by_entity[r.entity_1_id].append(r)
                if r.entity_2_id in entity_ids_set and r.entity_2_id != r.entity_1_id:
                    rels_by_entity[r.entity_2_id].append(r)

        # ---- 7. Bulk load EntityAlias ----
        aliases_by_entity: dict[int, list[EntityAlias]] = defaultdict(list)
        if include_aliases and entity_ids_set:
            for a in (
                session.query(EntityAlias)
                .filter(EntityAlias.entity_id.in_(entity_ids_set))
                .all()
            ):
                aliases_by_entity[a.entity_id].append(a)

        # ---- 8. EntityType name lookup ----
        type_name_by_id: dict[int, str] = {
            et.id: et.name for et in session.query(EntityType).all()
        }

        # ---- 9. Build rows ----
        rows = []
        build_args = dict(
            pe_by_entity=pe_by_entity,
            pe_by_master=pe_by_master,
            pm_by_id=pm_by_id,
            entity_by_id=entity_by_id,
            pfam_by_master=pfam_by_master,
            rels_by_entity=rels_by_entity,
            aliases_by_entity=aliases_by_entity,
            type_name_by_id=type_name_by_id,
            include_pfam=include_pfam,
            include_pfam_details=include_pfam_details,
            max_pfam_ids=max_pfam_ids,
            include_rels=include_rels,
            include_aliases=include_aliases,
        )

        if all_mode:
            for entity_id in sorted(entity_ids_set):
                rows.append(
                    self._build_row(
                        entity_id=entity_id,
                        input_value=None,
                        matched_alias=None,
                        **build_args,
                    )
                )
        else:
            for norm, original in norm_to_original.items():
                if norm not in matched:
                    if emit_not_found:
                        rows.append(self._not_found_row(original))
                    continue
                entity_id, alias_value = matched[norm]
                rows.append(
                    self._build_row(
                        entity_id=entity_id,
                        input_value=original,
                        matched_alias=alias_value,
                        **build_args,
                    )
                )

        return pd.DataFrame(rows, columns=self.available_columns())

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------
    def _not_found_row(self, input_value: str) -> dict:
        row: dict = {col: None for col in self.available_columns()}
        row["input_value"] = input_value
        row["status"] = "not_found"
        row["note"] = "No matching protein alias found"
        return row

    def _build_row(
        self,
        entity_id: int,
        input_value: Optional[str],
        matched_alias: Optional[str],
        pe_by_entity: dict,
        pe_by_master: dict,
        pm_by_id: dict,
        entity_by_id: dict,
        pfam_by_master: dict,
        rels_by_entity: dict,
        aliases_by_entity: dict,
        type_name_by_id: dict,
        include_pfam: bool,
        include_pfam_details: bool,
        max_pfam_ids: int,
        include_rels: bool,
        include_aliases: bool,
    ) -> dict:
        row: dict = {col: None for col in self.available_columns()}
        row["input_value"] = input_value
        row["input_matched_alias"] = matched_alias
        row["entity_id"] = entity_id
        row["status"] = "found"

        # --- ProteinEntity context ---
        pe = pe_by_entity.get(entity_id)
        if pe is None:
            row["status"] = "error"
            row["note"] = "ProteinEntity record missing"
            return row

        row["protein_master_id"] = pe.protein_id
        row["input_is_isoform"] = pe.is_isoform
        row["input_isoform_accession"] = (
            pe.isoform_accession if pe.is_isoform else None
        )

        all_for_master = pe_by_master.get(pe.protein_id, [])
        canonical_pe = next(
            (p for p in all_for_master if not p.is_isoform), None
        )
        row["canonical_entity_id"] = (
            canonical_pe.entity_id if canonical_pe else entity_id
        )
        row["isoform_count"] = sum(1 for p in all_for_master if p.is_isoform)

        # --- ProteinMaster ---
        pm = pm_by_id.get(pe.protein_id)
        if pm:
            row["protein_id"] = pm.protein_id
            row["function"] = pm.function
            row["location"] = pm.location
            row["tissue_expression"] = pm.tissue_expression

        # --- Data source metadata ---
        entity = entity_by_id.get(entity_id)
        if entity and entity.data_source:
            row["protein_data_source"] = entity.data_source.name
            ss = getattr(entity.data_source, "source_system", None)
            if ss:
                row["protein_source_system"] = ss.name

        # --- Pfam summary ---
        if include_pfam:
            pfams = pfam_by_master.get(pe.protein_id, [])
            row["pfam_total_count"] = len(pfams)
            if pfams:
                type_counts: dict[str, int] = defaultdict(int)
                type_accs: dict[str, list[str]] = defaultdict(list)
                for p in pfams:
                    ptype = p.type or "Unknown"
                    type_counts[ptype] += 1
                    if include_pfam_details:
                        type_accs[ptype].append(p.pfam_acc)
                row["pfam_count_by_type"] = "; ".join(
                    f"{k}:{v}" for k, v in sorted(type_counts.items())
                )
                if include_pfam_details:
                    parts = []
                    for k in sorted(type_accs):
                        accs = type_accs[k][:max_pfam_ids]
                        parts.append(f"{k}:{','.join(accs)}")
                    row["pfam_ids_by_type"] = "; ".join(parts)

        # --- Relationships ---
        if include_rels:
            rels = rels_by_entity.get(entity_id, [])
            row["total_entity_relationships"] = len(rels)
            if rels:
                group_counts: dict[str, int] = defaultdict(int)
                for r in rels:
                    other_type_id = (
                        r.entity_2_type_id
                        if r.entity_1_id == entity_id
                        else r.entity_1_type_id
                    )
                    t = (
                        type_name_by_id.get(other_type_id, "Unknown")
                        if other_type_id
                        else "Unknown"
                    )
                    group_counts[t] += 1
                row["entity_relationships_by_group"] = "; ".join(
                    f"{k}:{v}" for k, v in sorted(group_counts.items())
                )

        # --- Other aliases ---
        if include_aliases:
            all_aliases = aliases_by_entity.get(entity_id, [])
            known: set[str] = {
                v
                for v in [matched_alias, row.get("protein_id")]
                if v is not None
            }
            other = [
                a.alias_value
                for a in all_aliases
                if a.alias_value not in known and not a.is_primary
            ]
            row["other_aliases"] = "; ".join(other[:15]) if other else None

        return row
