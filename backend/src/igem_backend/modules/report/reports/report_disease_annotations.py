from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

import pandas as pd
from sqlalchemy import or_
from sqlalchemy.orm import Session

from igem_backend.modules.report.reports.base_report import ReportBase


class DiseaseAnnotationsReport(ReportBase):
    """
    Master annotation report for disease entities.

    Accepts a list of disease identifiers (MONDO IDs, OMIM IDs, MeSH IDs,
    ICD-10 codes, disease names, or any registered alias). Returns one row
    per matched entity with cross-references, disease group memberships, and
    relationship summary. Passing an empty input list returns all diseases
    (optionally filtered by group name).
    """

    REPORT_NAME = "disease_annotations"
    REPORT_VERSION = "1.0.0"
    REPORT_DESCRIPTION = (
        "Master disease annotation table: cross-references (MONDO/OMIM/MeSH/"
        "ICD-10/Orphanet), group memberships, and relationship summary per disease."
    )

    def available_columns(self) -> list[str]:
        return [
            "input_value",
            "input_matched_alias",
            "entity_id",
            "disease_id",
            "label",
            "description",
            "mondo_id",
            "omim_id",
            "mesh_id",
            "icd10",
            "orphanet_id",
            "disease_groups",
            "disease_parent_count",
            "disease_child_count",
            "entity_relationships_by_group",
            "total_entity_relationships",
            "other_aliases",
            "status",
            "note",
        ]

    def example_input(self) -> dict[str, Any]:
        return {
            "input_values": ["MONDO:0005301", "OMIM:104300", "multiple sclerosis"],
            "group_filter": None,
            "include_relationships": True,
        }

    # -------------------------------------------------------------------------
    # Main run
    # -------------------------------------------------------------------------
    def run(self, session: Session, **kwargs) -> pd.DataFrame:
        from igem_backend.modules.db.models.model_diseases import (
            DiseaseGroup,
            DiseaseGroupMembership,
            DiseaseMaster,
        )
        from igem_backend.modules.db.models.model_entities import (
            Entity,
            EntityAlias,
            EntityRelationship,
            EntityType,
        )

        inputs = self.resolve_input_list(self.param(kwargs, "input_values"))
        group_filter: Optional[str] = self.param(kwargs, "group_filter")
        emit_not_found = self.param(kwargs, "emit_not_found_rows", True)
        include_rels = self.param(kwargs, "include_relationships", True)
        include_aliases = self.param(kwargs, "include_aliases", True)

        # --- Disease entity type ---
        disease_type = (
            session.query(EntityType).filter_by(name="Diseases").one_or_none()
        )
        if disease_type is None:
            return pd.DataFrame(columns=self.available_columns())

        # ---- 1. Match inputs ----
        all_mode = not inputs
        norm_to_original: dict[str, str] = {}
        if inputs:
            for v in inputs:
                norm = v.strip().lower()
                if norm:
                    norm_to_original[norm] = v

        matched: dict[str, tuple[int, str]] = {}
        if norm_to_original:
            for a in (
                session.query(EntityAlias)
                .filter(
                    EntityAlias.alias_norm.in_(norm_to_original.keys()),
                    EntityAlias.type_id == disease_type.id,
                )
                .all()
            ):
                if a.alias_norm not in matched:
                    matched[a.alias_norm] = (a.entity_id, a.alias_value)

        if all_mode:
            entity_ids_set: set[int] = {
                dm.entity_id for dm in session.query(DiseaseMaster).all()
            }
        else:
            entity_ids_set = {eid for eid, _ in matched.values()}

        if not entity_ids_set and not all_mode:
            rows: list[dict] = []
            if emit_not_found:
                for original in norm_to_original.values():
                    rows.append(self._not_found_row(original))
            return pd.DataFrame(rows, columns=self.available_columns())

        # ---- 2. Bulk load DiseaseMaster (by entity_id) ----
        dm_by_entity: dict[int, DiseaseMaster] = {}
        dm_ids: set[int] = set()
        if entity_ids_set:
            for dm in (
                session.query(DiseaseMaster)
                .filter(DiseaseMaster.entity_id.in_(entity_ids_set))
                .all()
            ):
                dm_by_entity[dm.entity_id] = dm
                dm_ids.add(dm.id)

        # ---- 3. Apply group filter ----
        if group_filter and dm_ids:
            target_group = (
                session.query(DiseaseGroup)
                .filter(DiseaseGroup.name.ilike(group_filter))
                .one_or_none()
            )
            if target_group is None:
                return pd.DataFrame(columns=self.available_columns())

            kept_dm_ids: set[int] = {
                mem.disease_id
                for mem in session.query(DiseaseGroupMembership)
                .filter_by(group_id=target_group.id)
                .all()
            }
            dm_ids &= kept_dm_ids
            dm_by_entity = {
                eid: dm for eid, dm in dm_by_entity.items() if dm.id in dm_ids
            }
            entity_ids_set = {dm.entity_id for dm in dm_by_entity.values()}

        # ---- 4. Bulk load disease group memberships ----
        groups_by_entity: dict[int, list[str]] = defaultdict(list)
        if dm_ids:
            group_id_to_name: dict[int, str] = {
                g.id: g.name for g in session.query(DiseaseGroup).all()
            }
            for mem in (
                session.query(DiseaseGroupMembership)
                .filter(DiseaseGroupMembership.disease_id.in_(dm_ids))
                .all()
            ):
                dm = next(
                    (d for d in dm_by_entity.values() if d.id == mem.disease_id), None
                )
                if dm:
                    gname = group_id_to_name.get(mem.group_id)
                    if gname:
                        groups_by_entity[dm.entity_id].append(gname)

        # ---- 5. Bulk load Entity ----
        entity_by_id: dict[int, Entity] = {}
        if entity_ids_set:
            for e in (
                session.query(Entity)
                .filter(Entity.id.in_(entity_ids_set))
                .all()
            ):
                entity_by_id[e.id] = e

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
        disease_type_id = disease_type.id

        # ---- 9. Build rows ----
        rows = []
        build_args = dict(
            dm_by_entity=dm_by_entity,
            entity_by_id=entity_by_id,
            groups_by_entity=groups_by_entity,
            rels_by_entity=rels_by_entity,
            aliases_by_entity=aliases_by_entity,
            type_name_by_id=type_name_by_id,
            disease_type_id=disease_type_id,
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
                if group_filter and entity_id not in entity_ids_set:
                    if emit_not_found:
                        rows.append(self._not_found_row(original))
                    continue
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
        row["note"] = "No matching disease alias found"
        return row

    def _build_row(
        self,
        entity_id: int,
        input_value: Optional[str],
        matched_alias: Optional[str],
        dm_by_entity: dict,
        entity_by_id: dict,
        groups_by_entity: dict,
        rels_by_entity: dict,
        aliases_by_entity: dict,
        type_name_by_id: dict,
        disease_type_id: int,
        include_rels: bool,
        include_aliases: bool,
    ) -> dict:
        row: dict = {col: None for col in self.available_columns()}
        row["input_value"] = input_value
        row["input_matched_alias"] = matched_alias
        row["entity_id"] = entity_id
        row["status"] = "found"

        # --- DiseaseMaster ---
        dm = dm_by_entity.get(entity_id)
        if dm:
            row["disease_id"] = dm.disease_id
            row["label"] = dm.label
            row["description"] = dm.description
            row["mondo_id"] = dm.mondo_id
            row["omim_id"] = dm.omim_id
            row["mesh_id"] = dm.mesh_id
            row["icd10"] = dm.icd10
            row["orphanet_id"] = dm.orphanet_id

        # --- Disease groups ---
        groups = groups_by_entity.get(entity_id, [])
        row["disease_groups"] = "; ".join(sorted(groups)) if groups else None

        # --- Relationships ---
        if include_rels:
            rels = rels_by_entity.get(entity_id, [])
            row["total_entity_relationships"] = len(rels)

            parent_count = 0
            child_count = 0
            group_counts: dict[str, int] = defaultdict(int)

            for r in rels:
                if r.entity_1_id == entity_id:
                    other_type_id = r.entity_2_type_id
                    # entity_1 is child in is_a/part_of → entity_2 is parent
                    if other_type_id == disease_type_id:
                        parent_count += 1
                else:
                    other_type_id = r.entity_1_type_id
                    # entity_2 is parent in is_a/part_of → entity_1 is child
                    if other_type_id == disease_type_id:
                        child_count += 1

                t = (
                    type_name_by_id.get(other_type_id, "Unknown")
                    if other_type_id
                    else "Unknown"
                )
                group_counts[t] += 1

            row["disease_parent_count"] = parent_count
            row["disease_child_count"] = child_count
            if group_counts:
                row["entity_relationships_by_group"] = "; ".join(
                    f"{k}:{v}" for k, v in sorted(group_counts.items())
                )

        # --- Other aliases ---
        if include_aliases:
            all_aliases = aliases_by_entity.get(entity_id, [])
            known: set[str] = {
                v
                for v in [
                    matched_alias,
                    row.get("disease_id"),
                    row.get("label"),
                    row.get("mondo_id"),
                    row.get("omim_id"),
                    row.get("mesh_id"),
                    row.get("icd10"),
                ]
                if v is not None
            }
            other = [
                a.alias_value
                for a in all_aliases
                if a.alias_value not in known and not a.is_primary
            ]
            row["other_aliases"] = "; ".join(other[:15]) if other else None

        return row
