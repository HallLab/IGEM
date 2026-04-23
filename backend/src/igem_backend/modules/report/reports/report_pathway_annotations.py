from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

import pandas as pd
from sqlalchemy import or_
from sqlalchemy.orm import Session

from igem_backend.modules.report.reports.base_report import ReportBase


class PathwayAnnotationsReport(ReportBase):
    """
    Master annotation report for biological pathways.

    Accepts a list of pathway identifiers (Reactome IDs, KEGG IDs, pathway
    names, or any registered alias). Returns one row per matched entity with
    source metadata and relationship summary. Input values with no match
    return a 'not_found' row. Passing an empty input list returns all pathways.
    """

    REPORT_NAME = "pathway_annotations"
    REPORT_VERSION = "1.0.0"
    REPORT_DESCRIPTION = (
        "Master pathway annotation table: source ID, name, source database, "
        "and relationship summary per pathway."
    )

    def available_columns(self) -> list[str]:
        return [
            "input_value",
            "input_matched_alias",
            "entity_id",
            "pathway_id",
            "pathway_name",
            "source_db",
            "organism",
            "pathway_source_system",
            "pathway_data_source",
            "entity_relationships_by_group",
            "total_entity_relationships",
            "other_aliases",
            "status",
            "note",
        ]

    def example_input(self) -> dict[str, Any]:
        return {
            "input_values": ["R-HSA-109581", "hsa04110", "Cell Cycle"],
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
        from igem_backend.modules.db.models.model_pathways import PathwayMaster

        inputs = self.resolve_input_list(self.param(kwargs, "input_values"))
        emit_not_found = self.param(kwargs, "emit_not_found_rows", True)
        include_rels = self.param(kwargs, "include_relationships", True)
        include_aliases = self.param(kwargs, "include_aliases", True)

        # --- Pathway entity type ---
        pathway_type = (
            session.query(EntityType).filter_by(name="Pathways").one_or_none()
        )
        if pathway_type is None:
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
            for a in (
                session.query(EntityAlias)
                .filter(
                    EntityAlias.alias_norm.in_(norm_to_original.keys()),
                    EntityAlias.type_id == pathway_type.id,
                )
                .all()
            ):
                if a.alias_norm not in matched:
                    matched[a.alias_norm] = (a.entity_id, a.alias_value)

        if all_mode:
            entity_ids_set: set[int] = {
                pm.entity_id for pm in session.query(PathwayMaster).all()
            }
        else:
            entity_ids_set = {eid for eid, _ in matched.values()}

        if not entity_ids_set and not all_mode:
            rows: list[dict] = []
            if emit_not_found:
                for original in norm_to_original.values():
                    rows.append(self._not_found_row(original))
            return pd.DataFrame(rows, columns=self.available_columns())

        # ---- 2. Bulk load PathwayMaster (by entity_id) ----
        pm_by_entity: dict[int, PathwayMaster] = {}
        if entity_ids_set:
            for pm in (
                session.query(PathwayMaster)
                .filter(PathwayMaster.entity_id.in_(entity_ids_set))
                .all()
            ):
                pm_by_entity[pm.entity_id] = pm

        # ---- 3. Bulk load Entity (data source metadata) ----
        entity_by_id: dict[int, Entity] = {}
        if entity_ids_set:
            for e in (
                session.query(Entity)
                .filter(Entity.id.in_(entity_ids_set))
                .all()
            ):
                entity_by_id[e.id] = e

        # ---- 4. Bulk load EntityRelationship ----
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

        # ---- 5. Bulk load EntityAlias ----
        aliases_by_entity: dict[int, list[EntityAlias]] = defaultdict(list)
        if include_aliases and entity_ids_set:
            for a in (
                session.query(EntityAlias)
                .filter(EntityAlias.entity_id.in_(entity_ids_set))
                .all()
            ):
                aliases_by_entity[a.entity_id].append(a)

        # ---- 6. EntityType name lookup ----
        type_name_by_id: dict[int, str] = {
            et.id: et.name for et in session.query(EntityType).all()
        }

        # ---- 7. Build rows ----
        rows = []
        build_args = dict(
            pm_by_entity=pm_by_entity,
            entity_by_id=entity_by_id,
            rels_by_entity=rels_by_entity,
            aliases_by_entity=aliases_by_entity,
            type_name_by_id=type_name_by_id,
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
        row["note"] = "No matching pathway alias found"
        return row

    def _build_row(
        self,
        entity_id: int,
        input_value: Optional[str],
        matched_alias: Optional[str],
        pm_by_entity: dict,
        entity_by_id: dict,
        rels_by_entity: dict,
        aliases_by_entity: dict,
        type_name_by_id: dict,
        include_rels: bool,
        include_aliases: bool,
    ) -> dict:
        row: dict = {col: None for col in self.available_columns()}
        row["input_value"] = input_value
        row["input_matched_alias"] = matched_alias
        row["entity_id"] = entity_id
        row["status"] = "found"

        # --- PathwayMaster ---
        pm = pm_by_entity.get(entity_id)
        if pm:
            row["pathway_id"] = pm.pathway_id
            row["source_db"] = pm.source_db
            row["organism"] = pm.organism
            row["pathway_name"] = pm.description

        # --- Data source metadata ---
        entity = entity_by_id.get(entity_id)
        if entity and entity.data_source:
            row["pathway_data_source"] = entity.data_source.name
            ss = getattr(entity.data_source, "source_system", None)
            if ss:
                row["pathway_source_system"] = ss.name

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
                for v in [matched_alias, row.get("pathway_id"), row.get("pathway_name")]
                if v is not None
            }
            other = [
                a.alias_value
                for a in all_aliases
                if a.alias_value not in known and not a.is_primary
            ]
            row["other_aliases"] = "; ".join(other[:15]) if other else None

        return row
