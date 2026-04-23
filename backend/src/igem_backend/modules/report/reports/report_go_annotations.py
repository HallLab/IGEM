from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

import pandas as pd
from sqlalchemy import or_
from sqlalchemy.orm import Session

from igem_backend.modules.report.reports.base_report import ReportBase


class GOAnnotationsReport(ReportBase):
    """
    Master annotation report for Gene Ontology terms.

    Accepts a list of GO identifiers (GO:xxxxxxx), term names, synonyms, or
    any registered alias. Returns one row per matched entity with namespace,
    name, and relationship summary. Supports filtering by namespace (BP, MF, CC).
    Passing an empty input list returns all GO terms (optionally filtered by
    namespace).
    """

    REPORT_NAME = "go_annotations"
    REPORT_VERSION = "1.0.0"
    REPORT_DESCRIPTION = (
        "Master GO term annotation table: namespace, name, and relationship "
        "summary per Gene Ontology term."
    )

    def available_columns(self) -> list[str]:
        return [
            "input_value",
            "input_matched_alias",
            "entity_id",
            "go_id",
            "go_name",
            "namespace",
            "namespace_label",
            "go_parent_count",
            "go_child_count",
            "entity_relationships_by_group",
            "total_entity_relationships",
            "other_aliases",
            "status",
            "note",
        ]

    def example_input(self) -> dict[str, Any]:
        return {
            "input_values": ["GO:0007049", "GO:0006281", "cell cycle"],
            "namespace": None,
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
        from igem_backend.modules.db.models.model_go import GOMaster, GORelation

        inputs = self.resolve_input_list(self.param(kwargs, "input_values"))
        namespace_filter = self.param(kwargs, "namespace")  # "BP", "MF", "CC" or None
        emit_not_found = self.param(kwargs, "emit_not_found_rows", True)
        include_rels = self.param(kwargs, "include_relationships", True)
        include_aliases = self.param(kwargs, "include_aliases", True)

        _NS_LABEL = {
            "BP": "Biological Process",
            "MF": "Molecular Function",
            "CC": "Cellular Component",
        }

        # --- GO entity type ---
        go_type = (
            session.query(EntityType)
            .filter_by(name="Gene Ontology")
            .one_or_none()
        )
        if go_type is None:
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
                    EntityAlias.type_id == go_type.id,
                )
                .all()
            ):
                if a.alias_norm not in matched:
                    matched[a.alias_norm] = (a.entity_id, a.alias_value)

        # Collect entity_ids
        if all_mode:
            gm_query = session.query(GOMaster)
            if namespace_filter:
                gm_query = gm_query.filter_by(namespace=namespace_filter)
            entity_ids_set: set[int] = {gm.entity_id for gm in gm_query.all()}
        else:
            entity_ids_set = {eid for eid, _ in matched.values()}

        if not entity_ids_set and not all_mode:
            rows: list[dict] = []
            if emit_not_found:
                for original in norm_to_original.values():
                    rows.append(self._not_found_row(original))
            return pd.DataFrame(rows, columns=self.available_columns())

        # ---- 2. Bulk load GOMaster (by entity_id) ----
        gm_by_entity: dict[int, GOMaster] = {}
        go_master_ids: set[int] = set()
        if entity_ids_set:
            for gm in (
                session.query(GOMaster)
                .filter(GOMaster.entity_id.in_(entity_ids_set))
                .all()
            ):
                gm_by_entity[gm.entity_id] = gm
                go_master_ids.add(gm.id)

            # Apply namespace filter in specific-input mode
            if namespace_filter and not all_mode:
                entity_ids_set = {
                    eid
                    for eid, gm in gm_by_entity.items()
                    if gm.namespace == namespace_filter
                }

        # ---- 3. Bulk load GORelation counts (parent/child) ----
        parent_count_by_master: dict[int, int] = defaultdict(int)
        child_count_by_master: dict[int, int] = defaultdict(int)
        if go_master_ids:
            for r in (
                session.query(GORelation)
                .filter(
                    or_(
                        GORelation.child_id.in_(go_master_ids),
                        GORelation.parent_id.in_(go_master_ids),
                    )
                )
                .all()
            ):
                if r.child_id in go_master_ids:
                    parent_count_by_master[r.child_id] += 1
                if r.parent_id in go_master_ids:
                    child_count_by_master[r.parent_id] += 1

        # ---- 4. Bulk load Entity (data source) ----
        entity_by_id: dict[int, Entity] = {}
        if entity_ids_set:
            for e in (
                session.query(Entity)
                .filter(Entity.id.in_(entity_ids_set))
                .all()
            ):
                entity_by_id[e.id] = e

        # ---- 5. Bulk load EntityRelationship ----
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

        # ---- 6. Bulk load EntityAlias ----
        aliases_by_entity: dict[int, list[EntityAlias]] = defaultdict(list)
        if include_aliases and entity_ids_set:
            for a in (
                session.query(EntityAlias)
                .filter(EntityAlias.entity_id.in_(entity_ids_set))
                .all()
            ):
                aliases_by_entity[a.entity_id].append(a)

        # ---- 7. EntityType name lookup ----
        type_name_by_id: dict[int, str] = {
            et.id: et.name for et in session.query(EntityType).all()
        }

        # ---- 8. Build rows ----
        rows = []
        build_args = dict(
            gm_by_entity=gm_by_entity,
            entity_by_id=entity_by_id,
            parent_count_by_master=parent_count_by_master,
            child_count_by_master=child_count_by_master,
            rels_by_entity=rels_by_entity,
            aliases_by_entity=aliases_by_entity,
            type_name_by_id=type_name_by_id,
            ns_label=_NS_LABEL,
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
                if namespace_filter and entity_id not in entity_ids_set:
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
        row["note"] = "No matching GO term alias found"
        return row

    def _build_row(
        self,
        entity_id: int,
        input_value: Optional[str],
        matched_alias: Optional[str],
        gm_by_entity: dict,
        entity_by_id: dict,
        parent_count_by_master: dict,
        child_count_by_master: dict,
        rels_by_entity: dict,
        aliases_by_entity: dict,
        type_name_by_id: dict,
        ns_label: dict,
        include_rels: bool,
        include_aliases: bool,
    ) -> dict:
        row: dict = {col: None for col in self.available_columns()}
        row["input_value"] = input_value
        row["input_matched_alias"] = matched_alias
        row["entity_id"] = entity_id
        row["status"] = "found"

        # --- GOMaster ---
        gm = gm_by_entity.get(entity_id)
        if gm:
            row["go_id"] = gm.go_id
            row["go_name"] = gm.name
            row["namespace"] = gm.namespace
            row["namespace_label"] = ns_label.get(gm.namespace or "", gm.namespace)
            row["go_parent_count"] = parent_count_by_master.get(gm.id, 0)
            row["go_child_count"] = child_count_by_master.get(gm.id, 0)

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
                for v in [matched_alias, row.get("go_id"), row.get("go_name")]
                if v is not None
            }
            other = [
                a.alias_value
                for a in all_aliases
                if a.alias_value not in known and not a.is_primary
            ]
            row["other_aliases"] = "; ".join(other[:15]) if other else None

        return row
