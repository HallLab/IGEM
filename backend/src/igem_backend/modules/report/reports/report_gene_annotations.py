from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from sqlalchemy import or_
from sqlalchemy.orm import Session

from igem_backend.modules.report.reports.base_report import ReportBase


class GeneAnnotationsReport(ReportBase):
    """
    Master annotation report for human genes.

    Accepts a list of gene identifiers (symbols, HGNC IDs, Ensembl IDs,
    Entrez IDs, or any registered alias). Returns one row per matched entity
    with consolidated cross-references, genomic coordinates, and relationship
    summary. Input values with no match return a 'not_found' row.
    """

    REPORT_NAME = "gene_annotations"
    REPORT_VERSION = "1.0.0"
    REPORT_DESCRIPTION = (
        "Master gene annotation table: cross-references, locus classification, "
        "genomic coordinates and relationship summary per gene."
    )

    def available_columns(self) -> list[str]:
        return [
            "input_value",
            "input_matched_alias",
            "entity_id",
            "gene_symbol",
            "hgnc_id",
            "ensembl_id",
            "entrez_id",
            "hgnc_status",
            "gene_locus_group",
            "gene_locus_type",
            "gene_groups",
            "assembly",
            "chromosome",
            "start_position",
            "end_position",
            "strand",
            "entity_relationships_by_group",
            "total_entity_relationships",
            "other_aliases",
            "status",
            "note",
        ]

    def example_input(self) -> dict[str, Any]:
        return {
            "input_values": ["TP53", "BRCA1", "EGFR"],
            "assembly": "GRCh38.p14",
        }

    # -------------------------------------------------------------------------
    # Main run
    # -------------------------------------------------------------------------
    def run(self, session: Session, **kwargs) -> pd.DataFrame:
        from igem_backend.modules.db.models.model_config import GenomeAssembly
        from igem_backend.modules.db.models.model_entities import (
            Entity,
            EntityAlias,
            EntityLocation,
            EntityRelationship,
            EntityType,
        )
        from igem_backend.modules.db.models.model_genes import (
            GeneMaster,
        )

        inputs = self.resolve_input_list(self.param(kwargs, "input_values"))
        assembly_name = self.param(kwargs, "assembly", "GRCh38.p14")

        # --- Resolve gene entity type ---
        gene_type = (
            session.query(EntityType).filter_by(name="Genes").one_or_none()
        )
        if gene_type is None:
            return pd.DataFrame(columns=self.available_columns())

        # --- Resolve genome assembly ---
        assembly = (
            session.query(GenomeAssembly)
            .filter_by(assembly_name=assembly_name)
            .first()
        )
        assembly_id: Optional[int] = assembly.id if assembly else None

        # --- Match input values against EntityAlias.alias_norm ---
        norm_to_original: dict[str, str] = {}
        if inputs:
            for v in inputs:
                norm = v.strip().lower()
                if norm:
                    norm_to_original[norm] = v

        matched: dict[str, tuple[int, str]] = {}  # norm → (entity_id, alias_value)
        if norm_to_original:
            aliases_found = (
                session.query(EntityAlias)
                .filter(
                    EntityAlias.alias_norm.in_(norm_to_original.keys()),
                    EntityAlias.type_id == gene_type.id,
                )
                .all()
            )
            for a in aliases_found:
                if a.alias_norm not in matched:
                    matched[a.alias_norm] = (a.entity_id, a.alias_value)

        # --- Build result rows ---
        rows: list[dict] = []

        if norm_to_original:
            for norm, original in norm_to_original.items():
                if norm not in matched:
                    rows.append(self._not_found_row(original))
                    continue
                entity_id, alias_value = matched[norm]
                rows.append(
                    self._build_row(
                        session,
                        entity_id=entity_id,
                        matched_alias=alias_value,
                        input_value=original,
                        gene_type_id=gene_type.id,
                        assembly_id=assembly_id,
                        assembly_name=assembly_name,
                    )
                )
        else:
            # No input → return all genes
            for gm in session.query(GeneMaster).all():
                rows.append(
                    self._build_row(
                        session,
                        entity_id=gm.entity_id,
                        matched_alias=gm.symbol,
                        input_value=None,
                        gene_type_id=gene_type.id,
                        assembly_id=assembly_id,
                        assembly_name=assembly_name,
                    )
                )

        df = pd.DataFrame(rows, columns=self.available_columns())
        return df

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------
    def _not_found_row(self, input_value: str) -> dict:
        row: dict = {col: None for col in self.available_columns()}
        row["input_value"] = input_value
        row["status"] = "not_found"
        row["note"] = "No matching gene alias found"
        return row

    def _build_row(
        self,
        session: Session,
        entity_id: int,
        matched_alias: Optional[str],
        input_value: Optional[str],
        gene_type_id: int,
        assembly_id: Optional[int],
        assembly_name: str,
    ) -> dict:
        from igem_backend.modules.db.models.model_entities import (
            EntityAlias,
            EntityLocation,
            EntityRelationship,
            EntityType,
            Entity,
        )
        from igem_backend.modules.db.models.model_genes import GeneMaster

        row: dict = {col: None for col in self.available_columns()}
        row["input_value"] = input_value
        row["input_matched_alias"] = matched_alias
        row["entity_id"] = entity_id
        row["status"] = "found"

        # --- GeneMaster ---
        gm = (
            session.query(GeneMaster)
            .filter_by(entity_id=entity_id)
            .one_or_none()
        )
        if gm:
            row["gene_symbol"] = gm.symbol
            row["hgnc_status"] = gm.hgnc_status
            row["chromosome"] = gm.chromosome
            row["gene_locus_group"] = (
                gm.locus_group.name if gm.locus_group else None
            )
            row["gene_locus_type"] = (
                gm.locus_type.name if gm.locus_type else None
            )
            # Gene groups
            groups = sorted(
                m.gene_group.name
                for m in gm.group_memberships
                if m.gene_group
            )
            row["gene_groups"] = "; ".join(groups) if groups else None

        # --- Entity aliases: extract known cross-refs ---
        all_aliases = (
            session.query(EntityAlias)
            .filter_by(entity_id=entity_id)
            .all()
        )
        hgnc_id = next(
            (
                a.alias_value
                for a in all_aliases
                if a.xref_source == "HGNC" and a.alias_type == "code"
            ),
            None,
        )
        ensembl_id = next(
            (
                a.alias_value
                for a in all_aliases
                if a.xref_source == "ENSEMBL" and a.alias_type == "code"
            ),
            None,
        )
        entrez_id = next(
            (
                a.alias_value
                for a in all_aliases
                if a.xref_source == "NCBI" and a.alias_type == "code"
            ),
            None,
        )
        row["hgnc_id"] = hgnc_id
        row["ensembl_id"] = ensembl_id
        row["entrez_id"] = entrez_id

        # Other aliases: exclude already-captured IDs and primary symbol
        known_values = {
            v
            for v in [
                hgnc_id,
                ensembl_id,
                entrez_id,
                row.get("gene_symbol"),
                matched_alias,
            ]
            if v is not None
        }
        other = [
            a.alias_value
            for a in all_aliases
            if a.alias_value not in known_values and not a.is_primary
        ]
        row["other_aliases"] = "; ".join(other[:15]) if other else None

        # --- Genomic coordinates ---
        if assembly_id is not None:
            loc = (
                session.query(EntityLocation)
                .filter_by(entity_id=entity_id, assembly_id=assembly_id)
                .one_or_none()
            )
            if loc:
                row["assembly"] = assembly_name
                row["start_position"] = loc.start_pos
                row["end_position"] = loc.end_pos
                row["strand"] = loc.strand

        # --- Relationships ---
        rels = (
            session.query(EntityRelationship)
            .filter(
                or_(
                    EntityRelationship.entity_1_id == entity_id,
                    EntityRelationship.entity_2_id == entity_id,
                )
            )
            .all()
        )
        row["total_entity_relationships"] = len(rels)

        if rels:
            type_counts: dict[str, int] = {}
            for r in rels:
                other_id = (
                    r.entity_2_id if r.entity_1_id == entity_id else r.entity_1_id
                )
                other_entity = session.get(Entity, other_id)
                if other_entity and other_entity.entity_type:
                    t = other_entity.entity_type.name
                    type_counts[t] = type_counts.get(t, 0) + 1
            row["entity_relationships_by_group"] = "; ".join(
                f"{k}:{v}" for k, v in sorted(type_counts.items())
            )

        return row
