from __future__ import annotations

from igem_backend.core.components.base_component import BaseComponent


class NLPComponent(BaseComponent):
    """
    NLP component for Omic entity extraction from text sources.

    Planned capabilities:
    - scispaCy NER to identify Omic spans in free text
    - EntityAlias-based resolution: text span → canonical Entity
    - PubMed abstract / full-text ingestion pipeline
    - Confidence-scored EntityRelationship creation (discovery_method='nlp')

    Status: placeholder — to be implemented.
    """

    def extract_entities(self, text: str) -> list[dict]:
        raise NotImplementedError("NLP pipeline not yet implemented.")

    def resolve(self, span: str) -> list[dict]:
        """Resolve a text span to candidate entities via EntityAlias lookup."""
        db = self.require_db()
        from igem_backend.modules.db.models.model_entities import EntityAlias
        from igem_backend.utils.text import normalize_text

        norm = normalize_text(span)
        with db.get_session() as session:
            matches = (
                session.query(EntityAlias)
                .filter(EntityAlias.alias_norm == norm, EntityAlias.is_active.is_(True))
                .limit(20)
                .all()
            )
            return [
                {
                    "entity_id": m.entity_id,
                    "alias_value": m.alias_value,
                    "alias_type": m.alias_type,
                    "xref_source": m.xref_source,
                }
                for m in matches
            ]
