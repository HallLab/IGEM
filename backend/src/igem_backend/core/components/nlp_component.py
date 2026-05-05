from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

from igem_backend.core.components.base_component import BaseComponent

if TYPE_CHECKING:
    import pandas as pd
    from igem_backend.modules.nlp import (
        EntityResolver,
        OutputMode,
        ResolvedMatch,
    )


class NLPComponent(BaseComponent):
    """
    NLP entity-resolution component.

    Wraps `igem_backend.modules.nlp.EntityResolver` as a singleton bound to
    this `GE` instance. The first `resolve_*` call builds the
    `AliasDictionary` (~1-3 min for ~10M aliases); subsequent calls reuse
    the in-memory automaton (~3 ms per scan).

    Usage
    -----
        ge = GE(db_uri="postgresql://…/igem")

        # First call: build dictionary then scan
        matches = ge.nlp.resolve_text(
            "BRCA1 mutations cause breast cancer"
        )

        # Subsequent calls reuse the cached dictionary
        matches2 = ge.nlp.resolve_text("metformin treats diabetes")

        # Resolve a list of strings
        result = ge.nlp.resolve_list(
            ["BRCA1", "metformin"], mode="all_candidates"
        )

        # If new ETL packages have landed, reload the dictionary:
        ge.nlp.reload_dictionary()

    The legacy `resolve(span)` method (simple alias-norm lookup) is kept
    for backward compatibility but is shallower than `resolve_text` —
    new code should prefer the latter.
    """

    def __init__(self, core):
        super().__init__(core)
        self._resolver: Optional["EntityResolver"] = None
        self._resolver_session = None  # session held for the resolver lifetime

    # ------------------------------------------------------------------
    # Resolver lifecycle
    # ------------------------------------------------------------------
    def _get_resolver(
        self,
        type_names: Optional[list[str]] = None,
        domains: Optional[list[str]] = None,
        confidence_threshold: float = 0.9,
        min_alias_length: Optional[int] = None,
        stopwords: Optional[frozenset[str]] = None,
    ) -> "EntityResolver":
        if self._resolver is None:
            from igem_backend.modules.nlp import EntityResolver

            self.core.logger.log(
                "Building EntityResolver (this may take 1-3 min on full DB)…",
                "INFO",
            )
            self._resolver_session = self.core.require_db().get_session()
            self._resolver = EntityResolver(
                session=self._resolver_session,
                type_names=type_names,
                domains=domains,
                confidence_threshold=confidence_threshold,
                min_alias_length=min_alias_length,
                stopwords=stopwords,
            )
            self.core.logger.log(
                f"EntityResolver ready "
                f"({self._resolver._dictionary.entry_count:,} aliases, "
                f"{self._resolver._dictionary.norm_count:,} norms)",
                "INFO",
            )
        return self._resolver

    def reload_dictionary(self) -> None:
        """Rebuild the AliasDictionary from current DB state."""
        if self._resolver is None:
            return
        self.core.logger.log("Reloading AliasDictionary…", "INFO")
        self._resolver.reload_dictionary()

    def is_dictionary_stale(self) -> bool:
        """True if new ETLPackages have landed since the resolver was built."""
        if self._resolver is None:
            return False
        return self._resolver.is_dictionary_stale()

    # ------------------------------------------------------------------
    # Public resolve_* API (delegates to EntityResolver)
    # ------------------------------------------------------------------
    @staticmethod
    def _coerce_mode(
        mode: Union[str, "OutputMode"]
    ) -> "OutputMode":
        from igem_backend.modules.nlp import OutputMode
        if isinstance(mode, OutputMode):
            return mode
        return OutputMode(mode)

    def resolve_text(
        self,
        text: str,
        mode: Union[str, "OutputMode"] = "smart",
        source_record_id: Optional[str] = None,
        source_field: Optional[str] = None,
    ) -> list["ResolvedMatch"]:
        """
        Resolve a single text string to a list of ResolvedMatch.

        Parameters
        ----------
        text:
            Free text to scan.
        mode:
            "smart" (default), "best_match", or "all_candidates".
        source_record_id, source_field:
            Optional provenance fields written into each match.
        """
        resolver = self._get_resolver()
        return resolver.resolve_text(
            text=text,
            mode=self._coerce_mode(mode),
            source_record_id=source_record_id,
            source_field=source_field,
        )

    def resolve_list(
        self,
        texts: list[str],
        mode: Union[str, "OutputMode"] = "smart",
        source_field: Optional[str] = None,
    ) -> dict[int, list["ResolvedMatch"]]:
        """Resolve a list of strings — returns {index: matches}."""
        resolver = self._get_resolver()
        return resolver.resolve_list(
            texts=texts,
            mode=self._coerce_mode(mode),
            source_field=source_field,
        )

    def resolve_dataframe(
        self,
        df: "pd.DataFrame",
        text_columns: list[str],
        id_column: Optional[str] = None,
        mode: Union[str, "OutputMode"] = "smart",
    ) -> list["ResolvedMatch"]:
        """Resolve one or more text columns in a DataFrame."""
        resolver = self._get_resolver()
        return resolver.resolve_dataframe(
            df=df,
            text_columns=text_columns,
            id_column=id_column,
            mode=self._coerce_mode(mode),
        )

    # ------------------------------------------------------------------
    # Legacy direct lookup — kept for backward compatibility
    # ------------------------------------------------------------------
    def resolve(self, span: str) -> list[dict]:
        """
        Legacy: direct EntityAlias lookup by exact normalized text.

        Returns dict-shaped results. For full Aho-Corasick scanning with
        confidence scoring, use `resolve_text()` instead.
        """
        db = self.require_db()
        from igem_backend.modules.db.models.model_entities import EntityAlias
        from igem_backend.utils.text import normalize_text

        norm = normalize_text(span)
        with db.get_session() as session:
            matches = (
                session.query(EntityAlias)
                .filter(
                    EntityAlias.alias_norm == norm,
                    EntityAlias.is_active.is_(True),
                )
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

    def extract_entities(self, text: str) -> list[dict]:
        """ScispaCy NER pre-filter — not yet implemented."""
        raise NotImplementedError(
            "ScispaCy NER pipeline not yet implemented."
        )
