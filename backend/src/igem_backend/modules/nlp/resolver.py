"""
EntityResolver — public API for the NLP entity resolution module.

Instantiate once per batch (AliasDictionary is built on init).
Reuse the instance across multiple resolve calls for performance.

Usage (DTP batch mode):
    resolver = EntityResolver(session, domains=["Exposome"])
    for row in df.itertuples():
        matches = resolver.resolve_text(
            row.description,
            mode=OutputMode.BEST_MATCH,
            source_record_id=row.accession,
            source_field="description",
        )

Usage (interactive / explorer mode):
    resolver = EntityResolver(session)
    matches = resolver.resolve_text(
        "lead poisoning in childhood",
        mode=OutputMode.ALL_CANDIDATES,
    )
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from igem_backend.modules.nlp.dictionary import AliasDictionary
from igem_backend.modules.nlp.output import OutputMode, ResolvedMatch
from igem_backend.modules.nlp.strategies import exact as exact_strategy

logger = logging.getLogger(__name__)


class EntityResolver:
    """
    Resolves free text to EntityRelationship-ready ResolvedMatch objects.

    Strategies executed in order (first match wins per span):
      Phase 1:  exact_aho   — Aho-Corasick on alias_norm + boundary check
      Phase 3+: code_pattern, tsvector, fuzzy, embedding (future)

    Parameters
    ----------
    session:
        Active SQLAlchemy session. Must remain open for the resolver lifetime.
    type_names:
        Load only aliases belonging to these EntityType names.
        e.g. ["Genes", "Chemicals"]. None = load all.
    domains:
        Load only aliases from these EntityDomain values.
        e.g. ["Exposome"]. None = load all.
    confidence_threshold:
        Spans with best confidence >= threshold → review_status="auto".
        Spans below threshold → review_status="pending".
        Default 0.9.
    """

    def __init__(
        self,
        session: Session,
        type_names: Optional[list[str]] = None,
        domains: Optional[list[str]] = None,
        confidence_threshold: float = 0.9,
        min_alias_length: Optional[int] = None,
        stopwords: Optional[frozenset[str]] = None,
    ) -> None:
        self._session = session
        self._threshold = confidence_threshold
        dict_kwargs: dict = {
            "type_names": type_names,
            "domains": domains,
        }
        if min_alias_length is not None:
            dict_kwargs["min_alias_length"] = min_alias_length
        if stopwords is not None:
            dict_kwargs["stopwords"] = stopwords
        self._dictionary = AliasDictionary(session, **dict_kwargs).load()
        logger.info(
            "EntityResolver ready: %d aliases / %d norms loaded",
            self._dictionary.entry_count,
            self._dictionary.norm_count,
        )

    # ------------------------------------------------------------------
    # Core resolution methods
    # ------------------------------------------------------------------

    def resolve_text(
        self,
        text: str,
        mode: OutputMode = OutputMode.SMART,
        source_record_id: Optional[str] = None,
        source_field: Optional[str] = None,
    ) -> list[ResolvedMatch]:
        """
        Resolve a single text string.

        Returns a list of ResolvedMatch. With BEST_MATCH, at most one match
        per span (highest confidence). With ALL_CANDIDATES, all candidates
        per span. With SMART, best-only when confident, all when ambiguous.
        """
        candidates = exact_strategy.resolve(
            text=text,
            dictionary=self._dictionary,
            confidence_threshold=self._threshold,
            source_record_id=source_record_id,
            source_field=source_field,
        )
        return self._apply_mode(candidates, mode)

    def resolve_list(
        self,
        texts: list[str],
        mode: OutputMode = OutputMode.SMART,
        source_field: Optional[str] = None,
    ) -> dict[int, list[ResolvedMatch]]:
        """
        Resolve a list of strings. Returns {index → matches}.
        Index matches the position in the input list.
        """
        return {
            i: self.resolve_text(
                text,
                mode=mode,
                source_record_id=str(i),
                source_field=source_field,
            )
            for i, text in enumerate(texts)
        }

    def resolve_dataframe(
        self,
        df: pd.DataFrame,
        text_columns: list[str],
        id_column: Optional[str] = None,
        mode: OutputMode = OutputMode.SMART,
    ) -> list[ResolvedMatch]:
        """
        Resolve one or more text columns in a DataFrame.

        Each (row, column) pair is resolved independently.
        source_record_id = value of id_column (or str(row_index) if absent).
        source_field = column name.

        Returns a flat list of all ResolvedMatch across all rows and columns.
        """
        results: list[ResolvedMatch] = []
        for idx, row in df.iterrows():
            record_id = str(row[id_column]) if id_column else str(idx)
            for col in text_columns:
                text = str(row.get(col) or "").strip()
                if not text:
                    continue
                matches = self.resolve_text(
                    text,
                    mode=mode,
                    source_record_id=record_id,
                    source_field=col,
                )
                results.extend(matches)
        return results

    # ------------------------------------------------------------------
    # Stale check
    # ------------------------------------------------------------------

    def is_dictionary_stale(self) -> bool:
        """True if new ETL packages landed after this resolver was built."""
        return self._dictionary.is_stale()

    def reload_dictionary(self) -> None:
        """Rebuild the AliasDictionary from the current DB state."""
        self._dictionary.load()
        logger.info(
            "EntityResolver dictionary reloaded: %d aliases",
            self._dictionary.entry_count,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _apply_mode(
        self,
        candidates: list[ResolvedMatch],
        mode: OutputMode,
    ) -> list[ResolvedMatch]:
        if mode == OutputMode.ALL_CANDIDATES:
            return candidates

        # Group by span (start, end) to apply per-span logic
        spans: dict[tuple[int, int], list[ResolvedMatch]] = {}
        for m in candidates:
            spans.setdefault((m.span_start, m.span_end), []).append(m)

        result: list[ResolvedMatch] = []
        for span_matches in spans.values():
            # Sort by confidence descending within each span
            span_matches.sort(key=lambda m: -m.confidence)
            best_conf = span_matches[0].confidence

            if mode == OutputMode.BEST_MATCH:
                result.append(span_matches[0])
            else:  # SMART
                if best_conf >= self._threshold:
                    result.append(span_matches[0])
                else:
                    result.extend(span_matches)

        result.sort(key=lambda m: (m.span_start, -m.confidence))
        return result
