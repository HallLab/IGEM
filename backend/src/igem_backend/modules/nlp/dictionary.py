"""
AliasDictionary — in-memory alias cache with Aho-Corasick automaton.

Build once per process (or per domain filter set); reuse across all
resolve calls. Stale detection compares the highest ETLPackage id seen
at load time with the current DB value.

Usage:
    dictionary = AliasDictionary(session, domains=["Genomics"]).load()
    matches = dictionary.search("BRCA1 overexpression in breast cancer")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import ahocorasick
from sqlalchemy.orm import Session

from igem_backend.modules.nlp.normalizer import normalize

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AliasEntry:
    """Lightweight snapshot of one EntityAlias row + its EntityType metadata."""

    alias_id: int
    alias_value: str        # original, unnormalized
    alias_norm: str         # normalized form used as Aho-Corasick key
    alias_type: str         # preferred | synonym | code
    xref_source: Optional[str]
    entity_id: int
    entity_type_id: int
    entity_type_name: str   # "Genes", "Chemicals", …
    entity_domain: str      # "Genomics", "Exposome", "Knowledge"


@dataclass(frozen=True)
class AutomatonMatch:
    """Raw match returned by the Aho-Corasick scan before boundary checks."""

    alias_norm: str
    start: int
    end: int                # inclusive
    entries: tuple[AliasEntry, ...]


# Default minimum length for an alias_norm to be loaded into the dictionary.
# Shorter strings (e.g. "s", "in") collide with English particles and
# cause false positives during free-text scanning.
DEFAULT_MIN_ALIAS_LENGTH: int = 3

# Common biomedical / English terms that exist as standalone aliases in
# upstream sources but match too broadly in free text. They are dropped
# at AliasDictionary build time so the Aho-Corasick scan never produces
# a hit for them. Override via AliasDictionary(..., stopwords=set()) if
# you need the full vocabulary (e.g. for code-only resolution tasks).
DEFAULT_ALIAS_STOPWORDS: frozenset[str] = frozenset({
    # Generic biomedical / scientific concepts
    "disease", "factor", "protein", "gene", "cell", "human", "type",
    "system", "patient", "study", "level", "case", "group", "role",
    "form", "site", "size", "name", "code", "value", "data", "set",
    "test", "result", "rate", "time", "year", "day", "age", "sex",
    "model", "method", "process", "function", "pathway", "complex",
    # Common English particles that can sneak past min-length
    "the", "and", "with", "for", "from", "into", "this", "that",
    "these", "those", "any", "all", "such", "more", "most", "less",
    "very", "many", "some", "than", "then", "out", "off",
})


class AliasDictionary:
    """
    In-memory alias dictionary backed by Aho-Corasick for O(n) text scanning.

    Optional filters narrow the loaded set:
      - type_names: e.g. ["Genes", "Chemicals"] — loads only these entity types
      - domains:    e.g. ["Exposome"]           — loads only entities in these domains

    The automaton key is alias_norm (from EntityAlias.alias_norm).
    Multiple AliasEntry rows can share the same alias_norm (ambiguous aliases).
    The resolver decides how to handle ambiguity based on confidence strategy.
    """

    def __init__(
        self,
        session: Session,
        type_names: Optional[list[str]] = None,
        domains: Optional[list[str]] = None,
        min_alias_length: int = DEFAULT_MIN_ALIAS_LENGTH,
        stopwords: Optional[frozenset[str]] = None,
    ) -> None:
        self._session = session
        self._type_names = type_names
        self._domains = domains
        self._min_alias_length = min_alias_length
        self._stopwords = (
            stopwords if stopwords is not None else DEFAULT_ALIAS_STOPWORDS
        )

        self._entries: list[AliasEntry] = []
        self._norm_index: dict[str, list[AliasEntry]] = {}
        self._automaton: Optional[ahocorasick.Automaton] = None
        self._package_version: Optional[int] = None
        self._n_filtered_short: int = 0
        self._n_filtered_stopword: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> "AliasDictionary":
        """
        Build the dictionary's indexes and automaton.

        Two paths:
          1. Snapshot cache hit — if the DB engine is connected to a
             Parquet snapshot AND that snapshot has a pre-built NLP
             cache (built by `igem-server db snapshot-nlp`), load it
             directly in seconds.
          2. From DB — query EntityAlias and build the automaton
             in-memory (~30-90s on full IGEM).

        Returns self for chaining: AliasDictionary(session).load()
        """
        if self._try_load_from_snapshot_cache():
            logger.info(
                "AliasDictionary loaded from snapshot cache: "
                "%d aliases, %d unique norms",
                len(self._entries),
                len(self._norm_index),
            )
            return self

        self._load_entries()
        self._build_norm_index()
        self._build_automaton()
        self._package_version = self._current_package_version()
        logger.info(
            "AliasDictionary built from DB: %d aliases, %d unique norms "
            "(type_filter=%s domain_filter=%s; "
            "filtered: %d short < %d chars, %d stopwords)",
            len(self._entries),
            len(self._norm_index),
            self._type_names,
            self._domains,
            self._n_filtered_short,
            self._min_alias_length,
            self._n_filtered_stopword,
        )
        return self

    def _try_load_from_snapshot_cache(self) -> bool:
        """
        If the session's engine is bound to a Parquet snapshot AND the
        snapshot has a pre-built NLP cache, restore state from it.
        Returns True on cache hit, False to fall through to DB build.

        Filtered loads (type_names / domains) skip the cache because
        the cache holds the full vocabulary; sub-setting would require
        a different cache file or post-load filter.
        """
        if self._type_names or self._domains:
            return False

        try:
            engine = self._session.get_bind()
        except Exception:
            return False

        # The Database wrapper sets engine.snapshot_dir when in
        # snapshot mode; absence of the attribute means SQL backend.
        snap_dir = getattr(engine, "snapshot_dir", None)
        if snap_dir is None:
            return False

        from igem_backend.modules.db.snapshot_nlp import load_nlp_cache
        state = load_nlp_cache(snap_dir)
        if state is None:
            return False

        self._entries = state["entries"]
        self._norm_index = state["norm_index"]
        self._automaton = state["automaton"]
        self._name_cache = state.get("name_cache", {})
        self._package_version = state.get("package_version")
        self._min_alias_length = state.get(
            "min_alias_length", self._min_alias_length
        )
        self._stopwords = state.get("stopwords", self._stopwords)
        self._n_filtered_short = state.get("n_filtered_short", 0)
        self._n_filtered_stopword = state.get("n_filtered_stopword", 0)
        return True

    def search(self, text: str) -> list[AutomatonMatch]:
        """
        Scan text and return all Aho-Corasick hits (no boundary filtering).
        The caller (strategy layer) applies word-boundary and confidence logic.

        Returns matches sorted by start position.
        """
        if self._automaton is None:
            raise RuntimeError("Call load() before search().")
        if not text:
            return []

        norm_text = normalize(text)
        hits: list[AutomatonMatch] = []

        for end_idx, (alias_norm, entries) in self._automaton.iter(norm_text):
            start_idx = end_idx - len(alias_norm) + 1
            hits.append(
                AutomatonMatch(
                    alias_norm=alias_norm,
                    start=start_idx,
                    end=end_idx,
                    entries=tuple(entries),
                )
            )

        hits.sort(key=lambda h: h.start)
        return hits

    def lookup(self, text: str) -> list[AliasEntry]:
        """
        Direct lookup by exact normalized text. O(1).
        Returns all AliasEntry rows whose alias_norm equals normalize(text).
        """
        return self._norm_index.get(normalize(text), [])

    def lookup_norm(self, norm: str) -> list[AliasEntry]:
        """Direct lookup by already-normalized string. O(1)."""
        return self._norm_index.get(norm, [])

    def is_stale(self) -> bool:
        """True if new ETL packages have been loaded since last build."""
        return self._current_package_version() != self._package_version

    def get_primary_name(self, entity_id: int) -> str:
        """
        Return the primary display name for an entity.

        Pure dict lookup — the cache is populated upfront in load() from
        the same EntityAlias rows we scan, so this method NEVER calls
        the database. That guarantee is critical: per-match session
        queries during long batch runs (e.g. 217k HMDB descriptions ×
        many matches each) leak transaction state and exhaust memory.
        """
        return self._name_cache.get(entity_id, f"entity:{entity_id}")

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    @property
    def norm_count(self) -> int:
        return len(self._norm_index)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_entries(self) -> None:
        from igem_backend.modules.db.models.model_entities import (
            EntityAlias,
            EntityType,
        )

        q = (
            self._session.query(
                EntityAlias.id,
                EntityAlias.alias_value,
                EntityAlias.alias_norm,
                EntityAlias.alias_type,
                EntityAlias.xref_source,
                EntityAlias.entity_id,
                EntityAlias.type_id,
                EntityAlias.is_primary,
                EntityType.name.label("type_name"),
                EntityType.domain.label("domain"),
            )
            .join(EntityType, EntityAlias.type_id == EntityType.id)
            .filter(EntityAlias.is_active.is_(True))
            .filter(EntityAlias.alias_norm.isnot(None))
        )

        if self._type_names:
            q = q.filter(EntityType.name.in_(self._type_names))
        if self._domains:
            q = q.filter(EntityType.domain.in_(self._domains))

        # Apply min-length and stop-word filters at build time so the
        # automaton never carries entries that are guaranteed false
        # positives in free text.  ALSO populate the primary-name cache
        # in-line: this avoids per-match session.query() lookups during
        # downstream scans (which leak memory across long batch runs).
        entries: list[AliasEntry] = []
        name_cache: dict[int, str] = {}
        n_short = 0
        n_stopword = 0
        for row in q.yield_per(5000):
            # Pre-populate cache from is_primary aliases regardless of
            # length / stopword filters — display names are needed even
            # for entities whose primary alias is short or generic.
            if row.is_primary and row.entity_id not in name_cache:
                name_cache[row.entity_id] = row.alias_value

            norm = row.alias_norm or normalize(row.alias_value)
            if len(norm) < self._min_alias_length:
                n_short += 1
                continue
            if norm in self._stopwords:
                n_stopword += 1
                continue
            entries.append(AliasEntry(
                alias_id=row.id,
                alias_value=row.alias_value,
                alias_norm=norm,
                alias_type=row.alias_type,
                xref_source=row.xref_source,
                entity_id=row.entity_id,
                entity_type_id=row.type_id,
                entity_type_name=row.type_name,
                entity_domain=row.domain,
            ))

        self._entries = entries
        self._n_filtered_short = n_short
        self._n_filtered_stopword = n_stopword
        self._name_cache = name_cache

    def _build_norm_index(self) -> None:
        """Build O(1) lookup dict: alias_norm → list[AliasEntry]."""
        index: dict[str, list[AliasEntry]] = {}
        for entry in self._entries:
            index.setdefault(entry.alias_norm, []).append(entry)
        self._norm_index = index

    def _build_automaton(self) -> None:
        """Build Aho-Corasick automaton from the norm index."""
        A = ahocorasick.Automaton()
        for norm, entries in self._norm_index.items():
            if norm:
                A.add_word(norm, (norm, entries))
        A.make_automaton()
        self._automaton = A

    def _current_package_version(self) -> Optional[int]:
        from igem_backend.modules.db.models.model_etl import ETLPackage
        result = (
            self._session.query(ETLPackage.id)
            .order_by(ETLPackage.id.desc())
            .first()
        )
        return result[0] if result else None
