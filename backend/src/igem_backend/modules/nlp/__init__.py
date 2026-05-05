"""
IGEM NLP module — entity resolution and relationship extraction from text.

Public entry points:
    EntityResolver  — resolves text spans to entity_ids
    AliasDictionary — in-memory alias cache with Aho-Corasick automaton
    normalize       — canonical text normalization (shared with ETL)
    OutputMode      — controls how many candidates are returned per span
    ResolvedMatch   — resolved entity mention (output contract)
"""

from igem_backend.modules.nlp.normalizer import normalize, normalize_code
from igem_backend.modules.nlp.dictionary import AliasDictionary, AliasEntry
from igem_backend.modules.nlp.output import OutputMode, ResolvedMatch
from igem_backend.modules.nlp.resolver import EntityResolver

__all__ = [
    "normalize",
    "normalize_code",
    "AliasDictionary",
    "AliasEntry",
    "OutputMode",
    "ResolvedMatch",
    "EntityResolver",
]
