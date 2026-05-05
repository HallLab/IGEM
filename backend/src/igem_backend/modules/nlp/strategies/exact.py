"""
Exact Aho-Corasick strategy with word-boundary enforcement.

Pipeline:
  1. dictionary.search(text) → AutomatonMatch hits (positions in normalized text)
  2. Word-boundary check — rejects partial matches ("brca1" inside "brca1a")
  3. Group entries by entity_id — counts distinct entities per span
  4. Compute confidence = 1.0 / n_distinct_entities
  5. Select best alias entry per entity (preferred > synonym > code)
  6. Return ResolvedMatch list (one per distinct entity per span)

Boundary rule:
  A character is a "boundary" if it is a space, punctuation, or string edge.
  Applied on the normalized text (hyphens already converted to spaces).
"""

from __future__ import annotations

from typing import Optional

from igem_backend.modules.nlp.dictionary import AliasDictionary, AliasEntry
from igem_backend.modules.nlp.normalizer import normalize
from igem_backend.modules.nlp.output import ResolvedMatch, alias_type_priority

_CONTEXT_WINDOW = 100


def resolve(
    text: str,
    dictionary: AliasDictionary,
    confidence_threshold: float = 0.9,
    source_record_id: Optional[str] = None,
    source_field: Optional[str] = None,
) -> list[ResolvedMatch]:
    """
    Run exact Aho-Corasick matching on *text* and return resolved matches.

    Parameters
    ----------
    text:
        Raw input text (normalization is applied internally).
    dictionary:
        Loaded AliasDictionary instance.
    confidence_threshold:
        Matches below this threshold receive review_status="pending".
    source_record_id / source_field:
        Provenance metadata copied into every ResolvedMatch.

    Returns
    -------
    List of ResolvedMatch sorted by span_start, then confidence desc.
    """
    if not text or not text.strip():
        return []

    norm_text = normalize(text)
    hits = dictionary.search(text)   # search() normalizes internally
    results: list[ResolvedMatch] = []

    for hit in hits:
        if not _is_word_boundary(norm_text, hit.start, hit.end):
            continue

        # Group AliasEntry rows by entity_id
        by_entity: dict[int, list[AliasEntry]] = {}
        for entry in hit.entries:
            by_entity.setdefault(entry.entity_id, []).append(entry)

        n_entities = len(by_entity)
        confidence = 1.0 / n_entities
        review_status = (
            "auto" if confidence >= confidence_threshold else "pending"
        )
        context = _extract_context(norm_text, hit.start, hit.end)

        for entity_id, entries in by_entity.items():
            best = _best_alias(entries)
            primary_name = dictionary.get_primary_name(entity_id)
            results.append(
                ResolvedMatch(
                    matched_text=hit.alias_norm,
                    span_start=hit.start,
                    span_end=hit.end,
                    context=context,
                    entity_id=entity_id,
                    entity_primary_name=primary_name,
                    entity_type_id=best.entity_type_id,
                    entity_type_name=best.entity_type_name,
                    entity_domain=best.entity_domain,
                    alias_id=best.alias_id,
                    alias_value=best.alias_value,
                    alias_type=best.alias_type,
                    xref_source=best.xref_source,
                    match_method="exact_aho",
                    confidence=confidence,
                    review_status=review_status,
                    source_record_id=source_record_id,
                    source_field=source_field,
                )
            )

    results.sort(key=lambda r: (r.span_start, -r.confidence))
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_word_boundary(text: str, start: int, end: int) -> bool:
    """
    Return True if the match at [start, end] sits on word boundaries.

    A boundary is: string edge OR a non-alphanumeric character.
    Greek letters and digits count as alphanumeric (str.isalnum() handles them).
    """
    before_ok = (start == 0) or (not text[start - 1].isalnum())
    after_ok = (end >= len(text) - 1) or (not text[end + 1].isalnum())
    return before_ok and after_ok


def _extract_context(text: str, start: int, end: int) -> str:
    """Return a ~100-char window centred on the matched span."""
    half = _CONTEXT_WINDOW // 2
    lo = max(0, start - half)
    hi = min(len(text), end + half + 1)
    return text[lo:hi]


def _best_alias(entries: list[AliasEntry]) -> AliasEntry:
    """
    Pick the most informative alias entry for a given entity.

    Priority: preferred > synonym > code.
    Ties broken by alias_id (stable ordering).
    """
    return min(entries, key=lambda e: (alias_type_priority(e.alias_type), e.alias_id))
