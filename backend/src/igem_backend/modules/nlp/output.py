"""
Output contracts for the NLP entity resolver.

ResolvedMatch  — single resolved entity mention (the "currency" of the module)
OutputMode     — controls how many candidates are returned per text span
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class OutputMode(Enum):
    BEST_MATCH = "best_match"
    # Top-1 candidate per span, regardless of confidence.
    # For DTP batch mode where one output per input is required.

    ALL_CANDIDATES = "all_candidates"
    # All resolved candidates per span, sorted by confidence desc.
    # For exploratory / interactive use.

    SMART = "smart"
    # If best candidate confidence >= threshold → top-1 (like BEST_MATCH).
    # If best confidence < threshold → all candidates (like ALL_CANDIDATES).
    # Default for most use cases.


@dataclass(frozen=True)
class ResolvedMatch:
    """
    A single entity mention resolved to an entity_id.

    span_start / span_end are character offsets in the NORMALIZED text
    (after normalize() is applied). context is a ~100-char window from
    the same normalized text — sufficient for human review.

    confidence:
      - 1.0    → exact match, no other entity shares this alias_norm
      - 1/N    → exact match, N distinct entities share this alias_norm
      - < 0.5  → fuzzy or embedding match (set by those strategies)

    review_status is set by the strategy:
      - "auto"    → confidence >= threshold, accepted automatically
      - "pending" → ambiguous, needs human review
    """

    # The text span that triggered the match (in normalized form)
    matched_text: str
    span_start: int
    span_end: int
    context: str

    # Entity resolution
    entity_id: int
    entity_primary_name: str
    entity_type_id: int
    entity_type_name: str
    entity_domain: str

    # Alias that matched
    alias_id: int
    alias_value: str
    alias_type: str             # preferred | synonym | code
    xref_source: Optional[str]

    # Match metadata
    match_method: str           # exact_aho | code_pattern | tsvector | fuzzy | embedding
    confidence: float           # 0.0 – 1.0
    review_status: str          # auto | pending | confirmed | rejected

    # Source provenance (filled by caller / resolver)
    source_record_id: Optional[str] = None
    source_field: Optional[str] = None


# Priority order for picking the "best" alias entry when multiple
# aliases of the same entity match the same span.
_ALIAS_TYPE_PRIORITY: dict[str, int] = {
    "preferred": 0,
    "synonym": 1,
    "code": 2,
}


def alias_type_priority(alias_type: str) -> int:
    return _ALIAS_TYPE_PRIORITY.get(alias_type, 99)
