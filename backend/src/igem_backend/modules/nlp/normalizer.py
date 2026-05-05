"""
Canonical text normalization for entity alias matching.

Single source of truth — used by both:
  - ETL pipeline: populates EntityAlias.alias_norm
  - Entity resolver: normalizes input text before Aho-Corasick scan

Both sides MUST produce identical output for the same input.
Any change here requires re-normalizing alias_norm in the database.
"""

import re
import unicodedata

# Separators that should become spaces (hyphens in gene names, etc.)
_SEPARATOR_RE = re.compile(r"[-_/]")
_WHITESPACE_RE = re.compile(r"\s+")


def normalize(text: str) -> str:
    """
    Normalize a text string for stable alias matching.

    Steps:
      1. Lowercase
      2. NFKD decomposition — é → e + combining, fi-ligature → fi
      3. Strip combining characters — removes accent marks
      4. Replace [-_/] with space — "NF-kB" → "nf kb", "HMDB_0001" → "hmdb 0001"
      5. Collapse whitespace and strip edges

    Greek letters (α, β, κ…) are preserved as-is.
    Parentheses and brackets are preserved (e.g. "lead(ii)" stays).
    Numbers and punctuation outside the separator set are preserved.

    Examples:
        normalize("NF-κB")               → "nf κb"
        normalize("acetylsalicylic acid") → "acetylsalicylic acid"
        normalize("IL-6")                → "il 6"
        normalize("MESH:C007309")        → "mesh:c007309"
        normalize("café")                → "cafe"
    """
    if not text:
        return ""
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = _SEPARATOR_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


def normalize_code(text: str) -> str:
    """
    Normalize a structured identifier (CURIE, accession, code).

    Same as normalize() but does NOT replace colons or dots,
    since those are meaningful in IDs like MESH:C007309 or GO:0008150.
    Only lowercases and strips whitespace.

    Examples:
        normalize_code("MESH:C007309") → "mesh:c007309"
        normalize_code("GO:0008150")   → "go:0008150"
        normalize_code("HMDB0001839")  → "hmdb0001839"
    """
    if not text:
        return ""
    return text.strip().lower()
