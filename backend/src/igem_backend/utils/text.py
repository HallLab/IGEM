from typing import Optional

from igem_backend.modules.nlp.normalizer import normalize as _nlp_normalize


def normalize_text(s: Optional[str]) -> Optional[str]:
    """
    Canonical alias normalization — delegates to the NLP normalizer.

    Used by all DTPs to populate EntityAlias.alias_norm.
    The NLP entity resolver uses the same function on input text,
    guaranteeing that both sides produce identical output.

    Rules (applied in order):
      1. Lowercase
      2. Unicode NFKD decomposition + strip combining chars (café → cafe)
      3. Replace [-_/] with space (IL-6 → il 6, HMDB_0001 → hmdb 0001)
      4. Collapse whitespace

    Note: alias_norm values in the DB built before this change (plain
    lowercase only) will need to be refreshed by re-running affected
    master DTPs with --force load.
    """
    if s is None:
        return None
    return _nlp_normalize(str(s))


def as_list(value) -> list:
    """Coerce a scalar, list, or None into a flat list of non-empty strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if v is not None and str(v).strip()]
    s = str(value).strip()
    return [s] if s else []
