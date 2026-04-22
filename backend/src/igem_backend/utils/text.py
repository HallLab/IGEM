from typing import Optional


def normalize_text(s: Optional[str]) -> Optional[str]:
    """Lowercase and collapse whitespace for stable alias matching."""
    if s is None:
        return None
    return " ".join(str(s).lower().split())


def as_list(value) -> list:
    """Coerce a scalar, list, or None into a flat list of non-empty strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if v is not None and str(v).strip()]
    s = str(value).strip()
    return [s] if s else []
