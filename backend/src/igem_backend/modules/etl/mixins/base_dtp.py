import hashlib
import os
from pathlib import Path
from typing import Dict, Optional

import requests

from igem_backend.utils.text import normalize_text


class DTPBase:
    """
    Base class for all IGEM Data Transformation Packages (DTPs).

    Provides:
    - Text normalization and safe truncation helpers
    - HTTP download utilities
    - File path helpers (mirrors BF4 layout: <root>/<SourceSystem>/<dtp_name>/)
    - Entity group resolution cache
    """

    TRUNCATE_MODE: bool = True
    MAXLEN_ALIAS: int = 1000
    MAXLEN_SHORT: int = 255

    def __init__(self, *args, **kwargs):
        self.trunc_metrics: Dict[str, int] = {}

    # -------------------------------------------------------------------------
    # Text helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _normalize(s: Optional[str]) -> Optional[str]:
        return normalize_text(s)

    def _bump_trunc(self, field: str) -> None:
        self.trunc_metrics[field] = self.trunc_metrics.get(field, 0) + 1

    def safe_truncate(self, val: Optional[str], maxlen: int, field: str) -> Optional[str]:
        if val is None:
            return None
        v = str(val).strip()
        if self.TRUNCATE_MODE and len(v) > maxlen:
            self._bump_trunc(field)
            return v[:maxlen]
        return v

    def guard_alias(self, s: Optional[str]) -> Optional[str]:
        return self.safe_truncate(s, self.MAXLEN_ALIAS, "alias_value")

    def guard_alias_norm(self, s: Optional[str]) -> Optional[str]:
        return self.safe_truncate(self._normalize(s), self.MAXLEN_ALIAS, "alias_norm")

    def guard_short(self, s: Optional[str]) -> Optional[str]:
        return self.safe_truncate(s, self.MAXLEN_SHORT, "short_text")

    def _log_trunc_summary(self) -> None:
        if not self.trunc_metrics:
            return
        parts = [f"{k}={v}" for k, v in sorted(self.trunc_metrics.items())]
        self.logger.log(f"Truncation summary: {', '.join(parts)}", "WARNING")

    # -------------------------------------------------------------------------
    # HTTP helpers
    # -------------------------------------------------------------------------
    def http_download(self, url: str, dest_dir: str) -> tuple[bool, str, Optional[str]]:
        """Download url to dest_dir. Returns (ok, message, file_hash)."""
        filename = os.path.basename(url.split("?")[0])
        os.makedirs(dest_dir, exist_ok=True)
        local_path = Path(dest_dir) / filename

        self.logger.log(f"Downloading {filename} ...", "INFO")
        try:
            resp = requests.get(url, stream=True, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            msg = f"Download failed for {url}: {e}"
            return False, msg, None

        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)

        file_hash = self._hash_file(local_path)
        msg = f"Downloaded {filename} -> {local_path}"
        self.logger.log(msg, "INFO")
        return True, msg, file_hash

    @staticmethod
    def _hash_file(path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    # -------------------------------------------------------------------------
    # File path helpers
    # -------------------------------------------------------------------------
    def _dtp_dir(self, base_dir: str) -> Path:
        """Returns <base_dir>/<SourceSystem>/<dtp_name>/"""
        p = Path(base_dir) / self.data_source.source_system.name / self.data_source.name
        p.mkdir(parents=True, exist_ok=True)
        return p

    # -------------------------------------------------------------------------
    # Entity group cache
    # -------------------------------------------------------------------------
    def get_entity_group_id(self, name: str) -> int:
        from igem_backend.modules.db.models.model_entities import EntityGroup

        if not hasattr(self, "_entity_group_cache"):
            self._entity_group_cache: Dict[str, int] = {}

        if name not in self._entity_group_cache:
            group = self.session.query(EntityGroup).filter_by(name=name).first()
            if not group:
                raise ValueError(f"EntityGroup '{name}' not found in database.")
            self._entity_group_cache[name] = group.id
        return self._entity_group_cache[name]

    # -------------------------------------------------------------------------
    # Build alias list from a schema map
    # -------------------------------------------------------------------------
    def build_aliases(self, row, schema: dict) -> list[dict]:
        """
        Build a list of alias dicts from a pandas row using an alias schema.

        Schema format:
            { "field_name": ("alias_type", "xref_source", is_primary) }
        """
        from igem_backend.utils.text import as_list

        payloads = []
        for field, (atype, xref, is_primary) in schema.items():
            for raw in as_list(row.get(field)):
                payloads.append({
                    "alias_value": raw,
                    "alias_type": atype,
                    "xref_source": xref,
                    "is_primary": is_primary,
                    "alias_norm": self._normalize(raw),
                    "locale": "en",
                })
        return payloads
