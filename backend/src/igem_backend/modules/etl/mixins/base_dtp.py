import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from igem_backend.utils.text import normalize_text


@dataclass
class ETLStepStats:
    """
    Structured statistics returned by DTP extract / transform / load steps.

    Stored in ETLPackage.stats (JSON) and mapped to the typed row-count
    columns (extract_rows, transform_rows, load_rows) by the ETL manager.
    """

    # row-level counts (load / transform)
    total: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    warnings: int = 0
    errors: int = 0

    # file-level metadata (extract / transform)
    file_size_bytes: Optional[int] = None
    output_size_bytes: Optional[int] = None
    columns: Optional[int] = None

    # domain-specific extras (arbitrary key/value)
    extras: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d: dict = {}
        for f_name in (
            "total", "created", "updated", "skipped", "warnings", "errors",
            "file_size_bytes", "output_size_bytes", "columns",
        ):
            v = getattr(self, f_name)
            if v is not None and v != 0:
                d[f_name] = v
        if self.extras:
            d.update(self.extras)
        return d

    @property
    def row_count(self) -> int:
        """Primary row count for ETLPackage.*_rows columns."""
        return self.total or self.created


class DTPBase:
    """
    Base class for all IGEM Data Transformation Packages (DTPs).

    Provides:
    - Text normalization and safe truncation helpers
    - HTTP download utilities
    - File path helpers (mirrors BF4 layout: <root>/<SourceSystem>/<dtp_name>/)
    - Entity type resolution cache

    Class attributes to override per DTP:
    - DTP_TYPE: "master" | "relationship" | "mixed"
    - ROLLBACK_STRATEGY: "deactivate" | "delete"
      master      → deactivate (is_active=False); never deletes entities so
                    relationships from other sources stay intact
      relationship → delete; safe because only EntityRelationship rows are owned
      mixed        → deactivate entities/aliases, delete relationships
    """

    TRUNCATE_MODE: bool = True
    MAXLEN_ALIAS: int = 1000
    MAXLEN_SHORT: int = 255

    DTP_TYPE: str = "master"
    ROLLBACK_STRATEGY: str = "deactivate"

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
    def get_entity_type_id(self, name: str) -> int:
        from igem_backend.modules.db.models.model_entities import EntityType

        if not hasattr(self, "_entity_type_cache"):
            self._entity_type_cache: Dict[str, int] = {}

        if name not in self._entity_type_cache:
            entity_type = self.session.query(EntityType).filter_by(name=name).first()
            if not entity_type:
                raise ValueError(f"EntityType '{name}' not found in database.")
            self._entity_type_cache[name] = entity_type.id
        return self._entity_type_cache[name]

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
