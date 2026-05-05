"""
NLP automaton cache builder/loader for Parquet snapshots.

Builds the AliasDictionary's Aho-Corasick automaton from a snapshot's
parquet files, serializes it to disk inside the snapshot directory.
On subsequent loads (e.g. from inside the embedded:// container in HPC),
the resolver reads the pre-built automaton in ~5s instead of rebuilding
from Parquet (~70s).

Output layout (added inside the snapshot):

    <snapshot_dir>/nlp/
        alias_dictionary.bin    pickled state (automaton + name cache + …)
        metadata.json           build info: alias count, hash, timestamp

The manifest.json gets a new "nlp" key pointing at this directory and
listing the build's metadata.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import pickle
import time
from pathlib import Path
from typing import Optional

import pickle as _pickle  # for clarity in pickle protocol use

from igem_backend.modules.db.database import Database
from igem_backend.modules.nlp.dictionary import AliasDictionary
from igem_backend.utils.logger import Logger

_NLP_SUBDIR = "nlp"
_BIN_FILE = "alias_dictionary.bin"
_META_FILE = "metadata.json"
_PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL  # Python 3.12 → protocol 5


def build_nlp_cache(
    snapshot_dir: Path | str,
    overwrite: bool = False,
    logger: Optional[Logger] = None,
) -> dict:
    """
    Build the AliasDictionary from a Parquet snapshot and serialize it
    into <snapshot_dir>/nlp/. Updates the snapshot's manifest.json.

    Returns the metadata dict written to disk.
    """
    log = logger or Logger(log_level="INFO")
    snap = Path(snapshot_dir).expanduser().resolve()

    if not snap.is_dir():
        raise FileNotFoundError(f"Snapshot directory not found: {snap}")

    manifest_path = snap / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"manifest.json not found in {snap} — not a valid snapshot."
        )

    nlp_dir = snap / _NLP_SUBDIR
    bin_path = nlp_dir / _BIN_FILE
    if bin_path.exists() and not overwrite:
        raise FileExistsError(
            f"NLP cache already exists at {bin_path}. "
            f"Pass overwrite=True to rebuild."
        )

    nlp_dir.mkdir(parents=True, exist_ok=True)

    # 1. Open snapshot via the read-only DB layer (Slice 2.1 in action)
    log.log(f"Building NLP cache for snapshot at {snap}", "INFO")
    log.log("  Opening snapshot (DuckDB+Parquet, read-only)...", "INFO")
    db = Database(db_uri=str(snap))

    # 2. Build the dictionary using the same logic the resolver uses
    log.log("  Building AliasDictionary (~30-60s for full IGEM)...", "INFO")
    t0 = time.perf_counter()
    with db.get_session() as session:
        ad = AliasDictionary(session=session).load()
        build_seconds = time.perf_counter() - t0
    log.log(
        f"    Built: {ad.entry_count:,} aliases, "
        f"{ad.norm_count:,} unique norms in {build_seconds:.1f}s",
        "INFO",
    )

    # 3. Pickle the dictionary state (entries, norm_index, automaton,
    #    name cache, package_version, filter info). Strip the SQLAlchemy
    #    session ref because it's not picklable.
    log.log("  Serializing automaton + state...", "INFO")
    t1 = time.perf_counter()
    state = {
        "version": 1,
        "entries": ad._entries,
        "norm_index": ad._norm_index,
        "automaton": ad._automaton,
        "name_cache": ad._name_cache,
        "package_version": ad._package_version,
        "min_alias_length": ad._min_alias_length,
        "stopwords": ad._stopwords,
        "n_filtered_short": ad._n_filtered_short,
        "n_filtered_stopword": ad._n_filtered_stopword,
        "type_names": ad._type_names,
        "domains": ad._domains,
    }
    payload = pickle.dumps(state, protocol=_PICKLE_PROTOCOL)
    bin_path.write_bytes(payload)
    serialize_seconds = time.perf_counter() - t1
    size_bytes = bin_path.stat().st_size
    sha256 = _file_sha256(bin_path)

    log.log(
        f"    Wrote {bin_path.name} "
        f"({size_bytes:,} bytes) in {serialize_seconds:.1f}s",
        "INFO",
    )

    # 4. Write metadata + update manifest
    metadata = {
        "version": 1,
        "built_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "build_seconds": round(build_seconds + serialize_seconds, 1),
        "file": _BIN_FILE,
        "size_bytes": size_bytes,
        "sha256": sha256,
        "alias_count": ad.entry_count,
        "norm_count": ad.norm_count,
        "name_cache_count": len(ad._name_cache),
        "min_alias_length": ad._min_alias_length,
        "stopwords_count": len(ad._stopwords),
        "filtered_short": ad._n_filtered_short,
        "filtered_stopword": ad._n_filtered_stopword,
        "package_version": ad._package_version,
        "pickle_protocol": _PICKLE_PROTOCOL,
    }
    (nlp_dir / _META_FILE).write_text(json.dumps(metadata, indent=2))

    # Update top-level manifest
    manifest = json.loads(manifest_path.read_text())
    manifest["nlp"] = {
        "directory": _NLP_SUBDIR,
        "file": f"{_NLP_SUBDIR}/{_BIN_FILE}",
        "size_bytes": size_bytes,
        "sha256": sha256,
        "alias_count": ad.entry_count,
        "built_at": metadata["built_at"],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))

    log.log(
        f"NLP cache complete: {ad.entry_count:,} aliases ready for "
        f"instant load",
        "INFO",
    )
    return metadata


def load_nlp_cache(snapshot_dir: Path | str) -> Optional[dict]:
    """
    Load the pre-built AliasDictionary state from a snapshot, if it
    exists. Returns the unpickled state dict or None if no cache.

    The dict can be applied to a fresh AliasDictionary instance to
    skip the from-DB build path.
    """
    snap = Path(snapshot_dir).expanduser().resolve()
    bin_path = snap / _NLP_SUBDIR / _BIN_FILE
    if not bin_path.exists():
        return None
    return pickle.loads(bin_path.read_bytes())


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
