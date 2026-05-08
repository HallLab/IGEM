"""
Download a Parquet snapshot from a remote HTTP endpoint.

Workflow
--------
1. GET <url>/manifest.json — discover all files + their sha256 hashes
2. Download each table in parallel; verify sha256 against the manifest
3. If --include-nlp, also fetch the pre-compiled NLP automaton cache
4. Write a local copy of manifest.json

Once complete, the local directory is a fully usable snapshot — point
IGEM-Server / Apptainer / Docker at it via `--db-uri <dir>` or by
bind-mounting it as `/snapshot` inside the container.

Integrity is enforced — every file's sha256 must match the value the
manifest declares (the same hashes the `db export` step computes), or
the download aborts and the partial file is removed.

The default URL points at the public snapshot host:

    https://geneexposure.org/downloads/latest/

Override via --url for staging mirrors, archived versions, or local
testing.
"""

from __future__ import annotations

import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests

from igem_backend.utils.logger import Logger


DEFAULT_SNAPSHOT_URL = "https://geneexposure.org/downloads/latest/"
DEFAULT_WORKERS = 4
DEFAULT_TIMEOUT_S = 1800


class SnapshotIntegrityError(RuntimeError):
    """Downloaded file's sha256 does not match the manifest."""


def download_snapshot(
    url: str = DEFAULT_SNAPSHOT_URL,
    output_dir: Path | str = ".",
    include_nlp: bool = False,
    workers: int = DEFAULT_WORKERS,
    overwrite: bool = False,
    logger: Optional[Logger] = None,
) -> dict:
    """
    Download a snapshot to `output_dir` and return its manifest dict.

    Parameters
    ----------
    url:
        Base URL of the snapshot directory. Must end with `/` (added
        if missing). Must serve a `manifest.json` at the root.
    output_dir:
        Local directory to write the snapshot to. Created if missing.
    include_nlp:
        If True (and the manifest declares an `nlp` section), also
        download the NLP automaton cache (~3.5 GB).
    workers:
        Concurrent downloads. The bottleneck is usually the server's
        bandwidth — bumping this past 4–8 rarely helps and can stress
        the host.
    overwrite:
        If False (default), files that already exist locally and pass
        sha256 verification are skipped. If True, every file is
        re-downloaded.
    logger:
        Optional IGEM logger. If None, an INFO-level logger is created.

    Returns
    -------
    The manifest dict written into the output directory.
    """
    log = logger or Logger(log_level="INFO")
    out = Path(output_dir).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)

    if not url.endswith("/"):
        url = url + "/"

    log.log(f"Snapshot download from {url}", "INFO")
    log.log(f"  → {out}", "INFO")

    # --- 1. Fetch manifest ---
    manifest_url = urljoin(url, "manifest.json")
    log.log(f"  Fetching {manifest_url}", "INFO")
    try:
        resp = requests.get(manifest_url, timeout=30)
        resp.raise_for_status()
        manifest = resp.json()
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Failed to fetch manifest at {manifest_url}: {exc}"
        ) from exc
    except ValueError as exc:
        raise RuntimeError(
            f"manifest.json at {manifest_url} is not valid JSON: {exc}"
        ) from exc

    log.log(
        f"  snapshot_version={manifest.get('snapshot_version')} "
        f"schema_version={manifest.get('schema_version')}",
        "INFO",
    )

    # --- 2. Build download list (tables + optional NLP) ---
    download_jobs: list[tuple[str, str, int]] = []
    for table_name, meta in manifest.get("tables", {}).items():
        rel = meta.get("file", f"{table_name}.parquet")
        download_jobs.append((
            rel,
            meta.get("sha256", ""),
            int(meta.get("size_bytes", 0)),
        ))

    if include_nlp:
        nlp_meta = manifest.get("nlp")
        if nlp_meta:
            download_jobs.append((
                nlp_meta["file"],
                nlp_meta.get("sha256", ""),
                int(nlp_meta.get("size_bytes", 0)),
            ))
        else:
            log.log(
                "  (--include-nlp set but manifest has no nlp section; "
                "skipping)",
                "WARNING",
            )

    if not download_jobs:
        raise RuntimeError(
            "manifest declares no files to download — corrupt snapshot?"
        )

    total_bytes = sum(size for _, _, size in download_jobs)
    log.log(
        f"  {len(download_jobs)} files / {_human_size(total_bytes)} "
        f"total ({workers} concurrent workers)",
        "INFO",
    )

    # --- 3. Concurrent download ---
    print(
        f"\n  {'FILE':<40} {'SIZE':>12} {'STATUS':>12}",
        flush=True,
    )
    print(
        f"  {'-' * 40} {'-' * 12} {'-' * 12}",
        flush=True,
    )

    started_at = time.perf_counter()
    bytes_fetched = 0
    files_cached = 0
    errors: list[str] = []

    def _job(rel: str, expected_sha: str, expected_size: int):
        target = out / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and not overwrite:
            actual = _file_sha256(target)
            if actual == expected_sha:
                return rel, expected_size, "cached"
        _download_file(
            urljoin(url, rel),
            target,
            expected_sha,
            timeout=DEFAULT_TIMEOUT_S,
        )
        return rel, expected_size, "downloaded"

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(_job, rel, sha, size): rel
            for rel, sha, size in download_jobs
        }
        for fut in as_completed(futures):
            rel = futures[fut]
            try:
                rel_done, size, status = fut.result()
            except Exception as exc:
                errors.append(f"{rel}: {exc}")
                print(
                    f"  {rel:<40} {'?':>12} {'FAILED':>12}",
                    flush=True,
                )
                continue
            if status == "cached":
                files_cached += 1
            else:
                bytes_fetched += size
            print(
                f"  {rel_done:<40} {_human_size(size):>12} {status:>12}",
                flush=True,
            )

    duration_s = time.perf_counter() - started_at

    if errors:
        raise RuntimeError(
            "Snapshot download had failures:\n  - "
            + "\n  - ".join(errors)
        )

    # --- 4. Write manifest locally (last, so partial state never wins) ---
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))

    print(
        f"  {'-' * 40} {'-' * 12} {'-' * 12}",
        flush=True,
    )
    print(
        f"  {'TOTAL':<40} {_human_size(total_bytes):>12} "
        f"{f'{files_cached} cached':>12}",
        flush=True,
    )
    print(
        f"\nFetched {_human_size(bytes_fetched)} "
        f"({len(download_jobs) - files_cached} new files) in "
        f"{duration_s:.0f}s",
        flush=True,
    )
    log.log(f"Snapshot ready at {out}", "INFO")
    return manifest


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _download_file(
    url: str, dest: Path, expected_sha256: str, timeout: int
) -> None:
    """Stream-download `url` to `dest` and verify sha256.

    Writes to a `.part` sidecar file first, only renames on successful
    hash match. A partial file is removed on any failure.
    """
    h = hashlib.sha256()
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with requests.get(url, stream=True, timeout=timeout) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if chunk:
                        fh.write(chunk)
                        h.update(chunk)
        actual = h.hexdigest()
        if expected_sha256 and actual != expected_sha256:
            tmp.unlink(missing_ok=True)
            raise SnapshotIntegrityError(
                f"Hash mismatch for {url}: "
                f"expected {expected_sha256[:16]}..., got {actual[:16]}..."
            )
        tmp.replace(dest)
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _human_size(n: int) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"
