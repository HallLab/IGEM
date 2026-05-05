#!/bin/sh
# IGEM container entrypoint.
#
# Validates the snapshot bind-mount, sets the embedded:// URL the
# IGEM client will use, and exec's the user-supplied command.
#
# Bind mounts (expected at runtime):
#   /snapshot   read-only Parquet snapshot directory (REQUIRED)
#   /work       read-write workspace for the user's analysis script
#                                                     and its outputs
#
# Override env vars:
#   IGEM_URL          default embedded:///snapshot — point elsewhere
#                     to talk to a remote HTTP server instead
#   SNAPSHOT_DIR      default /snapshot — change the bind path
#
set -e

SNAPSHOT_DIR="${SNAPSHOT_DIR:-/snapshot}"
IGEM_URL="${IGEM_URL:-embedded://${SNAPSHOT_DIR}}"
export IGEM_URL

# Embedded mode requires a valid snapshot. Fail early with an
# actionable message instead of letting the analysis script crash
# halfway through with a cryptic error.
if echo "${IGEM_URL}" | grep -q '^embedded://'; then
    python - <<PY_EOF
import json, sys
from pathlib import Path

snap = Path("${SNAPSHOT_DIR}")
if not snap.is_dir():
    sys.stderr.write(
        f"ERROR: snapshot dir {snap} not found inside container.\n"
        "Bind-mount your snapshot:\n"
        "  docker run -v /local/snapshot:${SNAPSHOT_DIR}:ro ...\n"
    )
    sys.exit(2)

manifest_path = snap / "manifest.json"
if not manifest_path.exists():
    sys.stderr.write(
        f"ERROR: not a valid snapshot — manifest.json missing in {snap}.\n"
    )
    sys.exit(2)

manifest = json.loads(manifest_path.read_text())
sv = manifest.get("snapshot_version", "unknown")
sc = manifest.get("schema_version", "unknown")
nlp = manifest.get("nlp")
print(f"IGEM container ready", flush=True)
print(f"  snapshot_version : {sv}", flush=True)
print(f"  schema_version   : {sc}", flush=True)
print(f"  tables           : {len(manifest.get('tables', {}))}", flush=True)
print(f"  nlp cache        : {'present' if nlp else 'absent (rebuild on first NLP call)'}", flush=True)
PY_EOF
fi

# Exec user command (defaults to interactive python via CMD in Dockerfile)
exec "$@"
