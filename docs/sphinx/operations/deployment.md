# Container deployment (Docker / Apptainer)

Operational guide for deploying IGEM as a container — image
distribution, snapshot lifecycle, production configuration, and the
day-to-day commands a sysadmin or DevOps engineer needs to keep an
analysis environment running. The container bundles the **client,
the server (running in-process), and the full scientific Python
stack**; together with a versioned Parquet snapshot it forms a
self-contained, reproducible IGEM environment.

```{tip}
This page is sysadmin-focused: image management, snapshot
lifecycle, production checklist. For analyst-side recipes (interactive
shells, scripted runs, hybrid setups, job submission templates,
cloud platforms), see
[Cookbook → Container and HPC workflows](../cookbook/hpc-workflows.md).
The two pages are complementary — Operations covers *how to deploy*;
Cookbook covers *how to use what is deployed*.
```

---

## What ships in the image

The image is published to GitHub Container Registry as
`ghcr.io/halllab/igem`. A single OCI artefact, public, no
authentication required. It contains:

| Layer | Contents |
|---|---|
| Client | `igem` Python package + CLI (`igem`, `igem-server`) |
| Server | `igem-server` Python package, runs in-process via ASGI |
| Stack | pandas, numpy, scipy, statsmodels, sgkit, DuckDB, FastAPI |
| Entrypoint | `docker/entrypoint.sh` — validates the snapshot mount and routes the client to it via `IGEM_URL=embedded:///snapshot` |

Embedded versions are recorded as image labels for auditing — useful
for checking what is inside any image you have already pulled:

```bash
docker inspect ghcr.io/halllab/igem:latest \
  --format '{{json .Config.Labels}}'
# {"igem.client_version": "2.1.0",
#  "igem.server_version": "0.1.0",
#  "org.opencontainers.image.version": "1.0.0",
#  ...}
```

`:latest` is fine for a one-off inspection like this. Production
deployments should still pin a specific version — see
[Pinning a version](#pinning-a-version) below.

---

## Image distribution

### Pulling from GHCR

```bash
docker pull ghcr.io/halllab/igem:1.0.0
```

Equivalent on HPC nodes (no root daemon required):

```bash
apptainer pull igem-1.0.0.sif docker://ghcr.io/halllab/igem:1.0.0
```

### Pinning a version

**Never deploy `:latest` in production.** Pin a specific version tag
in every command, every job script, and every Cookbook recipe:

```bash
ghcr.io/halllab/igem:1.0.0    # ✓ pinned, reproducible
ghcr.io/halllab/igem:latest   # ✗ floats; analyses become non-reproducible
```

Available tags are listed at
<https://github.com/HallLab/IGEM/pkgs/container/igem>. The container
follows its own semver, decoupled from `igem` and `igem-server`
PyPI versions — bumps when the image itself changes (base image,
system deps, entrypoint), not necessarily when the embedded packages
change. The bundled package versions are recorded as image labels
(see above).

### Mirroring for air-gapped sites

For institutions with restricted egress, `docker save` / `docker
load` (or `apptainer pull` from an internal registry) handles
distribution. Snapshots can be hosted on an internal HTTPS endpoint
and passed to `--url` (see below).

---

## Snapshot lifecycle

The image contains the *engine*, never the *data*. The IGEM knowledge
graph ships separately as a versioned **Parquet snapshot** that the
container reads at runtime via a `:ro` mount at `/snapshot`.

### Where snapshots live

The Hall Lab publishes snapshots at:

| URL | Use |
|---|---|
| `https://geneexposure.org/downloads/latest/` | Floating pointer to the most recent stable snapshot |
| `https://geneexposure.org/downloads/<version>/` | Pinned, immutable version (e.g. `2026-04-25`) |

Each version directory contains the parquet files, an optional NLP
automaton cache, and a `manifest.json` listing every file with its
sha256 hash, schema version, and snapshot version.

### Download via the CLI (recommended)

The container ships with `igem-server db snapshot-download`. It
fetches every file declared in the manifest, verifies sha256, and
writes them to the directory you choose:

```bash
mkdir -p /srv/igem/snapshots/2026-04-25

docker run --rm \
  -v /srv/igem/snapshots/2026-04-25:/work \
  ghcr.io/halllab/igem:1.0.0 \
  igem-server db snapshot-download \
    --url https://geneexposure.org/downloads/2026-04-25/ \
    --output /work \
    --workers 4
```

| Flag | Default | When to use |
|---|---|---|
| `--url <URL>` | `…/downloads/latest/` | Pin a specific version for reproducibility |
| `--output <DIR>` | required | Where to write the parquet files |
| `--workers N` | 4 | Concurrent downloads — raise on fast links |
| `--include-nlp` | off | Include the NLP automaton cache (~3.5 GB extra; saves ~70 s on first NLP query) |
| `--overwrite` | off | Force re-download of every file (rare; only if local corruption is suspected) |

Re-running the same command against an existing output is **safe and
cheap**: files whose sha256 already matches the manifest are reported
as `cached` and skipped; only new or changed files come down the
network. A failed download leaves a `.part` file that is removed on
next run, so partial-corruption is not possible.

### Download via the web

The same URLs are browsable. Navigate to
`https://geneexposure.org/downloads/<version>/` in a browser to see
the directory listing, download the `manifest.json` first to confirm
the file list, then fetch each parquet (and the optional `nlp/`
directory). Use this path when:

- The host has no Docker / Apptainer runtime available for the bootstrap.
- You need to inspect the manifest before committing to download.
- Your network policy blocks the CLI's connection but allows browser HTTPS.

After a manual download, **verify integrity** by checking sha256 hashes
against `manifest.json` — the CLI does this automatically, manual
downloads do not.

### Storage layout

Once downloaded, the directory should look like:

```
/srv/igem/snapshots/2026-04-25/
├── manifest.json
├── entity_aliases.parquet
├── chemical_groups.parquet
├── ... (~40 parquet files, total ~tens of GB)
└── nlp/                          # optional, only if --include-nlp
    └── automaton.bin
```

This directory is what gets bind-mounted at `/snapshot:ro` inside
the container. On HPC, place it on a shared filesystem visible to
all compute nodes (`/project/...`, `/scratch/...`); for single-host
deployments, any local path works.

### Updating and rollback

Snapshots are **immutable** once published — a new analysis revision
ships as a new version directory at a new URL. To update: download
the new version into a new directory, validate it (e.g. run a
smoke-test script against it), then update job scripts to point at
the new path. Old versions remain on disk for as long as you need
them — you can keep multiple versions side by side and let users
pick the one they need.

To roll back: just point the bind mount at the previous version's
directory. Nothing inside the snapshot needs to change.

---

## Running the container in production

### Single-host (Docker)

```bash
docker run --rm \
  -v /srv/igem/snapshots/2026-04-25:/snapshot:ro \
  -v /srv/igem/work/$USER:/work -w /work \
  --memory=16g --cpus=4 \
  ghcr.io/halllab/igem:1.0.0 \
  python analysis.py
```

Two bind mounts cover every scenario:

| Path inside | Mode | Purpose |
|---|---|---|
| `/snapshot` | `:ro` | The Parquet snapshot the server reads |
| `/work` | `:rw` | The analyst's script + inputs + outputs |

### HPC (Apptainer, multi-user)

The same image as a `.sif`, with module-loaded Apptainer:

```bash
module load apptainer/1.4.1

apptainer exec \
  --bind /project/igem/snapshots/2026-04-25:/snapshot:ro \
  --bind /scratch/$USER/work:/work \
  --pwd /work \
  /opt/images/igem-1.0.0.sif \
  python analysis.py
```

For LSF / SLURM job templates, job arrays, and parallel patterns,
see [Cookbook → Container and HPC workflows](../cookbook/hpc-workflows.md).

### Configuration via environment variables

| Variable | Default | Purpose |
|---|---|---|
| `IGEM_URL` | `embedded:///snapshot` | Transport URL. Override to point at a remote `igem-server` (`http://server:8000`) instead of the embedded backend |
| `SNAPSHOT_DIR` | `/snapshot` | Path inside the container where the snapshot is mounted |
| `PYTHONUNBUFFERED` | `1` | Ensures `print()` output appears in real time in job logs |

The defaults are the right answer in 95% of deployments. Override
`IGEM_URL` only when the container should act as a *client to a
remote server* rather than as a self-contained backend.

### Resource sizing

The embedded backend is light — DuckDB over Parquet has small steady
memory use, scaling primarily with the size of the analysis result.
Typical guidance:

| Workload | Memory | Cores |
|---|---|---|
| Single-gene knowledge-graph queries | 2–4 GB | 1–2 |
| Large knowledge-graph reports (10k+ inputs) | 8–16 GB | 2–4 |
| GWAS / EWAS scan on biobank genotypes | 16–64 GB | 4–8 |

Snapshots themselves occupy disk, not memory. Plan for
**~tens of GB per snapshot version** (varies with what is
included — `--include-nlp` adds ~3.5 GB).

---

## End-to-end example

A complete, copy-pasteable example: a simple Python script,
delivered to the container, run end-to-end against a downloaded
snapshot.

### A simple analysis script

`analysis.py`:

```python
"""analysis.py — annotate a list of genes from the IGEM knowledge graph."""
from igem import IGEM

with IGEM() as igem:
    result = igem.reports.gene_annotations(
        input_values=["TP53", "BRCA1", "MYC", "APOE", "EGFR"],
        columns=["gene_symbol", "entrez_id", "chromosome",
                 "ensembl_id", "gene_locus_type"],
    )
    result.df.to_csv("annotations.csv", index=False)
    print(f"Wrote {len(result.df)} rows to annotations.csv")
```

Save it on the host. The container will see it at `/work/analysis.py`
because we bind-mount the analyst's working directory.

### Run it with Docker

```bash
docker run --rm \
  -v /srv/igem/snapshots/2026-04-25:/snapshot:ro \
  -v $(pwd):/work -w /work \
  ghcr.io/halllab/igem:1.0.0 \
  python analysis.py
```

Expected output:

```text
[INFO] IGEM container ready
  snapshot_version : 2026-04-25
  schema_version   : 0.1.0
  tables           : 41
[INFO] Snapshot connection established (read-only)
Wrote 5 rows to annotations.csv
```

`annotations.csv` lands in the current directory on the host, since
`$(pwd)` is bind-mounted to `/work` (read-write) and the script's
`cwd` is `/work`.

### Run it with Apptainer

```bash
apptainer exec \
  --bind /project/igem/snapshots/2026-04-25:/snapshot:ro \
  --bind $(pwd):/work \
  --pwd /work \
  /opt/images/igem-1.0.0.sif \
  python analysis.py
```

Same script, same output, same `annotations.csv` — only the runtime
changes.

---

## Production checklist

Before exposing the deployment to users:

- [ ] **Image tag pinned** — no `:latest` anywhere in production
      scripts.
- [ ] **Snapshot version pinned** — passed via `--url …/<version>/`
      to `snapshot-download`, not the floating `latest/`.
- [ ] **Snapshot integrity verified** — sha256 hashes checked
      against `manifest.json` (the CLI does this automatically; only
      manual downloads need explicit verification).
- [ ] **Snapshot mounted read-only** — `:ro` (Docker) or `:ro`
      suffix on `--bind` (Apptainer).
- [ ] **Working directory mounted writable** — analysts need a
      `:rw` mount they can write outputs to; without it, results
      vanish when the container exits.
- [ ] **Resource limits set** — `--memory` and `--cpus` (Docker) or
      scheduler directives (LSF / SLURM) appropriate to the workload.
- [ ] **Disk space planned** — multi-version snapshot retention
      adds tens of GB per version; budget ahead.
- [ ] **Update procedure documented** — when, how, and who validates
      a new snapshot before flipping job scripts to it.
- [ ] **Provenance recorded in outputs** — analyses log the image
      tag and snapshot version (the entrypoint prints both at startup
      automatically).

---

## See also

- [Cookbook → Container and HPC workflows](../cookbook/hpc-workflows.md)
  — analyst-facing recipes: interactive shell, scripted runs,
  hybrid remote KG, LSF / SLURM templates, job arrays, cloud
  platforms.
- [Operations → Snapshot generation](snapshot-generation.md) — for
  sites generating their own snapshots from a live `igem-server`.
- [Operations → Server setup](server-setup.md) — full server
  deployment with PostgreSQL, when the embedded mode is not enough.
- [Installation → Option 2 — Docker image](../getting-started/installation.md)
  — analyst-facing first-time install, complementary to this
  ops-focused page.
