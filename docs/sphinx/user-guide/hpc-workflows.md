# Container and HPC workflows

Ready-to-copy recipes for running IGEM in containerized environments
— from a laptop to an HPC cluster to a research-cloud platform. The
**container is the same** in every scenario; only the runtime and
the job scheduler change.

If you have not pulled the image yet, see
[Installation → Option 2 — Docker image](../getting-started/installation.md).

## Choosing a runtime

| Runtime | Where it runs | Why |
|---|---|---|
| **Docker** | Laptop, dev workstation, single-node server | Easy local install, root access available, large ecosystem |
| **Apptainer** *(formerly Singularity)* | HPC nodes (LSF, SLURM, PBS) | Runs as the calling user — no root daemon required, plays nicely with shared filesystems and job schedulers |
| **Singularity** | Older HPC clusters | Predecessor of Apptainer; commands are largely interchangeable |

The IGEM image at `ghcr.io/halllab/igem` is a standard OCI image, so
all three runtimes consume it without rebuilding.

## Prerequisites

Pick the right setup for your scenario and follow it once before
running any of the patterns below.

### A. Container with a local Parquet snapshot

This is the standard reproducible setup — knowledge graph data lives
on disk, no network required at query time.

```bash
# Pull the image (Docker)
docker pull ghcr.io/halllab/igem:latest

# Download a snapshot to ~/igem-snapshot
docker run --rm \
  -v $HOME/igem-snapshot:/work \
  ghcr.io/halllab/igem:latest \
  igem-server db snapshot-download --output /work
```

On HPC, the snapshot is typically pre-staged on a shared filesystem
by the sysadmin — confirm the path before running anything.

### B. Container with the public remote knowledge graph

If you do not want to manage a snapshot at all, the container can
talk to `https://geneexposure.org/api` instead. No `/snapshot` mount
required. See [Pattern 3](#pattern-3-remote) below.

---

## Pattern 1 — Interactive shell

For exploration, prototyping, and ad-hoc queries.

### Docker

```bash
docker run --rm -it \
  -v $HOME/igem-snapshot:/snapshot:ro \
  -v $(pwd):/work -w /work \
  ghcr.io/halllab/igem:latest bash
```

You land inside the container with the snapshot mounted read-only at
`/snapshot` and your current directory mounted read-write at
`/work`. From here:

```bash
# Inside the container
igem health
igem report list
python -c "from igem import IGEM; print(IGEM().health())"
```

### Apptainer (HPC)

```bash
module load apptainer/1.4.1   # adapt to your cluster's module system

apptainer shell \
  --bind $HOME/igem-snapshot:/snapshot:ro \
  --bind $(pwd):/work \
  --pwd /work \
  igem.sif
```

Same usage pattern. Apptainer reuses your shell environment by
default, so things like `$HOME` and your prompt carry over.

---

## Pattern 2 — Run a script

For batch analysis, CI jobs, and reproducible pipelines.

### A minimal `analysis.py`

```python
"""analysis.py — annotate a list of genes and persist the result."""
from igem import IGEM

with IGEM() as igem:
    result = igem.reports.gene_annotations(
        input_values=["TP53", "BRCA1", "APOE", "EGFR"],
        columns=["gene_symbol", "entrez_id", "chromosome",
                 "ensembl_id", "gene_locus_type"],
    )
    result.df.to_csv("annotations.csv", index=False)
    print(f"Wrote {len(result.df)} rows to annotations.csv")
```

### Docker

```bash
docker run --rm \
  -v $HOME/igem-snapshot:/snapshot:ro \
  -v $(pwd):/work -w /work \
  ghcr.io/halllab/igem:latest \
  python analysis.py
```

`annotations.csv` lands in your current directory, since `$(pwd)` is
bind-mounted to `/work` (read-write) and the script's `cwd` is
`/work`.

### Apptainer (HPC)

```bash
apptainer exec \
  --bind $HOME/igem-snapshot:/snapshot:ro \
  --bind $(pwd):/work \
  --pwd /work \
  igem.sif \
  python analysis.py
```

---

(pattern-3-remote)=
## Pattern 3 — Hybrid: remote knowledge graph

For when you want the container's full scientific Python stack
(`sgkit`, `statsmodels`, `pandas`, …) but do not want to manage a
local snapshot. The container connects to the public remote API
instead.

### Docker

```bash
docker run --rm -it \
  -e IGEM_URL=https://geneexposure.org/api \
  -v $(pwd):/work -w /work \
  ghcr.io/halllab/igem:latest bash
```

No `/snapshot` mount is needed. The `IGEM_URL` environment variable
overrides the embedded default and the container starts in remote
mode. From inside:

```bash
igem health        # hits geneexposure.org/api
igem report run --name gene_annotations --input TP53
```

### When to use this pattern

- Your laptop or workstation does not have the disk or bandwidth for
  a snapshot (~tens of GB).
- You want the bundled Python stack but not the responsibility of
  maintaining a frozen knowledge graph.
- You are running quick interactive queries that do not need offline
  reproducibility.

For published analyses, switch back to Pattern 1 or 2 with a pinned
snapshot — see [Reproducibility](#reproducibility) below.

---

## HPC: LSF (`bsub`)

The LSF templates below come from a working deployment on the Hall
Lab cluster. Adapt paths and queue names to your site.

### Job script template

```bash
#!/bin/bash
#BSUB -J igem-analysis
#BSUB -W 02:00
#BSUB -n 4
#BSUB -M 16000
#BSUB -R "rusage[mem=16000]"
#BSUB -o igem_%J.log
#BSUB -e igem_%J.err

module load apptainer/1.4.1

SNAP=/project/hall/datasets/igem/20260505
IMAGE=/project/hall/tools/igem/images/igem.sif
WORK=$LS_SUBCWD

apptainer exec \
  --bind $SNAP:/snapshot:ro \
  --bind $WORK:/work \
  --pwd /work \
  $IMAGE \
  python analysis.py
```

### Submit and monitor

```bash
bsub < igem_job.lsf       # submit (note "<" — bsub reads from stdin)
bjobs                     # list your jobs
bjobs -l <jobid>          # detailed status, including PENDING REASONS
tail -f igem_*.log        # live output
bkill <jobid>             # cancel
```

### Useful directives

| `#BSUB`                    | Meaning                                              |
|----------------------------|------------------------------------------------------|
| `-J <name>`                | Job name                                             |
| `-W HH:MM`                 | Wall-time limit                                      |
| `-n N`                     | Number of cores                                      |
| `-M <MB>`                  | Memory limit per process (MB)                        |
| `-R "rusage[mem=<MB>]"`    | Reserve that memory (required on some clusters)      |
| `-o <file_%J.log>`         | stdout file (`%J` = job ID)                          |
| `-e <file_%J.err>`         | stderr file (separate from stdout)                   |
| `-q <queue>`               | Specific queue                                       |

---

## HPC: SLURM (`sbatch`)

The same Apptainer command, with SLURM-style directives.

### Job script template

```bash
#!/bin/bash
#SBATCH --job-name=igem-analysis
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --output=igem_%j.log
#SBATCH --error=igem_%j.err

module load apptainer/1.4.1

SNAP=/project/hall/datasets/igem/20260505
IMAGE=/project/hall/tools/igem/images/igem.sif
WORK=$SLURM_SUBMIT_DIR

apptainer exec \
  --bind $SNAP:/snapshot:ro \
  --bind $WORK:/work \
  --pwd /work \
  $IMAGE \
  python analysis.py
```

### Submit and monitor

```bash
sbatch igem_job.slurm
squeue -u $USER
scontrol show job <jobid>
scancel <jobid>
```

---

## Parallel jobs and arrays

DuckDB on Parquet is **read-only and contention-free** — you can
launch thousands of jobs in parallel reading the same `/snapshot`
without locking, queueing, or cross-talk. Each job loads its own
in-process server.

### LSF job array

```bash
#BSUB -J "igem-array[1-100]"
```

Inside the script, read the index via `$LSB_JOBINDEX`:

```python
import os
partition = int(os.environ.get("LSB_JOBINDEX", "1"))
print(f"Processing partition {partition}", flush=True)
```

### SLURM job array

```bash
#SBATCH --array=1-100
```

Read the index via `$SLURM_ARRAY_TASK_ID` in the script.

---

## Cloud research platforms

The IGEM image is a standard OCI artefact. The recipe is similar on
every research-cloud platform: reference the image URI, mount or
download a snapshot (or use Pattern 3 with the public remote), and
call IGEM exactly as you would locally. Platform-specific notes:

### Anvil (NHGRI)

Anvil runs WDL workflows on Terra and supports custom Docker images
out of the box. Use `ghcr.io/halllab/igem:latest` as the `docker`
runtime image in your task definition. Snapshots can be staged in a
workspace bucket and bind-mounted via the standard Cromwell
input-localisation flow.

### DNAnexus

DNAnexus applets reference Docker images directly. Pull the image
into the platform's asset store, point the applet at it, and mount
the snapshot from a project file. Standard `docker run`-style
arguments translate one-to-one.

### All of Us Researcher Workbench

The Researcher Workbench supports custom container environments for
Jupyter notebooks and batch jobs. Reference the image URI in your
environment configuration; for snapshots, host them in the workspace
bucket and mount them via the standard volume mechanism.

For all three platforms, **Pattern 3** (remote knowledge graph) is
often the smoothest path — it removes the need to stage tens of GB
of snapshot data inside the platform.

---

## Reproducibility

For papers, regulatory submissions, and re-analyses, pin **both**
the container and the snapshot.

### Pin the container

Use a specific tag in every command:

```bash
ghcr.io/halllab/igem:1.0.0     # not :latest
```

The container records the embedded client and server versions as
image labels — inspect them with:

```bash
docker inspect ghcr.io/halllab/igem:1.0.0 \
  --format '{{json .Config.Labels}}'
```

### Pin the snapshot

Pass `--url` to `igem-server db snapshot-download` so the snapshot
version is recorded explicitly:

```bash
igem-server db snapshot-download \
  --url https://geneexposure.org/downloads/2026-04-25/ \
  --output /scratch/$USER/snapshots/2026-04-25
```

Every snapshot ships with a `manifest.json` containing version,
schema version, and per-file sha256 hashes — log these in your
analysis output:

```python
import json, pathlib
manifest = json.loads(
    pathlib.Path("/snapshot/manifest.json").read_text()
)
print(f"snapshot_version: {manifest['snapshot_version']}")
print(f"schema_version : {manifest['schema_version']}")
print(f"tables         : {len(manifest['tables'])}")
```

### Methods boilerplate

> *Knowledge-graph queries were performed using the IGEM platform
> (image `ghcr.io/halllab/igem:1.0.0`) against snapshot
> `2026-04-25` (schema version 0.1.0, 41 tables). Embedded backend
> mode was used, reading Parquet files via DuckDB; no external
> database connections were involved.*

The container is immutable and the snapshot is versioned and
hash-verified. Anyone with the same container and the same snapshot
reproduces the results bit-for-bit.

---

## Troubleshooting

**`apptainer pull` fails with `auth required`** — the image is
public; an auth error usually means the tag does not exist. Confirm
available tags at
<https://github.com/HallLab/IGEM/pkgs/container/igem>.

**`manifest.json missing` at startup** — the snapshot mount is wrong
or the snapshot is incomplete. Verify the host path contains a
`manifest.json` and that the bind syntax is correct (`SRC:DST:MODE`,
e.g. `/local/path:/snapshot:ro`).

**`python: command not found` inside the container** — bind mount
syntax error. Make sure `--bind` / `--volume` arguments use a colon
between source and destination, with no space.

**Job stuck in `PEND` (LSF) or `PD` (SLURM)** — the cluster cannot
satisfy the resource request. Inspect with `bjobs -l <jobid>` or
`scontrol show job <jobid>` and reduce `-n`, `-M`, or `--mem`.

**Output from `print()` does not appear in the log** — Python is
buffering stdout. Use `print(..., flush=True)`, run with `python -u`,
or set `PYTHONUNBUFFERED=1` (the container already does this; it can
get overridden in some scheduler environments).

**Results disappear after the job finishes** — you forgot to bind
your working directory. Anything written to a path that is *not*
inside a `--bind` / `--volume` mount lives only inside the
container's ephemeral filesystem and is lost when the container
exits. Always bind a writable host directory and use it as
`--pwd` / `-w`.

---

## See also

- [Installation](../getting-started/installation.md) — pulling the
  image and downloading snapshots.
- [Quickstart](../getting-started/quickstart.md) — first
  knowledge-graph query.
- [Snapshot generation](../operations/snapshot-generation.md) —
  building your own snapshots from a live IGEM server (Operations).
- [Container deployment](../operations/deployment.md) —
  sysadmin-level deployment options (Operations).
