# IGEM container — embedded/snapshot variant

Self-contained image bundling **IGEM-Server** (FastAPI + DuckDB) and
**IGEM-Client** (analysis API + scientific Python stack). At runtime
the container reads a Parquet **snapshot** that the user bind-mounts
from outside; the snapshot stays out of the image so:

- The same `igem:0.1.0` image works against **any** compatible snapshot
- Multiple containers / SLURM jobs share the same snapshot directory
  (DuckDB read-only is contention-free)
- The image rebuilds only when **code** changes, snapshots can update
  on their own cadence

## Build

From the repo root:

```bash
docker build -f docker/Dockerfile -t igem:0.1.0 .
```

Expected size: ~1-1.5 GB (Python 3.12 + pandas + scipy + sgkit + …).

## Run — Docker on a laptop

Suppose you have a snapshot at `/Users/me/igem/snapshots/2026-04-25`
and an analysis script at `./analysis.py`:

```bash
docker run --rm \
    -v /Users/me/igem/snapshots/2026-04-25:/snapshot:ro \
    -v "$PWD":/work \
    igem:0.1.0 \
    python analysis.py
```

The entrypoint validates `manifest.json`, sets
`IGEM_URL=embedded:///snapshot`, and exec's your Python. Outputs
written by your script land in `/work` (i.e. on the host).

## Run — Singularity / SLURM (HPC)

Pull the image once into Singularity's `.sif` format:

```bash
singularity pull /shared/igem/igem-0.1.0.sif docker://your-registry/igem:0.1.0
```

A typical SLURM job:

```bash
#!/bin/bash
#SBATCH --job-name=ewas-glucose
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G

module load singularity

SNAP=/scratch/groups/igem/snapshots/2026-04-25
WORK=$SLURM_SUBMIT_DIR

singularity exec \
    --bind $SNAP:/snapshot:ro \
    --bind $WORK:/work \
    --pwd /work \
    /shared/igem/igem-0.1.0.sif \
    python analysis.py
```

Singularity automatically maps the calling user, so files written into
`/work` are owned by the SLURM user (no UID juggling needed).

## Sample `analysis.py`

```python
from igem import IGEM

with IGEM() as igem:                       # IGEM_URL is preset by entrypoint
    phen = igem.data.read_phenotypes(
        "nhanes_subset.csv",
        outcomes=["GLUCOSE"], covariates=["AGE", "SEX"],
    )

    res = igem.analyze.ewas(phen, "GLUCOSE", use_survey=True)
    annotated = (
        res
        .with_correction("fdr_bh")
        .passing(p_corrected=0.05)
        .annotate(igem)
    )
    annotated.to_csv("ewas_results.csv")    # → /work/ewas_results.csv
```

The exact same script runs in **three contexts** without modification:

| context                | how                                      | env                                      |
|------------------------|------------------------------------------|------------------------------------------|
| Laptop, remote server  | `python analysis.py`                     | `IGEM_URL=https://igem-server.org`       |
| Laptop, offline         | `docker run -v /snap:/snapshot:ro …`     | `IGEM_URL=embedded:///snapshot` (default) |
| HPC SLURM              | `singularity exec --bind … python …`    | container injects the env var            |

## Bind mounts

| path        | mode | purpose                                                  |
|-------------|------|----------------------------------------------------------|
| `/snapshot` | `ro` | Parquet snapshot directory (manifest.json + parquet files + optional nlp/) |
| `/work`     | `rw` | User's analysis script + input data + outputs            |

**Both mounts are required.** The entrypoint refuses to start without
a valid snapshot at `/snapshot`.

## Override env vars

| variable        | default                       | when to override                                    |
|-----------------|-------------------------------|------------------------------------------------------|
| `IGEM_URL`      | `embedded:///snapshot`        | Point client at a remote HTTP server instead         |
| `SNAPSHOT_DIR`  | `/snapshot`                   | Use a different bind-mount path                      |

```bash
docker run --rm \
    -e IGEM_URL=https://igem-server.example.com \
    -v "$PWD":/work \
    igem:0.1.0 \
    python analysis.py
```

(Note: the bundled in-process server is wasted weight when overriding
to HTTP — for HTTP-only clients, build `Dockerfile.client` instead
when that variant lands.)

## Building the snapshot

The container does NOT include a snapshot. To produce one from your
PostgreSQL IGEM-Server:

```bash
poetry run igem-server db export \
    --output /local/snapshots/2026-04-25 \
    --version 2026-04-25

# Optional: pre-compile NLP automaton (faster startup)
poetry run igem-server db snapshot-nlp /local/snapshots/2026-04-25
```

Then ship `/local/snapshots/2026-04-25/` to the HPC's shared filesystem
(via `rsync`, `scp`, or `globus`).

## Reproducibility

Cite the pair `(igem:0.1.0, snapshot 2026-04-25)` in your paper.
The manifest.json carries:

- `snapshot_version` — date label
- `schema_version` — DB schema version
- `igem_version_compatible` — server version range
- `tables[*].sha256` — per-table content hash
- `nlp.sha256` — NLP cache content hash (when present)

Anyone with the same image + snapshot reproduces your analysis bit-for-bit.
