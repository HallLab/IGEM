# Installation

There are two supported ways to install IGEM, chosen by what you want
to do with it:

| You want to…                                        | Install via | When                                                          |
| --------------------------------------------------- | ----------- | ------------------------------------------------------------- |
| Run analyses against a remote knowledge graph       | **pip**     | Daily analyst workflow, laptop, notebook, CI                  |
| Run a self-contained, reproducible IGEM environment | **image**   | HPC, network-restricted hosts, frozen-version reproducibility |

Both paths give you the same `igem` Python API and CLI. The
difference is only **where the knowledge graph lives** — on a remote
server you point at, or inside a container you run locally.

---

## Option 1 — pip

### Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install igem
```

Confirm the install:

```bash
igem --version
# igem, version 2.1.0
```

### Connect to the GE knowledge graph

The IGEM client is purpose-built for the **analyst side** of the
pipeline: it is intentionally lightweight and does _not_ manage
knowledge data itself. Knowledge curation — ETL pipelines, entity
normalization (NLP), smart queries, and the catalog of ingested data
— lives in the companion **`igem-server`** package, which is
typically deployed once per institution and shared.

```{tip}
Many of IGEM's analytical functions **do not require a server
connection**. Local-only operations include data loading, QC via
`describe` and `modify`, single-feature association tests
(`gwas` / `ewas`), interaction tests (`lrt`), multi-test correction
(Bonferroni / FDR), and visualization. Connecting to a server is
only required for knowledge-graph reports
([gene_annotations], [pathway_annotations], etc.) and for the
filter-then-test workflows that consume them.
```

The Hall Lab maintains a public endpoint that anyone can use to
explore the knowledge graph:

> `https://geneexposure.org/api`

Point your client at it with `igem config`:

```bash
igem config set server-url https://geneexposure.org/api
# set server-url = https://geneexposure.org/api
#   → /your/cwd/.igem.toml
```

By default this writes a project-scoped `./.igem.toml` in the current
directory. To set it user-globally (used from any directory), pass
`--global`:

```bash
igem config set --global server-url https://geneexposure.org/api
#   → ~/.igem.toml
```

The resulting file is plain TOML:

```toml
[client]
server_url = "https://geneexposure.org/api"
```

Manage it through the CLI or by hand:

```bash
igem config show           # print the merged local + home config
igem config get server-url # print just the resolved value
igem config unset server-url
```

Resolution order is **environment variable → local `./.igem.toml`
(walking up from cwd) → home `~/.igem.toml`** — so a one-off
`IGEM_URL=…` env var always wins for a single command.

### Verify

```bash
igem health
# status: ok
```

If the call returns `ok`, the client is wired up correctly. From here
the [Quickstart](quickstart.md) walks through running your first
report.

### Hosting your own server

Standing up an `igem-server` instance — Postgres backend, ETL
pipelines, snapshot generation — is documented separately in
[Operations → Server setup](../operations/server-setup.md). For most
analysts the public endpoint above is sufficient and no local
server is needed.

[gene_annotations]: ../user-guide/reports-catalog.md#gene_annotations
[pathway_annotations]: ../user-guide/reports-catalog.md#pathway_annotations

---

## Option 2 — Docker image

For HPC environments, network-restricted hosts, or any scenario where
you want a frozen, reproducible IGEM stack, the project ships a
single container image on GitHub Container Registry that bundles the
client, the server (running in-process), and the full scientific
Python stack. The container reads the knowledge graph from a
**Parquet snapshot** mounted at `/snapshot`, with no external
database connection involved.

### Pull the image

```bash
docker pull ghcr.io/halllab/igem:latest
```

The image is public — no authentication required. Equivalent
Apptainer pull on HPC nodes:

```bash
apptainer pull igem.sif docker://ghcr.io/halllab/igem:latest
```

:::{note}
The `:latest` tag tracks the most recent stable release and is the
right choice for everyday use. For **scientific reproducibility** —
papers, regulatory submissions, re-analyses — pin a specific tag
instead (for example `ghcr.io/halllab/igem:1.0.0`). All published
tags are listed at
<https://github.com/HallLab/IGEM/pkgs/container/igem>. The container
records the embedded client and server versions as image labels;
inspect them with:

```bash
docker inspect ghcr.io/halllab/igem:latest \
    --format '{{json .Config.Labels}}'
```
:::

### Download a knowledge graph snapshot

The container ships with the `igem-server db snapshot-download`
command, which fetches a versioned Parquet snapshot, verifies every
file against a sha256 manifest, and writes them to a directory you
choose:

```bash
mkdir -p $HOME/igem-snapshot

docker run --rm \
  -v $HOME/igem-snapshot:/work \
  ghcr.io/halllab/igem:latest \
  igem-server db snapshot-download --output /work
```

The default URL is `https://geneexposure.org/downloads/latest/`. To
pin a specific version for reproducibility, pass `--url`:

```bash
docker run --rm -v $HOME/igem-snapshot:/work \
  ghcr.io/halllab/igem:latest \
  igem-server db snapshot-download \
    --url https://geneexposure.org/downloads/2026-04-25/ \
    --output /work
```

Useful flags:

- `--workers N` — concurrent downloads (default 4).
- `--include-nlp` — also fetch the NLP automaton cache (~3.5 GB,
  saves ~70s on first NLP query).
- `--overwrite` — force re-download of every file. Without this
  flag, files whose sha256 already matches the manifest are
  skipped (`cached`), so re-running the command is safe and cheap.

### Run a query

With the snapshot in place, mount it read-only and run any IGEM
command:

```bash
docker run --rm \
  -v $HOME/igem-snapshot:/snapshot:ro \
  -v $(pwd):/work -w /work \
  ghcr.io/halllab/igem:latest \
  igem report run --name gene_annotations \
    --input BRCA1 --input TP53 --input MYC \
    --columns gene_symbol,entrez_id,chromosome,gene_locus_type
```

The container's entrypoint validates the snapshot, starts the server
in-process, and routes the client to it via
`IGEM_URL=embedded:///snapshot` — no manual configuration required.

### HPC and cloud platforms

For LSF / SLURM clusters, Apptainer-based execution, job submission
templates, and integration notes for **Anvil**, **DNAnexus**, and
**All of Us**, see
[Cookbook → Container and HPC workflows](../cookbook/hpc-workflows.md).
The container is the same; only the runtime (Docker vs Apptainer)
and the job scheduler change.

That page also collects ready-to-copy recipes for the three common
execution patterns: interactive shell, scripted runs, and hybrid
setups that combine the container's Python stack with the public
remote knowledge graph.

---

## Troubleshooting

**`igem: command not found`** — the install succeeded but the entry
point is not on `PATH`. Activate the virtual environment, or invoke
with `python -m igem.api.cli.main --version`.

**SSL or proxy errors on `igem health`** — your network blocks
outbound HTTPS to `geneexposure.org`. Either route through your
institution's proxy (`HTTPS_PROXY=…`), or move to the Docker image
above with a downloaded snapshot — once the snapshot is local, no
further network access is required.

**`status: not ok` from `igem health`** — the server is reachable
but reports an internal problem. Re-run with `--debug` to see the
raw response and contact the maintainers if it persists.

**Docker `pull` fails with `manifest unknown`** — the tag does not
exist. List available tags at
<https://github.com/HallLab/IGEM/pkgs/container/igem>.

**Container runs but `manifest.json missing`** — the snapshot bind
mount is wrong. Confirm the host path contains a `manifest.json`
file and that the bind syntax is `--volume HOST:/snapshot:ro` (note
the colon between source and destination, and `:ro` for read-only).
