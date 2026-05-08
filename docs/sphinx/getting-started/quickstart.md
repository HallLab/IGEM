# Quickstart

A five-minute tour from zero to your first knowledge-graph query.
By the end you will have installed IGEM, connected to the public
server, retrieved real biological annotations for a set of genes,
and seen how the same workflow is available both from the command
line and from Python.

```{note}
**Requirement:** Python 3.11 or newer. Everything else is one
`pip install` away.
```

## 1. Install and connect

Install the client from PyPI (a virtual environment is recommended):

```bash
python -m venv .venv
source .venv/bin/activate
pip install igem
```

Point it at the public IGEM server maintained by the Hall Lab:

```bash
igem config set server-url https://geneexposure.org/api
# set server-url = https://geneexposure.org/api
#   → /your/cwd/.igem.toml
```

Verify the connection:

```bash
igem health
# status: ok
```

If you see `status: ok`, you are ready to query the knowledge graph.

```{tip}
The command above writes a project-scoped `./.igem.toml`. To set the
URL once for any directory, use `igem config set --global server-url …`
which writes `~/.igem.toml` instead. See [Installation](installation.md)
for the full configuration reference, troubleshooting, and the
optional `[embedded]` extras for HPC use.
```

## 2. Discover the knowledge graph

Ask the server which reports it exposes:

```bash
igem report list
```

```text
Name                   Version   Description
--------------------------------------------------------
gene_annotations       1.0       Gene metadata for HGNC symbols
go_annotations         1.0       Gene Ontology terms by gene
disease_annotations    1.0       Disease associations by gene
pathway_annotations    1.0       Reactome pathways by gene
protein_annotations    1.0       UniProt proteins by gene
```

Read the documentation for any of them:

```bash
igem report explain --name gene_annotations
```

## 3. Run your first report

Annotate three well-known cancer genes — *BRCA1*, *TP53*, *MYC* —
and trim the output to four columns so it fits your terminal:

```bash
igem report run --name gene_annotations \
  --input BRCA1 --input TP53 --input MYC \
  --columns gene_symbol,entrez_id,chromosome,gene_locus_type
```

```text
[INFO] ============================================
[INFO] IGEM — Client
[INFO]   Server URL : https://geneexposure.org/api
[INFO] ============================================
[INFO] [report] Running 'gene_annotations'...
[INFO] [report] 'gene_annotations' complete: 3 rows in 0.3s

gene_symbol  entrez_id  chromosome  gene_locus_type
BRCA1        672        17          gene with protein product
TP53         7157       17          gene with protein product
MYC          4609       8           gene with protein product
```

The server resolved each input symbol to its HGNC entity, joined the
genomic-coordinate metadata, and returned a typed result in under a
second.

### Save to a file

For larger inputs, redirect to CSV:

```bash
igem report run --name gene_annotations \
  --input-file my_genes.txt \
  --output annotations.csv
```

`my_genes.txt` is a plain text file, one identifier per line.

## 4. The same flow in Python

The CLI is a thin wrapper around the `IGEM` Python facade. Anything
the CLI does is one method call away:

```python
from igem import IGEM

with IGEM() as igem:
    result = igem.reports.gene_annotations(
        input_values=["BRCA1", "TP53", "MYC"],
        columns=["gene_symbol", "entrez_id", "chromosome",
                 "gene_locus_type"],
    )

print(result.df.head())
```

`result.df` is a `pandas.DataFrame` — chain it directly into
downstream analysis:

```python
result.df.to_parquet("annotations.parquet")
result.df.query("chromosome == '17'")
```

The `IGEM()` constructor reads the same `./.igem.toml` (or
`~/.igem.toml`) that the CLI uses, so no further configuration is
needed inside scripts or notebooks.

## What's next

You have just used IGEM as a pure knowledge-graph client. The full
surface is split between the **client** (analyst-facing, the
`igem` package) and the **server** (sysadmin-facing, the
`igem-server` package). A quick map of capabilities and where each
is documented:

### IGEM client — analytical surface

- **Data loading** — `igem.data.read_plink`, `read_vcf`,
  `read_phenotypes`. PLINK 1.x BED/BIM/FAM, VCF, and phenotype CSV /
  TSV. → [Loading data](../user-guide/loading-data.md)
- **Quality control** — `igem.describe.*` for summaries, missingness
  reports, type inference, and skewness; `igem.modify.*` for
  recoding, outlier removal, categorisation, and column / row
  filters. → [Phenotype analysis](../user-guide/phenotype-analysis.md)
- **Single-feature analysis** — `igem.analyze.gwas` and
  `igem.analyze.ewas` for genome- and environment-wide association
  scans, with built-in Bonferroni and FDR corrections. →
  [GWAS / EWAS](../user-guide/gwas-ewas.md)
- **Interaction analysis** — `igem.analyze.lrt` runs likelihood-ratio
  tests between nested and full models, the canonical mechanism for
  GxG, GxE, and ExE interaction terms. →
  [GWAS / EWAS](../user-guide/gwas-ewas.md)
- **Knowledge-graph reports** — `igem.reports.*`: gene, GO, disease,
  pathway, and protein annotations queried from the server (what
  this Quickstart used). → [KG queries](../user-guide/kg-queries.md)
- **HPC orchestration** — LSF / SLURM submission helpers and offline
  *embedded* mode against a Parquet snapshot. →
  [HPC workflows](../user-guide/hpc-workflows.md)
- **Visualisation** *(roadmap)* — Manhattan plots (with Bonferroni
  and FDR thresholds), top-results plots, and exposome distribution
  plots, ported from the CLARITE lineage.

### IGEM server — backend surface

- **Database** — `ge.db`: PostgreSQL schema, Alembic migrations, and
  connection management for the knowledge graph. →
  [Server setup](../operations/server-setup.md)
- **ETL** — `ge.etl`: ingestion pipelines for HGNC, UniProt,
  Reactome, GO, CTD, HMDB, PharmGKB, NHANES, and the other
  authoritative sources behind the graph. →
  [ETL pipeline](../operations/etl-pipeline.md)
- **NLP** — `ge.nlp`: entity normalisation and synonym / alias
  resolution across the supported ontologies, used during ingestion
  and at query time.
- **Reports** — `ge.report`: server-side report registry that
  surfaces typed analytical endpoints (`gene_annotations`,
  `pathway_annotations`, …) to any client.
- **Snapshot generation** — frozen Parquet exports of the graph,
  versioned by tag and used by clients in offline / embedded mode. →
  [Snapshot generation](../operations/snapshot-generation.md)
- **Container deployment** — Docker / Apptainer image bundling the
  client and the server for single-host or HPC deployment. →
  [Container deployment](../operations/deployment.md)

If you would prefer to understand the moving parts before diving in,
the next page — [Concepts](concepts.md) — explains the client /
server / snapshot model and the filter-then-test loop at the centre
of an IGEM study.
