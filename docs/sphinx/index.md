# IGEM

**Biologically-informed interaction filtering for GWAS and EWAS.**

IGEM is an open-source platform that couples a curated biomedical knowledge
graph with a Python analysis client to constrain the search space of
gene–gene (**GxG**) and gene–environment (**GxE**) interaction hypotheses
*before* multiple-testing correction is applied. Statistical power is
focused on candidate pairs with biological grounding instead of being
diluted across combinatorial noise.

Cohort genotypes and phenotypes never leave the analyst's machine — only
entity identifiers are exchanged with the IGEM server.

---

## The challenge

Pairwise interaction screens collapse under their own combinatorics.
A published GxE study of type 2 diabetes in the eMERGE Network (Hall
et al., *Nature Communications* 2017) considered 1 million SNPs and
≈ 300 exposures: an unfiltered scan would have evaluated **3 × 10⁸
hypotheses**, with a Bonferroni-corrected significance threshold of
**1.67 × 10⁻¹⁰**. At that scale, most true effects are statistically
invisible even when the underlying biology is well established.

Filtering *post-hoc* by *p*-value does not recover power. Filtering
*a priori* by **prior biological evidence** does — an approach
established for gene–gene interactions via Biofilter (Bush et al.
2009; Pendergrass et al. 2013) and extended by IGEM to GxE and ExE.

---

## How IGEM addresses it

::::{grid} 1 2 2 3
:gutter: 3
:margin: 4 4 0 0

:::{grid-item-card} Curated knowledge graph
Three domains — *Genomics*, *Exposome*, *Knowledge* — and 13 entity
types unify gene, variant, protein, pathway, GO term, disease,
chemical, phenotype, metabolite, and exposure data from HGNC,
UniProt, Reactome, GO, CTD, HMDB, PharmGKB, NHANES, and other
authoritative sources.
:::

:::{grid-item-card} Pre-correction filtering
Interaction hypotheses are restricted to pairs supported by known
relationships in the graph (GxG, GxE, ExE, GxK, ExK), reducing the
multiple-testing burden by orders of magnitude before the first model
is fit.
:::

:::{grid-item-card} Local-first analysis
GWAS, EWAS, and phenotype workflows run entirely on the analyst's
machine or HPC node. Only entity identifiers are exchanged with the
server — raw genotypes and clinical data stay where they are.
:::

::::

---

## Architecture

Three independently versioned artefacts, designed to be deployed
together or separately.

::::{grid} 1 3 3 3
:gutter: 3
:margin: 4 4 0 0

:::{grid-item-card} `igem` — analysis client
Python package distributed on PyPI. Runs on a laptop or HPC node.
Provides data loading (PLINK, VCF, phenotype CSV), descriptive
statistics, GWAS/EWAS pipelines, and a transport-agnostic facade for
knowledge-graph queries.
:::

:::{grid-item-card} `igem-server` — knowledge backend
FastAPI service backed by PostgreSQL. Hosts the curated graph, the
ETL pipelines that maintain it, an NLP module for entity
normalisation, and the report endpoints consumed by the client.
:::

:::{grid-item-card} Snapshot — offline mode
Versioned Parquet export of the graph. Loaded by the client in
*embedded* mode for HPC environments without network egress, ensuring
full reproducibility of any analysis tied to a snapshot identifier.
:::

::::

A reference deployment ships as a Docker / Apptainer image on GHCR.
See [Concepts](getting-started/concepts.md) for the deployment
trade-offs.

End-to-end, the three components sit inside a wider pipeline that
connects analyst-side data to a curated knowledge graph backed by
public sources, accessible from the CLI, Python, or in fully offline
mode:

:::{thumbnail} _static/overview.jpg
:alt: End-to-end IGEM pipeline — data inputs, client analysis, secure API, knowledge graph, public sources, and deployment modes
:align: center
:::

---

## What the client gives you

The `igem` Python package is organised as **six capability modules**
that compose into the end-to-end study workflow. Each module has its
own reference page in the [User Guide — IGEM](user-guide/index.md);
end-to-end recipes that combine them live in the
[Cookbook](cookbook/index.md).

:::{thumbnail} _static/stack_igem_client.jpg
:alt: IGEM Client — six capability modules (Load, Describe, Modify, Analyze, Plot, Report) with example functions, end-to-end workflow, and three execution modes
:align: center
:::

::::{grid} 1 2 3 3
:gutter: 3
:margin: 4 4 0 0

:::{grid-item-card} 1 · `igem.data` — Loading
:link: user-guide/loading-data
:link-type: doc

Read PLINK / VCF / Zarr genotypes (lazy), phenotype CSV / TSV / XPT
with role metadata, generic tables, and GWAS sumstats from PLINK 2,
REGENIE, BOLT-LMM, or GWAS Catalog into a canonical schema.
:::

:::{grid-item-card} 2 · `igem.describe` — Describing
:link: user-guide/describing-data
:link-type: doc

Summary statistics, missingness, type inference, skewness,
correlations, contingency tables, group-stratified summaries, and
genotype QC metrics (call rate, MAF, HWE, het rate).
:::

:::{grid-item-card} 3 · `igem.modify` — Modifying
:link: user-guide/modifying-data
:link-type: doc

Type harmonisation, value transformations (log, RINT, Box-Cox,
z-score), outlier removal, structural filters, multi-frame assembly,
and the standard genotype QC pipeline including LD pruning.
:::

:::{grid-item-card} 4 · `igem.analyze` — Analysing
:link: user-guide/analyzing-data
:link-type: doc

Association studies (GWAS, EWAS, PheWAS), GxE / GxG interaction
tests via likelihood-ratio (LRT), survey-aware modelling with
weights, strata, and clusters, and built-in Bonferroni / FDR
correction.
:::

:::{grid-item-card} 5 · `igem.plot` — Visualising
:link: user-guide/plotting-data
:link-type: doc

Publication-ready figures for association and interaction studies:
Manhattan plots (Bonferroni / FDR thresholds), QQ plots, top-hits
dotplots, heatmaps, Miami plots, before / after comparisons.
:::

:::{grid-item-card} 6 · `igem.reports` — Knowledge-graph reports
:link: user-guide/reporting-data
:link-type: doc

Annotate results with biological context from the IGEM knowledge
graph: gene, GO, disease, pathway, protein, and ontology
annotations, queried over HTTP or in embedded mode. See the
[full report catalogue](user-guide/reports-catalog.md).
:::

::::

---

## Lineage

IGEM is the third generation of an integrated software line developed
by the Hall Lab for high-dimensional interaction research:

- **PLATO** (Hall et al., *Nature Communications* 2017) — an
  integrated framework for GxG, GxE, and phenome-wide association
  studies, applied to type 2 diabetes in the eMERGE Network.
- **CLARITE** (Lucas et al., *Frontiers in Genetics* 2019) — a
  high-throughput pipeline for exposome quality control and
  environment-wide association studies.
- **IGEM** *(this project)* — extends the knowledge-driven filtering
  approach previously established for GxG via Biofilter to GxE and
  ExE, combining the analytical surface of PLATO and CLARITE with a
  curated cross-domain knowledge graph.

The methodology was originally proposed to the NIH **ECHO** program
(Environmental influences on Child Health Outcomes) to support
interaction studies across cohorts of 50,000+ children with
neurodevelopmental outcomes, and is now maintained as a
general-purpose platform applicable to any GxE / ExE study.

---

## Choose your path

::::{grid} 2 2 2 2
:gutter: 3
:margin: 4 4 0 0

:::{grid-item-card} Bioinformatician
:link: user-guide/index
:link-type: doc

Load data, run phenotype analyses, GWAS / EWAS, query the graph,
orchestrate jobs in containers and on HPC.
:::

:::{grid-item-card} Server developer
:link: user-guide-server/index
:link-type: doc

Drive the `igem-server` programmatically — database lifecycle,
ETL pipelines, NLP, and custom reports.
:::

:::{grid-item-card} Sysadmin / DevOps
:link: operations/index
:link-type: doc

Stand up the server, run the ETL, generate snapshots, deploy
containers, monitor production.
:::

:::{grid-item-card} Cookbook
:link: cookbook/index
:link-type: doc

End-to-end recipes that combine the client and server in realistic
research workflows.
:::

::::

---

## Quick start

```bash
pip install igem
```

```python
from igem import IGEM

with IGEM() as igem:
    print(igem.health())
```

The five-minute tour — connecting to a server, loading PLINK data, and
running a filtered GxE scan — is in the
[Quickstart](getting-started/quickstart.md).

---

## Project status

```{note}
IGEM is in **early access**. The client (`igem` 2.1.0) is in beta and
the server (`igem-server` 0.1.0) is in alpha; APIs may evolve before
the 1.0 release. Authentication is intentionally disabled during this
phase to keep the focus on the core analytical surface — production
deployments should be placed behind a network boundary.
```

IGEM is released under the **MIT licence**. Releases follow semantic
versioning per artefact; see the
[Release process](development/release-process.md) for the tagging
conventions and CI guarantees.

```{toctree}
:caption: Getting Started
:maxdepth: 2
:hidden:

getting-started/index
```

```{toctree}
:caption: User Guide — IGEM
:maxdepth: 2
:hidden:

user-guide/index
```

```{toctree}
:caption: User Guide — IGEM Server
:maxdepth: 2
:hidden:

user-guide-server/index
```

```{toctree}
:caption: Operations
:maxdepth: 2
:hidden:

operations/index
```

```{toctree}
:caption: Cookbook
:maxdepth: 2
:hidden:

cookbook/index
```

```{toctree}
:caption: API Reference
:maxdepth: 2
:hidden:

api-reference/index
```

```{toctree}
:caption: Development
:maxdepth: 2
:hidden:

development/index
```

```{toctree}
:caption: Release Notes
:maxdepth: 1
:hidden:

release-notes
```
