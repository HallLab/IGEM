# Report catalog

The IGEM server ships a registry of curated **annotation reports**
that resolve identifiers from external biological databases against
the IGEM knowledge graph. This page is the per-report reference: one
section per registered report, with parameters, output columns,
typical inputs, and gotchas.

For *how to call* reports — `list` / `explain` / `run`, typed
helpers, the `ReportResult` API, and the `igem report` CLI — see
[Reporting data](reporting-data.md). This page assumes you already
know the mechanics and just want to look up *what each report does*.

```{tip}
The catalog below mirrors the markdown returned by
`igem.report.explain(name)`. Calling `explain` from a notebook is
often the fastest way to confirm a parameter or a column name
without leaving your session.
```

```{note}
**The catalog grows over time.** As new reports are added on the
server, this page is updated to keep parity with `igem.report.list()`
on a current snapshot. If `list()` returns a name not described
here, run `explain(name)` for the canonical contract — the server is
authoritative.
```

Currently five reports are registered. They share a uniform
**input / output contract**, summarised once below to avoid repeating
it in every section.

---

## Shared contract

**Inputs** (most reports):

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_values` | `list[str]` or comma-separated `str` | `[]` | Identifiers to look up. Empty → return all entities. |
| `emit_not_found_rows` | `bool` | `True` | Emit a `status="not_found"` row for inputs with no match. |
| `include_relationships` | `bool` | `True` | Populate `entity_relationships_by_group` and `total_entity_relationships`. |
| `include_aliases` | `bool` | `True` | Populate `other_aliases`. |

`gene_annotations` is the historical exception: it takes
`input_values` and `assembly` only and does not expose the
`emit_not_found_rows` / `include_*` toggles (the values are baked
in).

Every report below has a **typed helper** of the form
`igem.report.<name>(...)` whose named kwargs mirror the table in
each section. The signatures are intentionally close: once you know
how to call one, the others follow the same shape.

**Common output columns** (every report):

| Column | Description |
|---|---|
| `input_value` | Value supplied by the caller (preserved verbatim). |
| `input_matched_alias` | Stored alias that resolved the input. |
| `entity_id` | IGEM internal entity identifier. |
| `entity_relationships_by_group` | Semicolon-separated `EntityType:count` pairs. |
| `total_entity_relationships` | Total relationship count across all entity types. |
| `other_aliases` | Additional aliases not captured in the dedicated columns. |
| `status` | `found` / `not_found` / `error`. |
| `note` | Warning or error message when `status != "found"`. |

The per-report sections below list **only the report-specific
columns**; the shared columns above are present in all of them.

---

## `gene_annotations`

Master annotation report for human genes. Accepts a list of gene
identifiers (symbols, HGNC IDs, Ensembl IDs, Entrez IDs, or any
registered alias) and returns one row per matched entity with
consolidated cross-references, locus classification, genomic
coordinates, and a relationship summary.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_values` | `list[str]` | *(all genes)* | Symbols, HGNC IDs, Ensembl IDs, Entrez IDs, or aliases. |
| `assembly` | `str` | `"GRCh38.p14"` | Genome assembly name used for the coordinate columns. |

Identifiers are normalized case-insensitively and matched against
all registered aliases. An input with no match emits a row with
`status = "not_found"`.

### Report-specific output columns

| Column | Description |
|---|---|
| `gene_symbol` | Approved HGNC gene symbol. |
| `hgnc_id` | HGNC identifier (e.g. `HGNC:11998`). |
| `ensembl_id` | Ensembl gene ID (e.g. `ENSG00000141510`). |
| `entrez_id` | NCBI Entrez Gene ID. |
| `hgnc_status` | HGNC approval status. |
| `gene_locus_group` | HGNC locus group (e.g. `protein-coding gene`). |
| `gene_locus_type` | HGNC locus type (e.g. `gene with protein product`). |
| `gene_groups` | Semicolon-separated HGNC gene family / group names. |
| `assembly` | Genome assembly name for the coordinate columns. |
| `chromosome` | Chromosome number (1–22, 23=X, 24=Y, 25=MT). |
| `start_position` | Genomic start (1-based). |
| `end_position` | Genomic end (1-based). |
| `strand` | `+` or `-`. |

### Example

```python
with IGEM() as igem:
    result = igem.report.gene_annotations(
        input_values=["TP53", "BRCA1", "EGFR"],
        assembly="GRCh38.p14",
    )

result.df[["gene_symbol", "hgnc_id", "chromosome", "start_position"]]
```

### Notes

- `RegressionResults.annotate(igem)` calls this report under the
  hood to enrich association results — see [Analyzing
  data](analyzing-data.md).
- The `assembly` parameter only affects the coordinate columns; the
  HGNC / Ensembl / Entrez identifiers are assembly-agnostic.

---

## `disease_annotations`

Master annotation report for diseases. Accepts a list of disease
identifiers (MONDO IDs, OMIM IDs, MeSH IDs, ICD-10 codes, Orphanet
IDs, names, or any registered alias) and returns one row per matched
entity with cross-references, disease group memberships, and a
relationship summary. Passing an empty input list returns all
diseases.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_values` | `list[str]` | *(all diseases)* | Disease identifiers or names to look up. |
| `group_filter` | `str \| None` | `None` | Restrict the output to a named disease group (e.g. `"autoimmune"`). |
| `emit_not_found_rows` | `bool` | `True` | See [shared contract](#shared-contract). |
| `include_relationships` | `bool` | `True` | See [shared contract](#shared-contract). |
| `include_aliases` | `bool` | `True` | See [shared contract](#shared-contract). |

### Report-specific output columns

| Column | Description |
|---|---|
| `disease_id` | Source-native primary disease ID (typically MONDO). |
| `label` | Canonical disease label. |
| `description` | Human-readable description. |
| `mondo_id` | MONDO cross-reference. |
| `omim_id` | OMIM cross-reference. |
| `mesh_id` | MeSH cross-reference. |
| `icd10` | ICD-10 cross-reference. |
| `orphanet_id` | Orphanet cross-reference. |
| `disease_groups` | Semicolon-separated disease group memberships. |
| `disease_parent_count` | Number of parent diseases in the ontology. |
| `disease_child_count` | Number of child diseases in the ontology. |

### Example

```python
with IGEM() as igem:
    result = igem.report.disease_annotations(
        input_values=["MONDO:0005301", "OMIM:104300", "multiple sclerosis"],
    )

result.df[["disease_id", "label", "mondo_id", "icd10"]]
```

### Notes

- Mixed identifier types in a single call are supported — the
  matcher resolves each input independently against the alias index.
- `group_filter` is most useful in **all-mode** (no `input_values`)
  for slicing the catalog by therapeutic area.

---

## `go_annotations`

Master annotation report for **Gene Ontology** terms. Accepts GO
identifiers (`GO:xxxxxxx`), term names, synonyms, or any registered
alias. Returns one row per matched term with namespace, name, and
relationship summary. Supports filtering by namespace.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_values` | `list[str]` | *(all GO terms)* | GO IDs, term names, or aliases. |
| `namespace` | `"BP" \| "MF" \| "CC" \| None` | `None` | Restrict to a single GO namespace. |
| `emit_not_found_rows` | `bool` | `True` | See [shared contract](#shared-contract). |
| `include_relationships` | `bool` | `True` | See [shared contract](#shared-contract). |
| `include_aliases` | `bool` | `True` | See [shared contract](#shared-contract). |

Namespace codes:

| Code | Label |
|---|---|
| `BP` | Biological Process |
| `MF` | Molecular Function |
| `CC` | Cellular Component |

### Report-specific output columns

| Column | Description |
|---|---|
| `go_id` | GO identifier (e.g. `GO:0007049`). |
| `go_name` | GO term name. |
| `namespace` | `BP` / `MF` / `CC`. |
| `namespace_label` | Human-readable namespace (e.g. `Biological Process`). |
| `go_parent_count` | Number of parent terms in the GO DAG. |
| `go_child_count` | Number of child terms in the GO DAG. |

### Example

```python
with IGEM() as igem:
    result = igem.report.go_annotations(
        input_values=["GO:0007049", "GO:0006281", "cell cycle"],
        namespace="BP",
    )

result.df[["go_id", "go_name", "namespace_label"]]
```

### Notes

- Combining `namespace="BP"` with an explicit `input_values` list
  filters the *output* — terms outside BP simply emit `not_found`.
- The relationship summary is most informative when querying broad
  terms (large `entity_relationships_by_group` totals).

---

## `pathway_annotations`

Master annotation report for biological pathways. Accepts Reactome
IDs, KEGG IDs, pathway names, or any registered alias. Returns one
row per matched pathway with source metadata and relationship
summary.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_values` | `list[str]` | *(all pathways)* | Reactome IDs, KEGG IDs, names, or aliases. |
| `emit_not_found_rows` | `bool` | `True` | See [shared contract](#shared-contract). |
| `include_relationships` | `bool` | `True` | See [shared contract](#shared-contract). |
| `include_aliases` | `bool` | `True` | See [shared contract](#shared-contract). |

### Report-specific output columns

| Column | Description |
|---|---|
| `pathway_id` | Source-native ID (e.g. `R-HSA-109581`, `hsa04110`). |
| `pathway_name` | Human-readable pathway name. |
| `source_db` | Database of origin (`Reactome`, `KEGG`). |
| `organism` | Species (e.g. `Homo sapiens`). |
| `pathway_source_system` | Source system name. |
| `pathway_data_source` | Data source name (e.g. `pathway_reactome`). |

### Example

```python
with IGEM() as igem:
    result = igem.report.pathway_annotations(
        input_values=["R-HSA-109581", "hsa04110", "Cell Cycle"],
    )

result.df[["pathway_id", "pathway_name", "source_db",
           "entity_relationships_by_group"]]
```

### Notes

- Reactome IDs and KEGG IDs share a single `pathway_id` column —
  disambiguate with `source_db` if needed.
- Querying by name (`"Cell Cycle"`) is exact-match against the
  alias index; close matches without an exact hit return
  `not_found`.

---

## `protein_annotations`

Master annotation report for human proteins. Accepts UniProt
accessions, protein names, gene symbols, or any registered alias.
Returns one row per matched entity with consolidated UniProt
cross-references, function / location notes, Pfam domain summary,
and relationship summary. Isoform inputs are resolved and annotated
with their canonical counterpart.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_values` | `list[str]` | *(all proteins)* | UniProt accessions, names, gene symbols, or aliases. |
| `include_pfam_summary` | `bool` | `True` | Populate `pfam_total_count` and `pfam_count_by_type`. |
| `include_pfam_details` | `bool` | `False` | Also populate `pfam_ids_by_type` (accession lists). |
| `max_pfam_ids_per_type` | `int` | `10` | Cap the number of Pfam accessions per type when `include_pfam_details=True`. |
| `emit_not_found_rows` | `bool` | `True` | See [shared contract](#shared-contract). |
| `include_relationships` | `bool` | `True` | See [shared contract](#shared-contract). |
| `include_aliases` | `bool` | `True` | See [shared contract](#shared-contract). |

### Report-specific output columns

| Column | Description |
|---|---|
| `canonical_entity_id` | Entity ID of the canonical (non-isoform) entry. |
| `protein_master_id` | Internal `ProteinMaster` PK (shared across isoforms). |
| `protein_id` | UniProt accession (e.g. `P04637`). |
| `input_is_isoform` | `True` if the matched entity is an isoform. |
| `input_isoform_accession` | Isoform-specific accession (e.g. `P04637-2`), or `None`. |
| `isoform_count` | Total number of isoforms registered for this protein master. |
| `function` | Functional description from UniProt (truncated to 512 chars). |
| `location` | Subcellular location(s). |
| `tissue_expression` | Tissue expression notes. |
| `protein_source_system` | Source system name (e.g. `UniProt`). |
| `protein_data_source` | Data source name (e.g. `protein_uniprot`). |
| `pfam_total_count` | Total Pfam domains linked to this protein master. |
| `pfam_count_by_type` | Semicolon-separated `Type:count` pairs. |
| `pfam_ids_by_type` | Pfam accessions per type (only when `include_pfam_details=True`). |

### Example

```python
with IGEM() as igem:
    result = igem.report.protein_annotations(
        input_values=["P04637", "P00533", "P04637-2"],
        include_pfam_details=True,
        max_pfam_ids_per_type=5,
    )

result.df[[
    "protein_id", "input_is_isoform", "input_isoform_accession",
    "canonical_entity_id", "pfam_total_count",
]]
```

### Notes

- Isoform queries (`P04637-2`) resolve to the same
  `protein_master_id` as the canonical accession (`P04637`); use
  `input_is_isoform` to tell them apart.
- Pfam **details** can inflate the response considerably for
  proteins with many domains — prefer the summary
  (`include_pfam_summary=True`, `include_pfam_details=False`) unless
  you specifically need the accessions.
- Querying by gene symbol returns the protein product, not the
  gene — use [`gene_annotations`](#gene_annotations) for the gene.

---

## See also

- [Reporting data](reporting-data.md) — how to call the reports
  (Python API and CLI), `ReportResult` interface, and integration
  with `analyze.annotate`.
- [Analyzing data](analyzing-data.md) — `RegressionResults.annotate`
  for joining `gene_annotations` columns into an EWAS / GWAS result.
- [Cookbook → Custom report end-to-end](../cookbook/custom-report-end-to-end.md)
  — adding a new report on the server side.
