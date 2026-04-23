# gene_annotations

Master annotation report for human genes. Accepts a list of gene identifiers
and returns one row per matched entity with consolidated cross-references,
genomic coordinates, and relationship summary.

---

## Parameters

| Parameter      | Type     | Default      | Description                                             |
|----------------|----------|--------------|---------------------------------------------------------|
| `input_values` | list[str]| *(all genes)*| Symbols, HGNC IDs, Ensembl IDs, Entrez IDs, or aliases |
| `assembly`     | str      | `GRCh38.p14` | Genome assembly used for coordinate columns             |

Input values are normalized (case-insensitive) and matched against all
registered aliases. An input with no match returns a row with
`status = "not_found"`.

---

## Output columns

| Column                        | Description                                                  |
|-------------------------------|--------------------------------------------------------------|
| `input_value`                 | Value supplied by the caller                                 |
| `input_matched_alias`         | The alias that resolved the input                            |
| `entity_id`                   | IGEM internal entity identifier                              |
| `gene_symbol`                 | Approved HGNC gene symbol                                    |
| `hgnc_id`                     | HGNC identifier (e.g. `HGNC:11998`)                         |
| `ensembl_id`                  | Ensembl gene ID (e.g. `ENSG00000141510`)                     |
| `entrez_id`                   | NCBI Entrez Gene ID                                          |
| `hgnc_status`                 | HGNC approval status (`Approved`, `Gene from NCBI`, etc.)   |
| `gene_locus_group`            | HGNC locus group (e.g. `protein-coding gene`)               |
| `gene_locus_type`             | HGNC locus type (e.g. `gene with protein product`)          |
| `gene_groups`                 | Semicolon-separated HGNC gene family / group names           |
| `assembly`                    | Genome assembly name for coordinate columns                  |
| `chromosome`                  | Chromosome number (1-22, 23=X, 24=Y, 25=MT)                 |
| `start_position`              | Genomic start (1-based, GRCh38.p14 by default)              |
| `end_position`                | Genomic end (1-based)                                        |
| `strand`                      | `+` or `-`                                                   |
| `entity_relationships_by_group` | Semicolon-separated `EntityType:count` pairs              |
| `total_entity_relationships`  | Total relationship count across all entity types             |
| `other_aliases`               | Additional aliases not captured in the dedicated columns     |
| `status`                      | `found` or `not_found`                                       |
| `note`                        | Warning or error message when `status = not_found`           |

---

## CLI examples

```bash
# Single gene
igem-server report run --name gene_annotations --input TP53

# Multiple genes
igem-server report run --name gene_annotations --input TP53 --input BRCA1 --input EGFR

# Save to CSV
igem-server report run --name gene_annotations --input TP53 --output tp53.csv

# Subset of columns
igem-server report run --name gene_annotations \
  --input TP53 \
  --columns gene_symbol,hgnc_id,ensembl_id,chromosome,start_position,end_position
```

## Python API examples

```python
from igem_backend.ge import GE

ge = GE("sqlite:///igem.db")

# Run for a list of genes
df = ge.report.run(
    "gene_annotations",
    input_values=["TP53", "BRCA1", "EGFR"],
    assembly="GRCh38.p14",
)

# Inspect results
print(df[["gene_symbol", "hgnc_id", "chromosome", "start_position", "end_position"]])
```
