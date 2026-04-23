# protein_annotations

Master protein annotation report. Returns one row per matched protein entity
with UniProt cross-references, functional annotations, Pfam domain summary,
and relationship counts.

Accepts a free list of identifiers (UniProt accessions, protein names, gene
symbols, or any registered alias). Isoform accessions are resolved and the
row is annotated with the canonical counterpart. Inputs with no match produce
a `not_found` row unless `emit_not_found_rows=False`. Passing an empty input
list returns all proteins in the database.

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_values` | list / str | `[]` | Protein identifiers to look up. Empty → all proteins. |
| `emit_not_found_rows` | bool | `True` | Include `not_found` rows for unmatched inputs. |
| `include_pfam_summary` | bool | `True` | Populate `pfam_total_count` and `pfam_count_by_type`. |
| `include_pfam_details` | bool | `False` | Also populate `pfam_ids_by_type` (accession lists). |
| `max_pfam_ids_per_type` | int | `10` | Max Pfam accessions listed per domain type in `pfam_ids_by_type`. |
| `include_relationships` | bool | `True` | Populate relationship summary columns. |
| `include_aliases` | bool | `True` | Populate `other_aliases`. |

---

## Output Columns

| Column | Description |
|---|---|
| `input_value` | Original input identifier as supplied. |
| `input_matched_alias` | The stored alias value that matched the input. |
| `entity_id` | IGEM entity ID for the matched protein (may be an isoform). |
| `canonical_entity_id` | Entity ID of the canonical (non-isoform) entry for this master. |
| `protein_master_id` | Internal ProteinMaster PK (shared across isoforms). |
| `protein_id` | UniProt accession (e.g. `P04637`). |
| `input_is_isoform` | `True` if the matched entity is an isoform. |
| `input_isoform_accession` | Isoform-specific accession (e.g. `P04637-2`), or `None`. |
| `isoform_count` | Total number of isoforms registered for this protein. |
| `function` | Functional description from UniProt (truncated to 512 chars). |
| `location` | Subcellular location(s) from UniProt. |
| `tissue_expression` | Tissue expression notes from UniProt. |
| `protein_source_system` | Source system name (e.g. `UniProt`). |
| `protein_data_source` | Data source name (e.g. `protein_uniprot`). |
| `pfam_total_count` | Total Pfam domains linked to this protein master. |
| `pfam_count_by_type` | Semicolon-separated type counts, e.g. `Domain:5; Family:2`. |
| `pfam_ids_by_type` | Pfam accessions per type (requires `include_pfam_details=True`). |
| `entity_relationships_by_group` | Relationship counts by entity type, e.g. `Gene Ontology:45; Genes:1`. |
| `total_entity_relationships` | Total EntityRelationship rows for this entity. |
| `other_aliases` | Semicolon-separated non-primary aliases (up to 15). |
| `status` | `found` \| `not_found` \| `error`. |
| `note` | Human-readable detail when `status != found`. |

---

## CLI Usage

```bash
# Look up specific proteins
igem report run --name protein_annotations --input "P04637,P00533"

# With Pfam accession details
igem report run --name protein_annotations --input "P04637" \
    --columns "protein_id,pfam_total_count,pfam_count_by_type,pfam_ids_by_type"

# Export to CSV
igem report run --name protein_annotations --input "P04637,P00533,Q9Y6K9" \
    --output proteins.csv

# Export all proteins (no --input)
igem report run --name protein_annotations --output all_proteins.csv
```

## Python API

```python
from igem_backend.core import IGEM

igem = IGEM()

df = igem.report.run(
    "protein_annotations",
    input_values=["P04637", "P00533"],
    include_pfam_details=True,
    max_pfam_ids_per_type=5,
)

# Isoform query
df = igem.report.run(
    "protein_annotations",
    input_values=["P04637-2"],
)
print(df[["protein_id", "input_is_isoform", "input_isoform_accession", "canonical_entity_id"]])
```
