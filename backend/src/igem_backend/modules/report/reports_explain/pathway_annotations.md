# pathway_annotations

Master pathway annotation report. Returns one row per matched pathway entity
with source identifier, name, database of origin, and relationship counts.

Accepts a free list of identifiers (Reactome IDs, KEGG IDs, pathway names,
or any registered alias). Inputs with no match produce a `not_found` row
unless `emit_not_found_rows=False`. Passing an empty input list returns all
pathways in the database.

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_values` | list / str | `[]` | Pathway identifiers to look up. Empty → all pathways. |
| `emit_not_found_rows` | bool | `True` | Include `not_found` rows for unmatched inputs. |
| `include_relationships` | bool | `True` | Populate relationship summary columns. |
| `include_aliases` | bool | `True` | Populate `other_aliases`. |

---

## Output Columns

| Column | Description |
|---|---|
| `input_value` | Original input identifier as supplied. |
| `input_matched_alias` | The stored alias value that matched the input. |
| `entity_id` | IGEM entity ID for the matched pathway. |
| `pathway_id` | Source-native identifier (e.g. `R-HSA-109581`, `hsa04110`). |
| `pathway_name` | Human-readable pathway name. |
| `source_db` | Database of origin (`Reactome`, `KEGG`). |
| `organism` | Species (e.g. `Homo sapiens`). |
| `pathway_source_system` | Source system name (e.g. `Reactome`, `KEGG`). |
| `pathway_data_source` | Data source name (e.g. `pathway_reactome`, `pathway_kegg`). |
| `entity_relationships_by_group` | Relationship counts by entity type, e.g. `Genes:45; Proteins:12`. |
| `total_entity_relationships` | Total EntityRelationship rows for this entity. |
| `other_aliases` | Semicolon-separated non-primary aliases (up to 15). |
| `status` | `found` \| `not_found` \| `error`. |
| `note` | Human-readable detail when `status != found`. |

---

## CLI Usage

```bash
# Look up specific pathways by ID
igem report run --name pathway_annotations --input "R-HSA-109581,hsa04110"

# Look up by name (quoted)
igem report run --name pathway_annotations --input "Cell Cycle"

# Export to CSV
igem report run --name pathway_annotations --input "R-HSA-109581,R-HSA-1640170" \
    --output pathways.csv

# Export all pathways (no --input)
igem report run --name pathway_annotations --output all_pathways.csv
```

## Python API

```python
from igem_backend.core import IGEM

igem = IGEM()

# By Reactome ID
df = igem.report.run(
    "pathway_annotations",
    input_values=["R-HSA-109581", "R-HSA-1640170"],
)

# By KEGG ID
df = igem.report.run(
    "pathway_annotations",
    input_values=["hsa04110", "hsa04151"],
)

# By name
df = igem.report.run(
    "pathway_annotations",
    input_values=["Cell Cycle", "PI3K-Akt signaling pathway"],
)

print(df[["pathway_id", "pathway_name", "source_db", "entity_relationships_by_group"]])
```
