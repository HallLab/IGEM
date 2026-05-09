# disease_annotations

Master annotation report for diseases. Returns one row per matched
disease entity with cross-references (MONDO, OMIM, MeSH, ICD-10,
Orphanet), disease group memberships, and relationship summary.

Accepts a free list of identifiers (MONDO IDs, OMIM IDs, MeSH IDs,
ICD-10 codes, Orphanet IDs, disease names, or any registered alias).
Mixed identifier types in a single call are supported. Inputs with no
match produce a `not_found` row unless `emit_not_found_rows=False`.
Passing an empty input list returns all diseases (optionally filtered
by group name).

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_values` | list / str | `[]` | Disease identifiers or names to look up. Empty → all diseases. |
| `group_filter` | str | `None` | Restrict the output to a named disease group (e.g. `"autoimmune"`). |
| `emit_not_found_rows` | bool | `True` | Include `not_found` rows for unmatched inputs. |
| `include_relationships` | bool | `True` | Populate relationship summary columns. |
| `include_aliases` | bool | `True` | Populate `other_aliases`. |

---

## Output Columns

| Column | Description |
|---|---|
| `input_value` | Original input identifier as supplied. |
| `input_matched_alias` | The stored alias value that matched the input. |
| `entity_id` | IGEM entity ID for the matched disease. |
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
| `entity_relationships_by_group` | Relationship counts by entity type, e.g. `Genes:45; Drugs:12`. |
| `total_entity_relationships` | Total `EntityRelationship` rows for this entity. |
| `other_aliases` | Semicolon-separated non-primary aliases (up to 15). |
| `status` | `found` \| `not_found` \| `error`. |
| `note` | Human-readable detail when `status != "found"`. |

---

## CLI Usage

```bash
# Look up by MONDO ID and OMIM ID
igem report disease_annotations \
    --input "MONDO:0005301" --input "OMIM:104300"

# Look up by name
igem report disease_annotations --input "multiple sclerosis"

# Filter to a named group
igem report disease_annotations \
    --group-filter autoimmune \
    --output autoimmune_diseases.csv
```

## Python API

```python
from igem import IGEM

with IGEM() as igem:
    # Mixed identifier types in one call
    result = igem.report.disease_annotations(
        input_values=[
            "MONDO:0005301",       # MONDO
            "OMIM:104300",         # OMIM
            "multiple sclerosis",  # name
        ],
    )

    # Filter to a single disease group
    result = igem.report.disease_annotations(
        group_filter="autoimmune",
    )

print(result.df[["disease_id", "label", "mondo_id", "icd10"]])
```
