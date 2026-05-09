# go_annotations

Master annotation report for Gene Ontology terms. Returns one row per
matched term with namespace, name, parent / child counts in the GO
DAG, and relationship summary.

Accepts a free list of identifiers (GO IDs of the form `GO:xxxxxxx`,
term names, synonyms, or any registered alias). Inputs with no match
produce a `not_found` row unless `emit_not_found_rows=False`. Passing
an empty input list returns all GO terms (optionally restricted by
namespace).

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_values` | list / str | `[]` | GO IDs, term names, or aliases. Empty → all terms. |
| `namespace` | str | `None` | Restrict to a single GO namespace: `"BP"`, `"MF"`, or `"CC"`. |
| `emit_not_found_rows` | bool | `True` | Include `not_found` rows for unmatched inputs. |
| `include_relationships` | bool | `True` | Populate relationship summary columns. |
| `include_aliases` | bool | `True` | Populate `other_aliases`. |

Namespace codes:

| Code | Label |
|---|---|
| `BP` | Biological Process |
| `MF` | Molecular Function |
| `CC` | Cellular Component |

---

## Output Columns

| Column | Description |
|---|---|
| `input_value` | Original input identifier as supplied. |
| `input_matched_alias` | The stored alias value that matched the input. |
| `entity_id` | IGEM entity ID for the matched GO term. |
| `go_id` | GO identifier (e.g. `GO:0007049`). |
| `go_name` | GO term name. |
| `namespace` | `BP` / `MF` / `CC`. |
| `namespace_label` | Human-readable namespace (e.g. `Biological Process`). |
| `go_parent_count` | Number of parent terms in the GO DAG. |
| `go_child_count` | Number of child terms in the GO DAG. |
| `entity_relationships_by_group` | Relationship counts by entity type, e.g. `Genes:120; Proteins:85`. |
| `total_entity_relationships` | Total `EntityRelationship` rows for this entity. |
| `other_aliases` | Semicolon-separated non-primary aliases (up to 15). |
| `status` | `found` \| `not_found` \| `error`. |
| `note` | Human-readable detail when `status != "found"`. |

---

## CLI Usage

```bash
# Look up specific GO IDs
igem report go_annotations \
    --input "GO:0007049" --input "GO:0006281"

# Look up by name
igem report go_annotations --input "cell cycle"

# Restrict to a single namespace
igem report go_annotations --namespace BP --output bp_terms.csv
```

## Python API

```python
from igem import IGEM

with IGEM() as igem:
    # Mixed identifier types
    result = igem.report.go_annotations(
        input_values=["GO:0007049", "GO:0006281", "cell cycle"],
    )

    # Restrict to Molecular Function
    result = igem.report.go_annotations(namespace="MF")

print(result.df[["go_id", "go_name", "namespace_label"]])
```
