# NLP Module — Entity Resolution

Resolve free text (descriptions, abstracts, literature) to `entity_id` values
already present in the IGEM knowledge base, and persist the resulting mentions
and co-occurrence relationships.

The module is deliberately decoupled from any specific data source. Any DTP
that has text columns (HMDB descriptions today; MONDO definitions, UniProt
comments, PubMed abstracts tomorrow) can instantiate one `EntityResolver` and
feed it rows.

---

## 1. Architecture

```
                         ┌──────────────────────────┐
  ETL master DTPs  ───▶  │  entity_aliases (DB)     │
  (HGNC, ChEBI, HMDB…)   │  alias_value, alias_norm │
                         └────────────┬─────────────┘
                                      │   (loaded once per process)
                                      ▼
                         ┌──────────────────────────┐
                         │  AliasDictionary         │
                         │  - Aho-Corasick automaton│
                         │  - norm_index (O(1))     │
                         └────────────┬─────────────┘
                                      │
                                      ▼
       text  ──▶  ┌──────────────────────────────────┐ ──▶  list[ResolvedMatch]
                  │  EntityResolver                  │
                  │  strategies: exact_aho  (now)    │
                  │              code_pattern (next) │
                  │              tsvector    (next)  │
                  │              embedding   (next)  │
                  └──────────────────────────────────┘
                                      │
                   ┌──────────────────┴──────────────────┐
                   ▼                                     ▼
         ┌──────────────────┐               ┌──────────────────────────┐
         │  entity_matches  │               │  entity_relationships    │
         │  (one row per    │               │  relation_type =         │
         │   mention)       │               │  co_occurs_with          │
         └──────────────────┘               └──────────────────────────┘
          persister.py                        relation_builder.py
```

### EntityAlias vs EntityMatch

Two tables that look similar but answer different questions.

| aspect         | EntityAlias                           | EntityMatch                                |
|----------------|---------------------------------------|--------------------------------------------|
| meaning        | **dictionary entry**                  | **observation / evidence**                 |
| asks           | "what text maps to this entity?"      | "where was this entity mentioned?"         |
| produced by    | master DTPs (HGNC, HMDB, …)           | NLP resolver                               |
| example        | `"BRCA1" → entity_id=42`              | `"…mutations in BRCA1 are…" → entity_id=42`|
| keyed by       | entity + normalized alias             | entity + source text span                  |
| lifetime       | curated reference; rebuilt by ETL     | append-only log of where we found mentions |

The resolver **reads** EntityAlias (via the AliasDictionary) and **writes**
EntityMatch. The two tables never overwrite each other.

---

## 2. Public interface

All imports below come from the `igem_backend.modules.nlp` package.

### `EntityResolver`

The single entry point. Instantiate once per batch — the AliasDictionary
build is the expensive step.

```python
from igem_backend.modules.nlp import EntityResolver, OutputMode

resolver = EntityResolver(
    session=session,                  # SQLAlchemy session, kept open
    type_names=["Genes", "Chemicals"],# optional: narrow the dictionary
    domains=["Exposome"],             # optional: narrow by domain
    confidence_threshold=0.9,         # default
)
```

Build cost is proportional to the number of active aliases (≈30–60 s on a full
IGEM DB). After that, per-text cost is a few milliseconds regardless of
dictionary size (Aho-Corasick is O(text length), not O(dict size)).

**Three methods, same underlying engine:**

```python
# 1. single string
matches = resolver.resolve_text(
    "Lead exposure and IL-6 elevation in childhood",
    mode=OutputMode.SMART,
    source_record_id="HMDB0000001",
    source_field="description",
)

# 2. list of strings, keyed by input index
results = resolver.resolve_list(
    ["BRCA1 in breast cancer", "mercury toxicity in tuna"],
    mode=OutputMode.BEST_MATCH,
    source_field="abstract",
)
# {0: [ResolvedMatch(...)], 1: [ResolvedMatch(...)]}

# 3. DataFrame over one or more text columns
matches = resolver.resolve_dataframe(
    df,
    text_columns=["title", "abstract"],
    id_column="pmid",
)
```

**Maintenance helpers:**

```python
resolver.is_dictionary_stale()   # True if new ETLPackages landed since build
resolver.reload_dictionary()     # Rebuild from current DB state
```

### `OutputMode`

Controls how ambiguity is surfaced per text span.

| mode              | behavior                                               | when to use                               |
|-------------------|--------------------------------------------------------|-------------------------------------------|
| `BEST_MATCH`      | top-1 candidate per span, always                       | DTP batch mode (one answer per input)     |
| `ALL_CANDIDATES`  | every candidate per span, sorted by confidence desc    | exploratory UI, human review workflows    |
| `SMART` (default) | top-1 when confident; all candidates when ambiguous    | general-purpose default                   |

"Confident" = best candidate's confidence >= `confidence_threshold`.

### `ResolvedMatch`

Frozen dataclass — the "currency" of the module. See full field list in
[output.py](output.py). Key fields:

```python
ResolvedMatch(
    matched_text="il 6",         # what matched, in normalized form
    span_start=21, span_end=24,  # character offsets in normalized text
    context="…and il 6 elevation in childhood",

    entity_id=1234,              # the resolved entity
    entity_type_name="Genes",
    entity_domain="Genomics",

    alias_id=9876,               # which alias row was hit
    alias_type="synonym",        # preferred | synonym | code
    xref_source="HGNC",

    match_method="exact_aho",    # strategy that produced this match
    confidence=0.5,              # 1.0 / n_distinct_entities for exact hits
    review_status="pending",     # auto | pending | confirmed | rejected

    source_record_id="PMID123",  # provenance — where the text came from
    source_field="abstract",
)
```

### DB-facing helpers (internal, but exposed for DTP authors)

```python
from igem_backend.modules.nlp.persister import persist_matches
from igem_backend.modules.nlp.relation_builder import build_from_matches

# Both take a pandas DataFrame with the columns produced by the transform step
# and return (created, errors) / (created, skipped).
```

A typical relationship DTP's `load()` does exactly two things:

```python
persist_matches(df, session, etl_package_id)          # writes EntityMatch
build_from_matches(df, session, data_source_id,       # writes EntityRelationship
                   etl_package_id)
```

See [dtp_nlp_hmdb.py](../etl/dtps/dtp_nlp_hmdb.py) for a complete reference.

---

## 3. Design decisions

### 3.1 Confidence formula: `1.0 / n_distinct_entities`

An exact match that hits exactly one entity gets `confidence = 1.0`. A match
that hits N entities (ambiguous alias — e.g. `"lead"` mapping to both the
metal and the verb sense) gets `confidence = 1/N`.

No magic numbers, no tuning — the formula follows from "how many ways could
this have been right?". The `review_status` is then a simple threshold:

```python
review_status = "auto" if confidence >= threshold else "pending"
```

Default threshold is `0.9`, so anything more ambiguous than 1-of-1 drops to
`pending` until a human confirms. Downstream queries can filter on
`review_status = 'auto'` to work only with high-confidence data.

### 3.2 Normalization alignment

**Both** the ETL (when it writes `alias_norm`) and the resolver (when it
scans input text) **must** use the same `normalize()` function. If they drift,
hyphenated aliases like `"IL-6"` stop matching because one side stores
`"il-6"` and the other scans `"il 6"`.

Enforced by having a single canonical `normalize()` in
[normalizer.py](normalizer.py) that both paths import. The ETL-side helper
`igem_backend.utils.text.normalize_text` is a thin delegator to this function.

Rules:
- lowercase
- strip accents (NFKD + combining mark filter)
- `[-_/]` → space
- collapse whitespace

`normalize_code()` is a separate helper for structured IDs (HMDB0000001,
MESH:C007309) — it lowercases and strips but preserves colons and dots.

### 3.3 Strategy pattern

Today only one strategy is wired: `exact_aho` (Aho-Corasick on `alias_norm`
followed by a word-boundary check). The resolver's `_apply_mode` is strategy-
agnostic — adding a new strategy is a two-step change:

1. write `strategies/<name>.py` exposing `resolve(text, dictionary, ...) -> list[ResolvedMatch]`
2. chain it in `EntityResolver.resolve_text` with first-match-wins-per-span semantics

Planned strategies:

| strategy       | when it fires                                          |
|----------------|--------------------------------------------------------|
| `code_pattern` | regex for structured IDs (`HMDB\d{7}`, `HGNC:\d+`, …) — runs before exact_aho for ID-like spans |
| `tsvector`     | PostgreSQL full-text morphological matching for inflections the normalizer doesn't cover |
| `fuzzy`        | Levenshtein distance fallback when exact_aho misses    |
| `embedding`    | SapBERT + pgvector cosine similarity for semantic hits |

### 3.4 `MAX_ENTITIES_PER_RECORD = 20`

In `relation_builder.py`, any source record that resolves to more than 20
distinct entities is **skipped** for relationship building. This guards
against N² explosion (50 entities → 1 225 pairs, most of them noise from a
description that names half of biochemistry).

EntityMatch rows are still persisted for those records — only the co-occurrence
edges are suppressed.

### 3.5 alias_id FK uses `SET NULL`, not `CASCADE`

`EntityMatch.alias_id` points to the specific alias that matched. When an
ETL run refreshes the alias set, some alias rows get recreated with new ids.
Using `ON DELETE SET NULL` keeps the historical EntityMatch row intact —
we still know the entity, the span, and the method; we just lose the pointer
to the exact alias row that no longer exists. `CASCADE` would delete the
observation, which is the wrong default for append-only evidence.

### 3.6 Why build the dictionary in memory

Alternative considered: query `entity_aliases` per input text via `ILIKE` or
tsvector. Rejected because:
- dictionary is mostly stable across a batch (stale check handles updates)
- Aho-Corasick scan is O(text length) — independent of dictionary size
- in-DB matching pays a round-trip per input; in-memory pays one bulk load

Trade-off: peak memory ≈ a few hundred MB for a full alias set. Acceptable
for batch jobs; long-running services call `is_dictionary_stale()` and
`reload_dictionary()` to stay current.

---

## 4. Performance notes

| phase                          | cost (full IGEM DB)            |
|--------------------------------|--------------------------------|
| AliasDictionary load from DB   | ~20–40 s                       |
| Aho-Corasick automaton build   | ~10–20 s                       |
| per-text scan                  | ~1–5 ms per description        |
| HMDB full NLP pass (~220k rec) | ~5–15 min end-to-end           |

Bottleneck today is the per-row DataFrame iteration in `dtp_nlp_hmdb.transform`,
not the scan itself. If throughput becomes an issue, the natural move is to
batch the scan into `resolver.resolve_list()` and parallelize via multiprocess
workers sharing the same read-only automaton.

---

## 5. Files in this module

| file                      | role                                                  |
|---------------------------|-------------------------------------------------------|
| `__init__.py`             | public exports                                        |
| `normalizer.py`           | canonical `normalize()` + `normalize_code()`          |
| `dictionary.py`           | `AliasDictionary` + `AliasEntry` + Aho-Corasick       |
| `output.py`               | `OutputMode`, `ResolvedMatch`, alias priority         |
| `resolver.py`             | `EntityResolver` — public API                         |
| `strategies/exact.py`     | Aho-Corasick + word-boundary + confidence scoring     |
| `persister.py`            | `persist_matches()` — DataFrame → `EntityMatch` rows  |
| `relation_builder.py`     | `build_from_matches()` → `EntityRelationship` rows    |
