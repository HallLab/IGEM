# Server-side reports

Reports are the server's curated lookup operations against the IGEM
knowledge graph — `gene_annotations`, `pathway_annotations`,
`protein_annotations`, and so on. Each report is a small Python
class that runs a SQL query, post-processes the result into a
DataFrame, and returns it through a uniform contract that the client
consumes via `igem.reports`.

This page documents what's involved in **adding a new report**:
where the file goes, the contract you implement, how it gets picked
up at runtime, and the end-to-end checklist that takes a new report
from skeleton code to fully wired into the client.

For the analyst-facing perspective — *how to call* the reports —
see [Reporting data](../user-guide/reporting-data.md) and the
[Report catalogue](../user-guide/reports-catalog.md).

---

## Architecture in one diagram

```
┌──────────────────── client ────────────────────┐
│  igem.report.<name>(...)        ← typed helper│
│  igem.report.run("<name>", ...) ← generic     │
└───────────────┬────────────────────────────────┘
                │ HTTP POST /api/v1/reports/<name>/run
                ▼
┌──────────────────── server ────────────────────┐
│  ReportManager._discover()       ← auto-finds  │
│  YourNewReport.run(session, ...) ← your class  │
│         │                                       │
│         └──→ pandas.DataFrame                  │
│                  │                              │
│                  ▼                              │
│         ReportRunResponse  ←  ReportManager.run│
└────────────────────────────────────────────────┘
```

The server has **one** registry (`ReportManager`) that auto-discovers
every subclass of `ReportBase` inside the `reports/` subpackage. As
long as your file lives in the right directory and your class
inherits from `ReportBase` with a `REPORT_NAME` attribute, it shows
up in `igem.report.list()` automatically.

---

## File layout

```
backend/src/igem_backend/modules/report/
├── report_manager.py             ← registry + dispatcher (don't touch)
├── reports/
│   ├── base_report.py            ← ReportBase ABC (don't touch)
│   ├── report_gene_annotations.py    ← one file per report
│   ├── report_disease_annotations.py
│   ├── report_go_annotations.py
│   ├── report_pathway_annotations.py
│   └── report_protein_annotations.py
└── reports_explain/
    ├── gene_annotations.md       ← markdown shown by .explain()
    ├── disease_annotations.md
    └── ...
```

Two conventions worth highlighting:

- **One report per file**, named `report_<snake_case>.py`. The
  manager discovers via `pkgutil.iter_modules`, not by file name —
  any non-`base_report` module in `reports/` is scanned for
  `ReportBase` subclasses — but the prefix keeps the directory
  obvious to grep.
- **The markdown explain file's stem matches `REPORT_NAME` exactly.**
  `gene_annotations.md` is what the server returns when a client
  calls `igem.report.explain("gene_annotations")`. If the file is
  missing, the server falls back to a one-line stub built from the
  class's `REPORT_DESCRIPTION`, which is rarely useful.

---

## The `ReportBase` contract

Every report subclasses `ReportBase` (defined in
`backend/src/igem_backend/modules/report/reports/base_report.py`) and
implements three abstract methods plus three class attributes.

```python
from igem_backend.modules.report.reports.base_report import ReportBase
import pandas as pd
from sqlalchemy.orm import Session


class MyNewReport(ReportBase):
    # --- Class attributes ----------------------------------------
    REPORT_NAME        = "my_new_report"     # unique key, snake_case
    REPORT_VERSION     = "1.0.0"             # semver
    REPORT_DESCRIPTION = "One-line summary."  # also shown by list()

    # --- Abstract methods ----------------------------------------
    def run(self, session: Session, **kwargs) -> pd.DataFrame:
        """Execute the report and return one row per output entity."""
        ...

    def available_columns(self) -> list[str]:
        """Full ordered list of column names this report can produce."""
        ...

    def example_input(self) -> dict[str, Any]:
        """A sample kwargs dict suitable for a smoke-test invocation."""
        ...
```

### `REPORT_NAME`

The unique identifier the client uses (`igem.report.run("my_new_report", ...)`).
**Must be `snake_case`** to match the URL path (`/api/v1/reports/<name>/run`).
Once published, treat as immutable — renaming breaks every caller.

### `REPORT_VERSION`

Bump the patch when you fix output bugs without changing the column
list. Bump the minor when you add columns. Bump the major when you
remove columns or change parameter semantics. This becomes visible
in `igem.report.list()` and lets analysts pin against a specific
schema.

### `REPORT_DESCRIPTION`

One sentence, ≤80 chars. Shows up next to the report name in
`igem.report.list()`, so it should answer *"what does this resolve?"*
not *"how does it work?"*.

### `run(session, **kwargs)`

Where the work happens. Receives a SQLAlchemy `Session` (already
managed by `ReportManager.run`) and arbitrary kwargs the client sent
in the `params` dict. Must return a `pandas.DataFrame` whose columns
are a subset of `available_columns()`.

The two helpers from `ReportBase` cover the common patterns:

```python
inputs       = self.resolve_input_list(self.param(kwargs, "input_values"))
emit_missing = self.param(kwargs, "emit_not_found_rows", True)
```

`resolve_input_list` accepts a list, a comma-separated string, or
`None`, and always returns a flat `list[str]`. `param` is just a
`kwargs.get` with default — kept as a method for symmetry with the
other helpers.

### `available_columns()`

The **full** ordered list of column names the report can emit.
Clients use this to pre-validate `columns=` projections without
running the report. Keep this in sync with the columns you return
from `run` — drift here is a common gotcha.

### `example_input()`

A small kwargs dict that runs the report against the canonical demo
snapshot. Used by smoke tests and dev tooling. Should be small (a
handful of inputs), realistic, and exercise the typical happy path.

---

## End-to-end checklist

When adding a new report, work through these in order:

### 1. On the server

- Create `reports/report_<name>.py` with your `ReportBase` subclass.
- Add a markdown file at `reports_explain/<name>.md` with: title,
  one-paragraph summary, parameters table, output columns table,
  CLI examples, Python examples. The five existing reports are the
  canonical templates.
- Bump the snapshot generation script if your report needs new
  ETL outputs. Reports that only join existing tables don't.
- Add a backend test under `tests/report/test_<name>.py` covering:
  the happy path against a fixture session, an empty input list
  (all-mode), and any report-specific toggles.

### 2. On the client

- Add a typed helper to `ReportManager`
  (`client/src/igem/modules/report/manager.py`) using
  `_run_with_inputs` for the common merge / run / save flow.
- Add the matching wrapper to `ReportComponent`
  (`client/src/igem/core/components/report_component.py`).
- Add a smoke test in `client/tests/report/test_report_component.py` —
  the `TestFacadeCoverage` guard already covers the wiring; you only
  need to add a smoke test that propagates the report-specific
  kwargs (`namespace=`, `group_filter=`, etc.).
- Add manager-level tests for any new kwargs in
  `client/tests/report/test_report_manager.py`.

### 3. Documentation

- Add a new section to the
  [Report catalogue](../user-guide/reports-catalog.md) — parameters
  table, output columns, example, notes.
- If the report introduces a new analytical workflow (rather than
  just adding a row type), add a cookbook recipe under
  `docs/sphinx/cookbook/`.

### 4. Smoke check

```bash
# From the client side
python -c "
from igem import IGEM
with IGEM() as igem:
    print([r.name for r in igem.report.list()])
    print(igem.report.<your_helper>(input_values=[...]).df.head())
"
```

If your report shows up in `list()` and returns a DataFrame on
`<your_helper>(...)`, you're done.

---

## Failure modes worth knowing

### Report doesn't show up in `list()`

`ReportManager._discover()` swallows import errors and logs a
`WARNING`. Check the server log for `[report] Could not load
module 'report_<name>': <exc>`. The most common causes are a typo
in `REPORT_NAME`, a missing `from` import, or an exception raised
at *module import time* (rare, but happens when the report file
runs SQL at top level).

### `available_columns` and actual output drift

Clients that pass `columns=` will silently get an empty DataFrame
for any column you forgot to add to `available_columns()`. Add a
backend test that asserts
`set(df.columns).issubset(set(report.available_columns()))` for
every kwargs combination you support.

### Markdown explain not loaded

`ReportManager.explain` looks for
`reports_explain/<REPORT_NAME>.md`. If the stem doesn't match
exactly (one underscore off, plural vs singular), you'll silently
get the one-line description fallback. Match `REPORT_NAME` exactly.

### Forgot to wire on the client

`TestFacadeCoverage` in the client test suite catches this — it
asserts every public method on `ReportManager` is reachable on
`ReportComponent`. CI fails immediately. If you don't see the
failure, you forgot to add the client-side helper at step 2.
