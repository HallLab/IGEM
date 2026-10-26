# User Guide — IGEM Server

For developers and data engineers using the **`igem-server`** package
as a Python library — the programmatic surface that sits behind the
HTTP / ASGI API the client talks to.

This guide complements [User Guide — IGEM](../user-guide/index.md),
which covers the analyst-facing client. If you only need to run
analyses against an existing server, start there. If you are
**building, extending, or embedding** the server side — driving the
ETL programmatically, calling the NLP resolver, registering custom
reports, or composing the `GE` facade in your own pipelines — this
is the right guide.

```{toctree}
:maxdepth: 1

ge-facade
database/index
etl
nlp
reports
```
