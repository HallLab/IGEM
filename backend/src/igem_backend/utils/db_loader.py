"""
Model registration helpers.

Two concerns:

1. **Declarative models** — registered by importing the modules that
   define them (the import side effect attaches each ``Base``
   subclass to ``Base.metadata``).

2. **Imperative tables** — five chromosome-partitioned variants
   tables whose shape depends on the dialect (composite PK on PG,
   simple PK on SQLite). These are registered by invoking
   ``map_*(engine, metadata)`` functions.

Call :func:`bootstrap_models` once per engine to ensure both kinds
of tables are present in ``Base.metadata`` before
``create_all`` / Alembic autogenerate / migration runs.
"""

from __future__ import annotations

from importlib import import_module
from typing import Callable, Tuple


def load_all_models() -> None:
    """Import all declarative model modules so SQLAlchemy registers them."""
    # Core
    import_module("igem_backend.modules.db.models.model_config")
    import_module("igem_backend.modules.db.models.model_etl")
    import_module("igem_backend.modules.db.models.model_entities")
    # Domain — Onda 1
    import_module("igem_backend.modules.db.models.model_genes")
    import_module("igem_backend.modules.db.models.model_variants")
    import_module("igem_backend.modules.db.models.model_diseases")
    import_module("igem_backend.modules.db.models.model_chemicals")
    import_module("igem_backend.modules.db.models.model_pathways")
    import_module("igem_backend.modules.db.models.model_proteins")
    import_module("igem_backend.modules.db.models.model_go")
    import_module("igem_backend.modules.db.models.model_phenotypes")
    import_module("igem_backend.modules.db.models.model_anatomy")
    # NLP
    import_module("igem_backend.modules.db.models.model_nlp")


def register_imperative_tables(engine) -> None:
    """
    Register dialect-specific ``Table()`` objects into ``Base.metadata``.

    The five chromosome-partitioned variant tables cannot use declarative
    syntax because ``PARTITION BY LIST`` is PostgreSQL-only and cannot be
    expressed in SQLAlchemy 2.x metadata. Each ``map_*`` function returns
    a ``Table`` shaped for the active dialect:

    - **PostgreSQL**: composite PK ``(chromosome, variant_id)`` with an
      Identity column. The actual partitioning DDL ships in
      :mod:`igem_backend.modules.db.core_ddl`; this Table is for
      ORM/Core query usage only.
    - **SQLite**: plain table, ``variant_id`` as autoincrement PK.

    Idempotent: stale Table definitions are removed before re-registering,
    so this function is safe to call multiple times against different
    engines (e.g. during tests that switch dialects).
    """
    from igem_backend.modules.db.base import Base
    from igem_backend.modules.db.models.model_variants import (
        map_variant_effect_predictions,
        map_variant_gene_regulatory_evidence,
        map_variant_masters,
        map_variant_molecular_effects,
        map_variant_regulatory_elements,
    )

    registry: list[Tuple[str, Callable]] = [
        ("variant_masters", map_variant_masters),
        ("variant_molecular_effects", map_variant_molecular_effects),
        ("variant_effect_predictions", map_variant_effect_predictions),
        ("variant_regulatory_elements", map_variant_regulatory_elements),
        (
            "variant_gene_regulatory_evidence",
            map_variant_gene_regulatory_evidence,
        ),
    ]

    # Remove stale definitions before re-registering (necessary because
    # bootstrap_models can be called multiple times against engines of
    # different dialects — e.g. tests).
    for table_name, _ in registry:
        existing = Base.metadata.tables.get(table_name)
        if existing is not None:
            Base.metadata.remove(existing)

    for table_name, map_fn in registry:
        tbl = map_fn(engine, Base.metadata)
        if tbl.name != table_name:
            raise RuntimeError(
                f"Imperative table mapper returned unexpected name: "
                f"expected='{table_name}' got='{tbl.name}' "
                f"from {map_fn.__name__}"
            )


def bootstrap_models(engine) -> None:
    """One call to prepare all ORM models for the current engine."""
    load_all_models()
    register_imperative_tables(engine)
