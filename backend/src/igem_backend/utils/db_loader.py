from __future__ import annotations

from importlib import import_module


def load_all_models() -> None:
    """Import all model modules so SQLAlchemy registers their tables."""
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


def bootstrap_models(engine) -> None:
    """One call to prepare all ORM models for the current engine."""
    load_all_models()
