from __future__ import annotations

from importlib import import_module


def load_all_models() -> None:
    """Import all model modules so SQLAlchemy registers their tables."""
    import_module("igem_backend.modules.db.models.model_config")
    import_module("igem_backend.modules.db.models.model_etl")
    import_module("igem_backend.modules.db.models.model_entities")


def bootstrap_models(engine) -> None:
    """One call to prepare all ORM models for the current engine."""
    load_all_models()
