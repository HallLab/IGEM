"""
server.etl
=====

Extraction, transformation and loading functions from external databases to
the IGEM system in GE.db.

    .. autofunction:: collect
    .. autofunction:: prepare
    .. autofunction:: reduce
    .. autofunction:: map
"""
# flake8: noqa

from .collect import collect
from .extractors import *
from .map import map
from .prepare import prepare
from .reduce import reduce
from .utils import clean_folder, get_connectors, get_workflow

__all__ = ["collect", "prepare", "reduce", "map", "get_connectors", "get_workflow"]
