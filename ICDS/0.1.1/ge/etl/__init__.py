"""
ge.etl
=====

Extraction, transformation and loading functions from external databases to
the IGEM system in GE.db.

    .. autofunction:: collect
    .. autofunction:: prepare
    .. autofunction:: reduce
    .. autofunction:: map
"""


from .collect import collect
from .map import map
from .prepare import prepare
from .reduce import reduce

__all__ = ["collect", "prepare", "reduce", "map"]
