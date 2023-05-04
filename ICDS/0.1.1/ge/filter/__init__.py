"""
ge.filter
=====

Query functions with filters in the GE.db knowledge base.

    .. autofunction:: gene_exposome
    .. autofunction:: parameters_file
    .. autofunction:: snp_exposome
    .. autofunction:: term_map
    .. autofunction:: word_map
"""


from .convert import word_to_term
from .filters import (  # noqa E501
    gedb_tag,
    gene_exposome,
    parameters_file,
    snp_exposome,
    term_map,
    word_map,
)
from .tag import get_tag, get_tag_data

__all__ = [
    "parameters_file",
    "word_map",
    "term_map",
    "word_to_term",
    "gene_exposome",
    "snp_exposome",
    "gedb_tag",
    'get_tag',
    "get_tag_data",
]
