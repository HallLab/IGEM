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
    gene_exposome,
    parameters_file,
    snp_exposome,
    term_map,
    word_map,
)
from .genes import search_gene_map_data
from .tag import create_tag, get_tag, get_tag_data

__all__ = [
    "parameters_file",
    "word_map",
    "term_map",
    "word_to_term",
    "gene_exposome",
    "snp_exposome",
    'get_tag',
    "get_tag_data",
    "create_tag",
    "search_gene_map_data"
]
