from pathlib import Path

import pytest

from igem.ge import filter


# Parameter file path testing
@pytest.fixture
def parameters():
    v_path = Path(__file__).parent / "test_data_files"  # noqa E501

    return str(v_path)


# Test filters on TermMap table
def test_ge_filter_terms_map_byfile(parameters):
    y, tag = filter.term_map(
        path_in=(parameters + "/parameters_test.csv"),
        path_out=(parameters + "/results/terms_map_byfile.csv"),
    )
    assert y is True


def test_ge_filter_terms_map_byargs():
    y = filter.term_map(term=["gene:246126"])
    assert not y.empty


# Tests filters on gene exposome layout
def test_ge_filter_gene_exposome_byfile(parameters):
    y, tag = filter.gene_exposome(
        path_in=(parameters + "/parameters_test.csv"),
        path_out=(parameters + "/results/gene_exposome_byfile.csv"),
    )
    assert y is True


def test_ge_filter_gene_exposome_byargs():
    y = filter.gene_exposome(term=["gene:246126"])
    assert not y.empty


# Tests filters on snp exposome layout
def test_ge_filter_snp_exposome_byfile(parameters):
    y, tag = filter.snp_exposome(
        path_in=(parameters + "/parameters_test.csv"),
        path_out=(parameters + "/results/snp_exposome_byfile.csv"),
    )
    assert y is True


def test_ge_filter_snp_exposome_byargs_df():
    y = filter.snp_exposome(term=["gene:246126"])
    assert not y.empty


def test_ge_filter_snp_exposome_byargs_csv(parameters):
    y, tag = filter.snp_exposome(
        term=["gene:246126"],
        path_out=(parameters + "/results/snp_exposome_byargs_csv.csv"),
    )
    assert y is True


# Tests filters on snp exposome layout
def test_ge_filter_word_map_byfile(parameters):
    y = filter.word_map(
        path_in=(parameters + "/parameters_test.csv"),
        path_out=(parameters + "/results/word_map_byfile.csv"),
    )
    assert y is True


# Tests on Serch terms on words strings
def test_ge_filter_convert(parameters):
    y = filter.word_to_term(
        path=(parameters + "/convert.csv"),
    )
    assert y is True


# Criate Parameter File to input arguments
def test_ge_filter_create_parameter_file(parameters):
    y = filter.parameters_file(
        path=(parameters),
    )
    assert y is True
