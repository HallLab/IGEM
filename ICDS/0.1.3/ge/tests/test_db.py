from pathlib import Path

import pytest

from igem.ge import db


# Parameter file path testing
@pytest.fixture
def parameters():
    # define files path
    TESTS_PATH = Path(__file__).parent
    DATA_PATH = TESTS_PATH / "test_data_files" / "sync"
    return str(DATA_PATH)


def test_ge_db_get_data_datasource():
    df = db.get_data(table="datasource")
    col = ['datasource', 'description', 'website', 'category']
    col_df = list(df.columns)
    assert col == col_df


def test_ge_db_get_data_connector():
    df = db.get_data(table="connector")
    col = [
        'connector',
        'datasource',
        'datasource_id__datasource',
        'update_ds',
        'source_path',
        'source_web',
        'source_compact',
        'source_file_name',
        'source_file_format',
        'source_file_sep',
        'source_file_skiprow',
        'target_file_name',
        'target_file_format',
        'description'
        ]
    col_df = list(df.columns)
    assert col == col_df


def test_ge_db_get_data_ds_column():
    df = db.get_data(table="ds_column")
    col = [
        'connector',
        'connector_id__connector',
        'status',
        'column_number',
        'column_name',
        'pre_value',
        'single_word'
        ]
    col_df = list(df.columns)
    assert col == col_df


def test_ge_db_get_data_term_category():
    df = db.get_data(table="term_category")
    col = ['term_category', 'description']
    col_df = list(df.columns)
    assert col == col_df


def test_ge_db_get_data_term_group():
    df = db.get_data(table="term_group")
    col = ['term_group', 'description']
    col_df = list(df.columns)
    assert col == col_df


def test_ge_db_get_data_prefix():
    df = db.get_data(table="prefix")
    col = ['pre_value']
    col_df = list(df.columns)
    assert col == col_df


def test_ge_db_get_data_term():
    df = db.get_data(
        table="term",
        term={"term": "chem:c112297"},
        # term_group={"term_group_id__term_group": "environment"},
    )
    col = ['term', 'term_group', 'term_category', 'description']
    col_df = list(df.columns)
    assert col == col_df


def test_ge_db_get_data_wordterm():
    df = db.get_data(
        table="wordterm",
        term={"term_id__term": "chem:c112297"},
    )
    col = [
        'status',
        'commute',
        'word',
        'term',
        'term_id__term',
        'term_id__term_category_id__term_category',
        'term_id__term_group_id__term_group'
        ]
    col_df = list(df.columns)
    assert col == col_df


# Define better parameter to avoid return null or all table
# def test_ge_db_get_data_termmap():
#     df = db.get_data(
#         table="termmap",
#     )
#     col = [
#         ]
#     col_df = list(df.columns)
#     assert col == col_df


# Define better parameter to avoid return null or all table
# def test_ge_db_get_data_wordmap():
#     df = db.get_data(
#         table="wordmap",
#     )
#     col = [
#         ]
#     col_df = list(df.columns)
#     assert col == col_df


def test_ge_db_get_data_connector_select_columns():
    df = db.get_data(
        table="connector",
        # path="/users/andrerico/dev",
        columns=[
            "connector",
            "datasource_id__datasource"
        ],
        columns_out=[
            "connector",
            "datasource"
        ],
    )
    col = [
        "connector",
        "datasource"
        ]
    col_df = list(df.columns)
    assert col == col_df


""" Sync functions will truncate the DB to new data"""


def test_ge_db_sync_datasource_online():
    check = db.sync_db(table="datasource")
    assert check


def test_ge_db_sync_connector_online():
    check = db.sync_db(table="connector")
    assert check


def test_ge_db_sync_prefixopc_online():
    check = db.sync_db(table="prefixopc")
    assert check


def test_ge_db_sync_dstcolumn_online():
    check = db.sync_db(table="dstcolumn")
    assert check


def test_ge_db_sync_termgroup_online():
    check = db.sync_db(table="termgroup")
    assert check


def test_ge_db_sync_termcategory_online():
    check = db.sync_db(table="termcategory")
    assert check


def test_ge_db_sync_term_online():
    check = db.sync_db(table="term")
    assert check


def test_ge_db_sync_termmap_online():
    check = db.sync_db(table="termmap")
    assert check


def test_ge_db_sync_wfcontrol_online():
    check = db.sync_db(table="wfcontrol")
    assert check


def test_ge_db_sync_snpgene_online():
    check = db.sync_db(table="snpgene")
    assert check


def test_ge_db_sync_datasource_offline(parameters):
    check = db.sync_db(table="datasource", source=str(parameters))
    assert check


def test_ge_db_sync_connector_offline(parameters):
    check = db.sync_db(table="connector", source=str(parameters))
    assert check


def test_ge_db_sync_prefixopc_offline(parameters):
    check = db.sync_db(table="prefixopc", source=str(parameters))
    assert check


def test_ge_db_sync_dstcolumn_offline(parameters):
    check = db.sync_db(table="dstcolumn", source=str(parameters))
    assert check


def test_ge_db_sync_termgroup_offline(parameters):
    check = db.sync_db(table="termgroup", source=str(parameters))
    assert check


def test_ge_db_sync_termcategory_offline(parameters):
    check = db.sync_db(table="termcategory", source=str(parameters))
    assert check


def test_ge_db_sync_term_offline(parameters):
    check = db.sync_db(table="term", source=str(parameters))
    assert check


def test_ge_db_sync_termmap_offline(parameters):
    check = db.sync_db(table="termmap", source=str(parameters))
    assert check


def test_ge_db_sync_wfcontrol_offline(parameters):
    check = db.sync_db(table="wfcontrol", source=str(parameters))
    assert check


def test_ge_db_sync_snpgene_offline(parameters):
    check = db.sync_db(table="snpgene", source=str(parameters))
    assert check
