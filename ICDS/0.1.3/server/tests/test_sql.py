from pathlib import Path

import pytest

from igem.server import sql


# Parameter file path testing
@pytest.fixture
def parameters():
    v_path = Path(__file__).parent / "test_data_files"  # noqa E501
    return str(v_path)


# ---- load_data and delete_data functions tests ----

def test_ge_db_load_delete(parameters):
    # Load data

    # DATASOURCE
    load_alpha_ds = sql.load_data(
        table="datasource",
        path=(parameters + "/datasource_alpha.csv")
        )
    assert load_alpha_ds is True

    # CONNECTOR
    load_alpha_conn = sql.load_data(
        table="connector",
        path=(parameters + "/connector_alpha.csv")
        )
    assert load_alpha_conn is True

    # DSTCOLUMN
    load_alpha_dst = sql.load_data(
        table="ds_column",
        path=(parameters + "/dstcolumn_apha.csv")
        )
    assert load_alpha_dst is True

    # GROUP TERM
    load_alpha_grp = sql.load_data(
        table="term_group",
        path=(parameters + "/termgroup_alpha.csv")
        )
    assert load_alpha_grp is True

    # CATEGORY TERM
    load_alpha_cat = sql.load_data(
        table="term_category",
        path=(parameters + "/termcategory_alpha.csv")
        )
    assert load_alpha_cat is True

    # TERM
    load_alpha_term = sql.load_data(
        table="term",
        path=(parameters + "/term_alpha.csv")
        )
    assert load_alpha_term is True

    # WORDTERM
    load_alpha_wordterm = sql.load_data(
        table="wordterm",
        path=(parameters + "/wordterm_alpha.csv")
        )
    assert load_alpha_wordterm is True

    # TERMMAP
    load_alpha_termmap = sql.load_data(
        table="termmap",
        path=(parameters + "/termmap_alpha.csv")
        )
    assert load_alpha_termmap is True

    # Delete data

    # TERMMAP
    del_alpha_termmap = sql.delete_data(
        table="termmap",
        term={"term_1_id__term__in": ["alpha:000001"]}
        )
    assert del_alpha_termmap is True

    # WORDTERM
    del_alpha_wordterm = sql.delete_data(
        table="wordterm",
        word={"word__in": ["alpha test of wordterm"]}
        )
    assert del_alpha_wordterm is True

    # TERM
    del_alpha_term = sql.delete_data(
        table="term",
        term={"term__in": ["alpha:000001"]}
        )
    assert del_alpha_term is True

    # GROUP TERM
    del_alpha_grp = sql.delete_data(
        table="term_group",
        term_group={"term_group__in": ["alpha"]}
        )
    assert del_alpha_grp is True

    # CATEGORY TERM
    del_alpha_cat = sql.delete_data(
        table="term_category",
        term_category={"term_category__in": ["alpha"]}
        )
    assert del_alpha_cat is True

    # DSTCOLUMN
    del_alpha_dst = sql.delete_data(
        table="ds_column",
        connector={"connector_id__connector__in": ["alpha_conn"]}
        )
    assert del_alpha_dst is True

    # CONNECTOR
    del_alpha_conn = sql.delete_data(
        table="connector",
        connector={"connector__in": ["alpha_conn"]}
        )
    assert del_alpha_conn is True

    # DATASOURCE
    del_alpha_ds = sql.delete_data(
        table="datasource",
        datasource={"datasource__in": ["alpha"]}
        )
    assert del_alpha_ds is True


def test_ge_db_load_data_prefix(parameters):
    # PREFIX
    load_alpha_prefix = sql.load_data(
        table="prefix",
        path=(parameters + "/prefix_alpha.csv")
        )
    assert load_alpha_prefix is True


def test_ge_db_delete_data_prefix():
    # PREFIX
    del_alpha_prefix = sql.delete_data(
        table="prefix",
        prefix={"prefix__in": ["alpha:"]}
        )
    assert del_alpha_prefix is True


# BACKUPS
def test_ge_db_backup(parameters):
    x = sql.backup(path_out=parameters + "/backup")
    assert x is True


def test_ge_db_restore(parameters):
    x = sql.restore(table="all", source=parameters + "/backup")
    assert x is True


# TRUNCATE
def test_ge_db_truncate():
    x = sql.truncate_table(table="all")
    assert x is True
