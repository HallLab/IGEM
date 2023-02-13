from igem.ge import db


def test_ge_db():
    x = db.ge_db()
    assert x == "success"


def test_ge_db_truncate_table():
    x = db.truncate_table(table="logs")
    assert x is True


def test_ge_db_delete_data_datasource():
    x = db.delete_data(table="datasource", datasource={"datasource__in": ["alfa"]})
    assert x is True


def test_ge_db_delete_data_connector():
    x = db.delete_data(table="connector", connector={"connector__in": ["alfa_con"]})
    assert x is True


def test_ge_db_delete_data_term():
    x = db.delete_data(table="term", term={"term__in": ["alfa_term"]})
    assert x is True


def test_ge_db_delete_data_wordterm_by_word():
    x = db.delete_data(table="wordterm", word={"word__in": ["beta"]})
    assert x is True


def test_ge_db_delete_data_wordterm_by_term():
    x = db.delete_data(table="wordterm", term={"term_id__term__in": ["alfa_term"]})
    assert x is True


def test_ge_db_delete_data_term_category():
    x = db.delete_data(
        table="term_category", term_category={"term_category__in": ["alfa_cat"]}
    )
    assert x is True


def test_ge_db_delete_data_term_group():
    x = db.delete_data(
        table="term_group", term_group={"term_group__in": ["alpa_group"]}
    )
    assert x is True


# No need backup
def test_ge_db_get_data_datasource():
    x = db.get_data(
        table="datasource",
        # datasource={'datasource__in': ['alfa']},
        path="/users/andrerico/dev",
        # columns=['id', 'datasource']
    )
    print(x)
    assert x == x


def test_ge_db_get_data_connector():
    x = db.get_data(
        table="connector",
        # connector={'connector__in': ['sdf']},
        # datasource={'datasource_id__datasource__in': ['alfa']},
        path="/users/andrerico/dev",
        # columns=['datasource', 'datasource_id']
    )
    assert x is True


def test_ge_db_get_data_ds_column():
    x = db.get_data(
        table="ds_column",
        path="/users/andrerico/dev",
    )
    assert x is True


# No need backup
def test_ge_db_get_data_term_category():
    x = db.get_data(
        table="term_category",
        path="/users/andrerico/dev",
    )
    assert x is True


# No need backup
def test_ge_db_get_data_term_group():
    x = db.get_data(
        table="term_group",
        path="/users/andrerico/dev",
    )
    assert x is True


# No need backup
def test_ge_db_get_data_prefix():
    x = db.get_data(
        table="prefix",
        path="/users/andrerico/dev",
    )
    assert x is True


def test_ge_db_get_data_term():
    x = db.get_data(
        table="term",
        term={"term": "chem:c112297"},
        term_group={"term_group_id__term_group": "environment"},
        path="/users/andrerico/dev",
    )
    assert x is True


def test_ge_db_get_data_wordterm():
    x = db.get_data(
        table="wordterm",
        term={"term_id__term": "chem:c112297"},
        path="/users/andrerico/dev",
    )
    assert x is True


def test_ge_db_get_data_termmap():
    x = db.get_data(
        table="termmap",
        # term={'term_id__term': 'chem:c112297'},
        path="/users/andrerico/dev",
    )
    assert x is True


def test_ge_db_get_data_wordmap():
    x = db.get_data(
        table="wordmap",
        path="/users/andrerico/dev",
    )
    assert x is True


# ---- get data to backup ----


def test_ge_db_get_data_connector_backup():
    x = db.get_data(
        table="connector",
        path="/users/andrerico/dev",
        columns=[
            "connector",
            "datasource_id__datasource",
            "update_ds",
            "source_path",
            "source_web",
            "source_compact",
            "source_file_name",
            "source_file_format",
            "source_file_sep",
            "source_file_skiprow",
            "target_file_name",
            "target_file_format",
            "description",
        ],
        columns_out=[
            "connector",
            "datasource",
            "update_ds",
            "source_path",
            "source_web",
            "source_compact",
            "source_file_name",
            "source_file_format",
            "source_file_sep",
            "source_file_skiprow",
            "target_file_name",
            "target_file_format",
            "description",
        ],
    )
    assert x is True


def test_ge_db_get_data_ds_column_backup():
    x = db.get_data(
        table="ds_column",
        path="/users/andrerico/dev",
        columns=[
            "connector_id__connector",
            "status",
            "column_number",
            "column_name",
            "pre_value",
            "single_word",
        ],
        columns_out=[
            "connector",
            "status",
            "column_number",
            "column_name",
            "pre_value",
            "single_word",
        ],
    )
    assert x is True


def test_ge_db_get_data_term_backup():
    x = db.get_data(
        table="term",
        path="/users/andrerico/dev",
        columns=[
            "term",
            "term_group_id__term_group",
            "term_category_id__term_category",
            "description",
        ],
        columns_out=["term", "term_group", "term_category", "description"],
    )
    assert x is True


def test_ge_db_get_data_wordterm_backup():
    x = db.get_data(
        table="wordterm",
        path="/users/andrerico/dev",
        columns=["term_id__term", "word", "status", "commute"],
        columns_out=["term", "word", "status", "commute"],
    )
    assert x is True


def test_ge_db_get_data_termmap_backup():
    x = db.get_data(
        table="termmap",
        path="/users/andrerico/dev",
        columns=[
            "ckey",
            "connector_id__connector",
            "term_1_id__term",
            "term_2_id__term",
            "qtd_links",
        ],
        columns_out=["ckey", "connector", "term_1", "term_2", "qtd_links"],
    )
    assert x is True


# ----  LOAD DATA ----


def test_ge_db_load_data_datasource():
    x = db.load_data(table="datasource", path="/users/andrerico/dev/datasource.csv")
    assert x is True


def test_ge_db_load_data_connector():
    x = db.load_data(table="connector", path="/users/andrerico/dev/connector.csv")
    assert x is True


def test_ge_db_load_data_ds_column():
    x = db.load_data(table="ds_column", path="/users/andrerico/dev/ds_column.csv")
    assert x is True


def test_ge_db_load_data_term_category():
    x = db.load_data(
        table="term_category", path="/users/andrerico/dev/term_category.csv"
    )
    assert x is True


def test_ge_db_load_data_term_group():
    x = db.load_data(table="term_group", path="/users/andrerico/dev/term_group.csv")
    assert x is True


def test_ge_db_load_data_term():
    x = db.load_data(table="term", path="/users/andrerico/dev/term.csv")
    assert x is True


def test_ge_db_load_data_prefix():
    x = db.load_data(table="prefix", path="/users/andrerico/dev/prefix.csv")
    assert x is True


def test_ge_db_load_data_wordterm():
    x = db.load_data(table="wordterm", path="/users/andrerico/dev/wordterm.csv")
    assert x is True


def test_ge_db_load_data_termmap():
    x = db.load_data(table="termmap", path="/users/andrerico/dev/termmap.csv")
    assert x is True


def test_ge_db_backup_datasource():
    x = db.backup(path_out="/users/andrerico/dev")
    assert x is True
