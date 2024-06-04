from pathlib import Path

import pytest
from ge import db

from .test_base import GeTestBase


class GeDbGetTest(GeTestBase):
    def setUp(self) -> None:
        self.status_full_db = self.create_db()
        self.path = str(
            Path(__file__).parent / "test_data_files" / "sync"
        )
        return super().setUp()

    def test_ge_db_get_data_datasource(self):
        df = db.get_data(table="datasource")
        col = ['datasource', 'description', 'website', 'category']
        col_df = list(df.columns)
        assert col == col_df

    def test_ge_db_get_data_connector(self):
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

    def test_ge_db_get_data_ds_column(self):
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

    def test_ge_db_get_data_term_category(self):
        df = db.get_data(table="term_category")
        col = ['term_category', 'description']
        col_df = list(df.columns)
        assert col == col_df

    def test_ge_db_get_data_term_group(self):
        df = db.get_data(table="term_group")
        col = ['term_group', 'description']
        col_df = list(df.columns)
        assert col == col_df

    def test_ge_db_get_data_prefix(self):
        df = db.get_data(table="prefix")
        col = ['pre_value']
        col_df = list(df.columns)
        assert col == col_df

    def test_ge_db_get_data_term(self):
        df = db.get_data(table="term",)
        col = ['term', 'term_group', 'term_category', 'description']
        col_df = list(df.columns)
        assert col == col_df

    def test_ge_db_get_data_wordterm(self):
        df = db.get_data(table="wordterm",)
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

    def test_ge_db_get_data_termmap(self):
        df = db.get_data(
            table="termmap",
        )
        col = [
            'ckey',
            'connector',
            'term_1',
            'term_1__term',
            'term_2',
            'term_2__term',
            'qtd_links',
            ]
        col_df = list(df.columns)
        assert col == col_df

    # TODO: create test to other tables
    def test_ge_db_get_data_connector_select_columns(self):
        df = db.get_data(
            table="connector",
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


""" Sync ONLINE functions will truncate the DB to new data """
# Run all in a sigle test because the PK and FK integrid


class GeDbSyncTest(GeTestBase):
    def setUp(self) -> None:
        self.status_full_db = self.create_db()
        self.path = str(
            Path(__file__).parent / "test_data_files" / "sync"
        )
        return super().setUp()

    """ Sync OFFLINE functions will truncate the DB to new data """
    def test_ge_db_sync_datasource_offline(self):
        check = db.sync_db(table="datasource", source=self.path)
        assert check

    def test_ge_db_sync_connector_offline(self):
        check = db.sync_db(table="connector", source=self.path)
        assert check

    def test_ge_db_sync_prefixopc_offline(self):
        check = db.sync_db(table="prefixopc", source=self.path)
        assert check

    def test_ge_db_sync_dstcolumn_offline(self):
        check = db.sync_db(table="dstcolumn", source=self.path)
        assert check

    def test_ge_db_sync_termgroup_offline(self):
        check = db.sync_db(table="termgroup", source=self.path)
        assert check

    def test_ge_db_sync_termcategory_offline(self):
        check = db.sync_db(table="termcategory", source=self.path)
        assert check

    def test_ge_db_sync_term_offline(self):
        check = db.sync_db(table="term", source=self.path)
        assert check

    def test_ge_db_sync_wordterm_offline(self):
        check = db.sync_db(table="wordterm", source=self.path)
        assert check

    def test_ge_db_sync_termmap_offline(self):
        check = db.sync_db(table="termmap", source=self.path)
        assert check

    def test_ge_db_sync_wfcontrol_offline(self):
        check = db.sync_db(table="wfcontrol", source=self.path)
        assert check

    def test_ge_db_sync_snpgene_offline(self):
        check = db.sync_db(table="snpgene", source=self.path)
        assert check

    @pytest.mark.slow
    def test_ge_db_sync_all_online(self):
        check = db.sync_db(table="datasource")
        assert check
        check = db.sync_db(table="connector")
        assert check
        check = db.sync_db(table="prefixopc")
        assert check
        check = db.sync_db(table="dstcolumn")
        assert check
        check = db.sync_db(table="termgroup")
        assert check
        check = db.sync_db(table="termcategory")
        assert check
        check = db.sync_db(table="term")
        assert check
        check = db.sync_db(table="termmap")
        assert check
        check = db.sync_db(table="wfcontrol")
        assert check
        check = db.sync_db(table="snpgene")
        assert check
