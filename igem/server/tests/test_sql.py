from pathlib import Path

from ge.tests.test_base import GeTestBase
from server import sql


class ServerDbTest(GeTestBase):
    def setUp(self) -> None:
        self.status_full_db = self.create_db()
        self.path = str(
            Path(__file__).parent / "test_data_files"
        )
        return super().setUp()

# # ---- load_data and delete_data functions tests ----
    def test_ge_db_load_and_delete(self):
        # DATASOURCE
        load_alpha_ds = sql.load_data(
            table="datasource",
            path=(self.path + "/datasource_alpha.csv")
            )
        assert load_alpha_ds is False

        # CONNECTOR
        load_alpha_conn = sql.load_data(
            table="connector",
            path=(self.path + "/connector_alpha.csv")
            )
        assert load_alpha_conn is True

        # PREFIX
        load_alpha_prefix = sql.load_data(
            table="prefix",
            path=(self.path + "/prefix_alpha.csv")
            )
        assert load_alpha_prefix is True

        # DSTCOLUMN
        load_alpha_dst = sql.load_data(
            table="ds_column",
            path=(self.path + "/dstcolumn_apha.csv")
            )
        assert load_alpha_dst is True

        # GROUP TERM
        load_alpha_grp = sql.load_data(
            table="term_group",
            path=(self.path + "/termgroup_alpha.csv")
            )
        assert load_alpha_grp is True

        # CATEGORY TERM
        load_alpha_cat = sql.load_data(
            table="term_category",
            path=(self.path + "/termcategory_alpha.csv")
            )
        assert load_alpha_cat is True

        # TERM
        load_alpha_term = sql.load_data(
            table="term",
            path=(self.path + "/term_alpha.csv")
            )
        assert load_alpha_term is True

        # WORDTERM
        load_alpha_wordterm = sql.load_data(
            table="wordterm",
            path=(self.path + "/wordterm_alpha.csv")
            )
        assert load_alpha_wordterm is True

        # TERMMAP
        load_alpha_termmap = sql.load_data(
            table="termmap",
            path=(self.path + "/termmap_alpha.csv")
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

        # PREFIX
        del_alpha_prefix = sql.delete_data(
            table="prefix",
            prefix={"prefix__in": ["alpha:"]}
            )
        assert del_alpha_prefix is True

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

    # BACKUPS
    def test_ge_db_backup(self):
        x = sql.backup(path_out=self.path + "/backup")
        assert x is True

    def test_ge_db_restore(self):
        x = sql.restore(table="all", source=self.path + "/backup")
        assert x is True

    # TRUNCATE
    def test_ge_db_truncate(self):
        x = sql.truncate_table(table="all", vacuum=False)
        assert x is True
