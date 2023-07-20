from pathlib import Path

from ge import filter

from .test_base import GeTestBase


class GeTagTest(GeTestBase):
    def setUp(self) -> None:
        self.status_full_db = self.create_db()
        self.path = str(
            Path(__file__).parent / "test_data_files"
        )
        return super().setUp()

    def test_ge_tag_create(self):
        status, tag = filter.term_map(
            connector=['ctdcgint'],
            path_out=(self.path + "/results/terms_map_tag_create.csv"),
        )
        if status:
            v_tag = filter.create_tag(["ctdcgint"])
            assert tag in v_tag
            # assert "GE.db-TAG:" in v_tag
        else:
            # No data to check function
            assert tag == ''

    def test_ge_get_tag(self):
        status, tag = filter.term_map(
            connector=['ctdcgint'],
            path_out=(self.path + "/results/terms_map_tag_report.csv"),
        )
        if status:
            df = filter.get_tag(tag=tag)
            assert len(df) == 1
        else:
            assert tag == ''

    def test_ge_get_tag_data(self):
        status, tag = filter.term_map(
            connector=['ctdcgint'],
            path_out=(self.path + "/results/terms_map_tag_data.csv"),
        )
        if status:
            check = filter.get_tag_data(
                tag=tag,
                path=(self.path + "/results")
            )
            assert check
        else:
            # No data to check function
            assert tag == ''
