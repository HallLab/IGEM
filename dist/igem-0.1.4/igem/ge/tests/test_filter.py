from pathlib import Path

from ge import filter

from .test_base import GeTestBase


class GeFilterTest(GeTestBase):
    def setUp(self) -> None:
        self.status_full_db = self.create_db()
        self.path = str(
            Path(__file__).parent / "test_data_files"
        )
        return super().setUp()

    # Test filters on TermMap table
    def test_ge_filter_terms_map_byfile(self):
        y, tag = filter.term_map(
            path_in=(self.path + "/parameters_test.csv"),
            path_out=(self.path + "/results/terms_map_byfile.csv"),
        )
        assert y is True

    def test_ge_filter_terms_map_byargs(self):
        y = filter.term_map(term=["gene:1"])
        assert not y.empty

    # Tests filters on gene exposome layout
    def test_ge_filter_gene_exposome_byfile(self):
        y, tag = filter.gene_exposome(
            path_in=(self.path + "/parameters_test.csv"),
            path_out=(self.path + "/results/gene_exposome_byfile.csv"),
        )
        assert y is True

    def test_ge_filter_gene_exposome_byargs(self):
        y = filter.gene_exposome(term=["gene:1"])
        assert not y.empty

    # Tests filters on snp exposome layout
    def test_ge_filter_snp_exposome_byfile(self):
        y, tag = filter.snp_exposome(
            path_in=(self.path + "/parameters_test.csv"),
            path_out=(self.path + "/results/snp_exposome_byfile.csv"),
        )
        assert y is True

    def test_ge_filter_snp_exposome_byargs_df(self):
        y = filter.snp_exposome(term=["gene:1"])
        assert not y.empty

    def test_ge_filter_snp_exposome_byargs_csv(self):
        y, tag = filter.snp_exposome(
            term=["gene:1"],
            path_out=(self.path + "/results/snp_exposome_byargs_csv.csv"),
        )
        assert y is True

    # TODO: load a fake record to WordMap just to check the function
    # # Tests filters on WordMap
    # def test_ge_filter_word_map_byfile(self):
    #     y = filter.word_map(
    #         path_in=(self.path + "/parameters_test.csv"),
    #         path_out=(self.path + "/results/word_map_byfile.csv"),
    #     )
    #     assert y is True

    # Tests on Serch terms on words strings
    def test_ge_filter_convert(self):
        y = filter.word_to_term(
            path=(self.path + "/convert.csv"),
        )
        assert y is True

    # Criate Parameter File to input arguments
    def test_ge_filter_create_parameter_file(self):
        y = filter.parameters_file(
            path=(self.path),
        )
        assert y is True
