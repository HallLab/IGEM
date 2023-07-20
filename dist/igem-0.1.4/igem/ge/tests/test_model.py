from django.core.exceptions import ValidationError
from parameterized import parameterized

from .test_base import GeTestBase


class GeModelTest(GeTestBase):
    def setUp(self) -> None:
        self.datasource = self.make_datasource()
        self.connector = self.make_connector(datasource=self.datasource)
        self.prefix = self.make_prefixopc()
        self.dstcolumn = self.make_dstcolumn(
            connector=self.connector,
            pre_value=self.prefix
            )
        self.wfcontrol = self.make_wfcontrol(connector=self.connector)
        self.logs = self.make_logs()
        self.term_group = self.make_termgroup()
        self.term_category = self.make_termcategory()
        self.term = self.make_term(
            term_group=self.term_group,
            term_category=self.term_category
            )
        self.wordterm = self.make_wordterm(term=self.term)
        self.wordmap = self.make_wordmap(
            connector=self.connector,
            datasource=self.datasource,
            term_1=self.term,
            term_2=self.term
            )
        self.termmap = self.make_termmap(
            connector=self.connector,
            term_1=self.term,
            term_2=self.term
            )
        return super().setUp()

    # Datasource fields test
    @parameterized.expand([
        ('datasource', 20),
        ('description', 200),
        ('category', 20),
        ('website', 200),
    ])
    def test_datasource_fields_max_length(self, field, max_length):
        setattr(self.datasource, field, 'A' * (max_length + 1))
        with self.assertRaises(ValidationError):
            self.datasource.full_clean()

    # Connector fields test
    @parameterized.expand([
        ('connector', 20),
        ('description', 200),
        ('source_path', 300),
        ('source_file_name', 200),
        ('source_file_format', 200),
        ('source_file_sep', 3),
        ('target_file_name', 200),
        ('target_file_format', 200),
    ])
    def test_connector_fields_max_length(self, field, max_length):
        setattr(self.connector, field, 'A' * (max_length + 1))
        with self.assertRaises(ValidationError):
            self.connector.full_clean()

    # PrefixOpc fields test
    @parameterized.expand([
        ('pre_value', 5),
    ])
    def test_prefix_fields_max_length(self, field, max_length):
        setattr(self.prefix, field, 'A' * (max_length + 1))
        with self.assertRaises(ValidationError):
            self.prefix.full_clean()

    # Workflow fields test
    @parameterized.expand([
        ('source_file_version', 500),
        ('igem_version', 15),
        ('status', 1),
    ])
    def test_wfcontrol_fields_max_length(self, field, max_length):
        setattr(self.wfcontrol, field, 'A' * (max_length + 1))
        with self.assertRaises(ValidationError):
            self.wfcontrol.full_clean()

    # Workflow fields test
    @parameterized.expand([
        ('process', 65),
        ('igem_version', 15),
        ('status', 1),
    ])
    def test_logs_fields_max_length(self, field, max_length):
        setattr(self.logs, field, 'A' * (max_length + 1))
        with self.assertRaises(ValidationError):
            self.logs.full_clean()

    # Term group fields test
    @parameterized.expand([
        ('term_group', 20),
        ('description', 200),
    ])
    def test_termgroup_fields_max_length(self, field, max_length):
        setattr(self.term_group, field, 'A' * (max_length + 1))
        with self.assertRaises(ValidationError):
            self.term_group.full_clean()

    # Term category fields test
    @parameterized.expand([
        ('term_category', 20),
        ('description', 200),
    ])
    def test_termcategory_fields_max_length(self, field, max_length):
        setattr(self.term_category, field, 'A' * (max_length + 1))
        with self.assertRaises(ValidationError):
            self.term_category.full_clean()

    # Term fields test
    @parameterized.expand([
        ('term', 40),
        ('description', 400),
    ])
    def test_term_fields_max_length(self, field, max_length):
        setattr(self.term, field, 'A' * (max_length + 1))
        with self.assertRaises(ValidationError):
            self.term.full_clean()

    # WordTerm fields test
    @parameterized.expand([
        ('word', 400),
    ])
    def test_wordterm_fields_max_length(self, field, max_length):
        setattr(self.wordterm, field, 'A' * (max_length + 1))
        with self.assertRaises(ValidationError):
            self.wordterm.full_clean()
