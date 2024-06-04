from pathlib import Path

from django.test import TestCase
from ge import db
from ge.models import (
    Connector,
    Datasource,
    DSTColumn,
    Logs,
    PrefixOpc,
    Term,
    TermCategory,
    TermGroup,
    TermMap,
    WFControl,
    WordMap,
    WordTerm,
)


class GeMixin:
    # Create a full db
    def create_db(self):
        DATA_PATH = str(Path(__file__).parent / "test_data_files" / "sync")
        status = db.sync_db(table="all", source=DATA_PATH)
        return status

    # Create individual fields
    def make_datasource(
        self,
        datasource='hall',
        description='Datasource test from Hall Github',
        category='test',
        website='https://www.hall-lab.org/'
    ):
        return Datasource.objects.create(
            datasource=datasource,
            description=description,
            category=category,
            website=website,
        )

    def make_connector(
        self,
        connector='hall-con01',
        datasource=None,
        description='Connector Hall 01',
        update_ds=True,
        source_path='www.con',
        source_web=True,
        source_compact=True,
        source_file_name='File_name.gz',
        source_file_format='gz',
        source_file_sep=',',
        source_file_skiprow=1,
        target_file_name='hall_data.csv',
        target_file_format='csv',
        target_file_keep=False,
    ):
        if datasource is None:
            datasource = self.make_datasource()

        return Connector.objects.create(
            connector=connector,
            datasource=datasource,
            description=description,
            update_ds=update_ds,
            source_path=source_path,
            source_web=source_web,
            source_compact=source_compact,
            source_file_name=source_file_name,
            source_file_format=source_file_format,
            source_file_sep=source_file_sep,
            source_file_skiprow=source_file_skiprow,
            target_file_name=target_file_name,
            target_file_format=target_file_format,
            target_file_keep=target_file_keep,
        )

    def make_prefixopc(
        self,
        pre_value='test:',
    ):
        return PrefixOpc.objects.create(
            pre_value=pre_value,
        )

    def make_dstcolumn(
        self,
        connector=None,
        status=True,
        column_number=1,
        column_name='test Column',
        pre_value=None,
        single_word=True,
    ):
        if connector is None:
            connector = self.make_connector()
        if pre_value is None:
            pre_value = self.make_prefixopc()

        return DSTColumn.objects.create(
            connector=connector,
            status=status,
            column_number=column_number,
            column_name=column_name,
            pre_value=pre_value,
            single_word=single_word,
        )

    def make_wfcontrol(
        self,
        connector=None,
        last_update='2023-07-05',
        source_file_version='version-test-0',
        source_file_size=100,
        target_file_size=100,
        chk_collect=True,
        chk_prepare=False,
        chk_map=False,
        chk_reduce=False,
        igem_version='0.1.3',
        status='w',
        time_collect=150,
        time_prepare=0,
        time_map=0,
        time_reduce=0,
        row_collect=15015,
        row_prepare=0,
        row_map=0,
        row_reduce=0,
    ):
        if connector is None:
            connector = self.make_connector()

        return WFControl.objects.create(
            connector=connector,
            last_update=last_update,
            source_file_version=source_file_version,
            source_file_size=source_file_size,
            target_file_size=target_file_size,
            chk_collect=chk_collect,
            chk_prepare=chk_prepare,
            chk_map=chk_map,
            chk_reduce=chk_reduce,
            igem_version=igem_version,
            status=status,
            time_collect=time_collect,
            time_prepare=time_prepare,
            time_map=time_map,
            time_reduce=time_reduce,
            row_collect=row_collect,
            row_prepare=row_prepare,
            row_map=row_map,
            row_reduce=row_reduce,
            )

    def make_logs(
        self,
        process='process-test',
        igem_version='0.1.3',
        created_at='2023-07-05',
        status='s',
        description='log create from test in pytest',
    ):
        return Logs.objects.create(
            process=process,
            igem_version=igem_version,
            created_at=created_at,
            status=status,
            description=description,
        )

    def make_termgroup(
        self,
        term_group='group_test',
        description='Group of test created by pytest'
    ):
        return TermGroup.objects.create(
            term_group=term_group,
            description=description,
        )

    def make_termcategory(
        self,
        term_category='category_test',
        description='Category of test created by pytest'
    ):
        return TermCategory.objects.create(
            term_category=term_category,
            description=description,
        )

    def make_term(
        self,
        term='term_test',
        description='Term test created by pytest',
        term_group=None,
        term_category=None,
    ):
        if term_group is None:
            term_group = self.make_termgroup()
        if term_category is None:
            term_category = self.make_termcategory()
        return Term.objects.create(
            term=term,
            description=description,
            term_group=term_group,
            term_category=term_category,
        )

    def make_wordterm(
        self,
        word='test word',
        term=None,
        status=True,
        commute=True,
    ):
        if term is None:
            term = self.make_term()
        return WordTerm.objects.create(
            word=word,
            term=term,
            status=status,
            commute=commute,
        )

    def make_wordmap(
        self,
        cword='1-1',
        datasource=None,
        connector=None,
        term_1=None,
        term_2=None,
        word_1='word 1',
        word_2='word 2',
        qtd_links=30,
    ):
        if connector is None:
            connector = self.make_connector()
            datasource = connector.datasource
        if term_1 is None:
            term_1 = self.make_term(term='term_test_01')
        if term_2 is None:
            term_2 = self.make_term(term='term_test_02')

        return WordMap.objects.create(
            cword=cword,
            datasource=datasource,
            connector=connector,
            term_1=term_1,
            term_2=term_2,
            word_1=word_1,
            word_2=word_2,
            qtd_links=qtd_links,
        )

    def make_termmap(
        self,
        ckey='1-1',
        connector=None,
        term_1=None,
        term_2=None,
        qtd_links=100,
    ):
        if connector is None:
            connector = self.make_connector()
        if term_1 is None:
            term_1 = self.make_term(term='term_test_01')
        if term_2 is None:
            term_2 = self.make_term(term='term_test_02')
        return TermMap.objects.create(
            ckey=ckey,
            connector=connector,
            term_1=term_1,
            term_2=term_2,
            qtd_links=qtd_links,
        )


class GeTestBase(TestCase, GeMixin):
    def setUp(self) -> None:
        return super().setUp()
