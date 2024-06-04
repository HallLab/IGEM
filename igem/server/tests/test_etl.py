from pathlib import Path

from django.conf import settings
from ge.models import Connector, TermMap, WFControl
from ge.tests.test_base import GeTestBase
from ge.utils import start_logger
from server.etl import (  # noqa E501
    collect,
    get_connectors,
    get_workflow,
    map,
    prepare,
    reduce,
)


class ServerETLTest(GeTestBase):
    def setUp(self) -> None:
        self.status_full_db = self.create_db()
        self.path = str(
            Path(__file__).parent / "test_data_files"
        )
        return super().setUp()

    def test_get_connector(self):
        log = start_logger(__name__)
        v_conn = "test_ctd"
        qs_queryset = get_connectors(v_conn, log)
        assert len(qs_queryset) == 1

    def test_get_workflow(self):
        log = start_logger(__name__)
        v_conn = "test_ctd"
        qs_queryset = get_connectors(v_conn, log)
        qs = qs_queryset.first()
        process = "collect"
        qs_queryset, v_first_load = get_workflow(qs, process, log)
        assert len(qs_queryset) == 1
        assert v_first_load is True

    def test_server_etl_kegg(self):
        # Set kegg file path
        qs = Connector.objects.get(connector='test_kegg')
        qs.source_path = str(settings.BASE_DIR) + qs.source_path
        qs.save()
        v_collect = collect(connector="test_kegg")
        assert v_collect is True
        v_prepare = prepare(connector="test_kegg")
        assert v_prepare is True
        y = map(connector="test_kegg")
        assert y is True
        y = reduce(connector="test_kegg")
        assert y is True
        # Check if all map was created
        qry = TermMap.objects.filter(connector_id=103)
        assert len(qry) == 6
        # Check if all four process finished on workprocess control
        qry = WFControl.objects.get(connector_id=103)
        assert qry.status == 'c'
        # TODO: ERROR CODE: database table is locked
        # This error is because the MP with few date

    def test_server_etl_ctd(self):
        # TODO: Implement Order or dependency to sprit this tests
        v_collect = collect(connector="test_ctd")
        assert v_collect is True
        v_prepare = prepare(connector="test_ctd")
        assert v_prepare is True
        y = map(connector="test_ctd")
        assert y is True
        y = reduce(connector="test_ctd")
        assert y is True
        # Check if all map was created
        qry = TermMap.objects.filter(connector_id=100)
        assert len(qry) == 15
        # Check if all four process finished on workprocess control
        qry = WFControl.objects.get(connector_id=100)
        assert qry.status == 'c'
