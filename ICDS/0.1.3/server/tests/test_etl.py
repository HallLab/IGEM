from igem.server import etl

# TODO: Create a fake connector


def test_server_etl_collect():
    y = etl.collect(connector="ctdesassoc")
    assert y is True


def test_server_etl_prepare():
    y = etl.prepare(connector="ctdesassoc")
    assert y is True


def test_server_etl_map():
    y = etl.map(connector="ctdesassoc")
    assert y is True


def test_server_etl_reduce():
    y = etl.reduce(connector="ctdesassoc")
    assert y is True
