from igem.ge import etl


def test_ge_etl_collect_test():
    y = etl.collect_teste()
    print(y)
    assert y == y


def test_ge_etl_collect():
    y = etl.collect(connector="hmdb_csfmetab")
    assert y is True


def test_ge_etl_prepare(connector="hmdb_csfmetab"):
    y = etl.prepare()
    assert y is True


def test_ge_etl_map():
    y = etl.map()
    assert y is True


def test_ge_etl_reduce():
    y = etl.reduce()
    assert y is True
