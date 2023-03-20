from igem.omics import db


def test_ncbi_create_table():
    y = db.create_table()
    print(y)
    assert y == y
