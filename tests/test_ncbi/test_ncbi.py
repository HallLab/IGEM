from igem.omics import modules


def test_ncbi_create_table():
    y = modules.create_table()
    print(y)
    assert y == y
