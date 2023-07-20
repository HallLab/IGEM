# try:
#     import os
#     import sys

#     sys.path.append(
#         os.path.abspath(
#             os.path.join(
#                 os.path.dirname(__file__),
#                 '../igem'
#             )
#         )
#     )


# except:  # noqa E722
#     raise

from pathlib import Path

from epc.clarite import load

TESTS_PATH = Path(__file__).parent.parent
DATA_PATH = TESTS_PATH / "test_data_files"


def test_load_from_csv():
    df = load.from_csv(DATA_PATH / "load_one_row.csv", index_col=None)
    assert len(df) == 1


def test_load_from_tsv():
    df = load.from_tsv(DATA_PATH / "load_one_row.tsv", index_col=None)
    assert len(df) == 1
