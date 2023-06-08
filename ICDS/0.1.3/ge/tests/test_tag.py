from pathlib import Path

import pytest

from igem.ge import filter


# Parameter file path testing
@pytest.fixture
def parameters():
    v_path = Path(__file__).parent / "test_data_files"  # noqa E501
    return str(v_path)


def test_ge_tag_create(parameters):
    status, tag = filter.term_map(
        connector=['ctdesassoc'],
        path_out=(parameters + "/results/terms_map_tag_create.csv"),
    )
    if status:
        v_tag = filter.create_tag(["ctdesassoc"])
        assert "GE.db-TAG:" in v_tag
    else:
        # No data to check function
        assert tag == ''


def test_ge_get_tag(parameters):
    status, tag = filter.term_map(
        connector=['ctdesassoc'],
        path_out=(parameters + "/results/terms_map_tag_report.csv"),
    )
    if status:
        df = filter.get_tag(tag=tag)
        assert len(df) == 1
    else:
        assert tag == ''


def test_ge_get_tag_data(parameters):
    status, tag = filter.term_map(
        connector=['ctdesassoc'],
        path_out=(parameters + "/results/terms_map_tag_data.csv"),
    )
    if status:
        check = filter.get_tag_data(
            tag=tag,
            path=(parameters + "/results")
        )
        assert check
    else:
        # No data to check function
        assert tag == ''
