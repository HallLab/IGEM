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

import epc as igem
import matplotlib.pyplot as plt
import pytest

DATA_PATH = Path(__file__).parent.parent / "test_data_files"
PY_DATA_PATH = Path(__file__).parent.parent / "py_test_output"


@pytest.mark.parametrize(
    "categories",
    [
        None,
        {
            "race": "demographics",
            "RIAGENDR": "demographics",
            "agecat": "demographics",
            "first_degree_support": "environment",
        },
    ],
)
@pytest.mark.parametrize(
    "result_list,bonferroni,fdr,label_vars",
    [
        (["resultNHANESReal"], None, None, None),
        (["resultNHANESReal", "resultNHANESsmall"], None, None, None),
        (["resultNHANESReal", "resultNHANESsmall"], 0.05, 0.1, ["LBXBEC"]),
        (["resultNHANESReal_multi"], None, None, ["LBXBEC"]),
    ],
)
def test_manhattan(result_list, bonferroni, fdr, label_vars, categories, request):  # noqa E501
    dfs = {name: request.getfixturevalue(name) for name in result_list}
    igem.plot.manhattan(
        dfs=dfs,
        bonferroni=bonferroni,
        fdr=fdr,
        label_vars=label_vars,
        categories=categories,
    )


@pytest.mark.parametrize(
    "ewas_result_name,pvalue_name,cutoff,num_rows,filename",
    [
        (
            "resultNHANESReal",
            "pvalue",
            0.05,
            3,
            PY_DATA_PATH / "top_results_nhanesreal.png",
        ),
        (
            "resultNHANESReal",
            "pvalue",
            None,
            3,
            PY_DATA_PATH / "top_results_nhanesreal_no_cutoff.png",
        ),
        (
            "resultNHANESsmall",
            "pvalue_bonferroni",
            0.05,
            None,
            PY_DATA_PATH / "top_results_nhanessmall.png",
        ),
        pytest.param(
            "resultNHANESsmall",
            "pvalue_bonferroni",
            0.05,
            None,
            PY_DATA_PATH / "top_results_multioutcome.png",
            marks=pytest.mark.xfail,
        ),
    ],
)
def test_top_results(
    ewas_result_name, pvalue_name, cutoff, num_rows, filename, request
):
    ewas_result = request.getfixturevalue(ewas_result_name)
    igem.plot.top_results(
        ewas_result=ewas_result,
        pvalue_name=pvalue_name,
        cutoff=cutoff,
        num_rows=num_rows,
        filename=filename,
    )


@pytest.mark.parametrize(
    "column,kwargs",
    [("URXUPT", dict()), ("URXUPT", {"bins": 200}), ("SES_LEVEL", dict())],
)
def test_histogram(dataNHANESReal, column, kwargs):
    igem.plot.histogram(data=dataNHANESReal, column=column, **kwargs)
    plt.show()


@pytest.mark.parametrize("filename", ["test", "test.pdf"])
@pytest.mark.parametrize("quality", ["low", "medium", "high"])
def test_distributions(dataNHANESReal, filename, quality, tmpdir):
    filename = Path(tmpdir) / filename
    igem.plot.distributions(data=dataNHANESReal, filename=filename, quality=quality)  # noqa E501
