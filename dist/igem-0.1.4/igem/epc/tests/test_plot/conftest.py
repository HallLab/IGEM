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
import pandas as pd
import pytest

DATA_PATH = Path(__file__).parent.parent / "test_data_files"
PY_DATA_PATH = Path(__file__).parent.parent / "py_test_output"


@pytest.fixture
def dataNHANESReal():
    # Load the data
    df = igem.load.from_tsv(DATA_PATH / "nhanes_real.txt", index_col="ID")
    # Correct Variables
    df = igem.modify.make_binary(
        df,
        only=[
            "RHQ570",
            "first_degree_support",
            "SDDSRVYR",
            "female",
            "black",
            "mexican",
            "other_hispanic",
            "other_eth",
        ],
    )
    df = igem.modify.make_categorical(df, only=["SES_LEVEL"])

    return df


@pytest.fixture
def resultNHANESReal():
    # Load the data
    df = igem.load.from_tsv(DATA_PATH / "nhanes_real.txt", index_col="ID")

    # Split out survey info
    survey_cols = ["SDMVPSU", "SDMVSTRA", "WTMEC4YR", "WTSHM4YR", "WTSVOC4Y"]
    survey_df = df[survey_cols]
    df = df.drop(columns=survey_cols)

    df = igem.modify.make_binary(
        df,
        only=[
            "RHQ570",
            "first_degree_support",
            "SDDSRVYR",
            "female",
            "black",
            "mexican",
            "other_hispanic",
            "other_eth",
        ],
    )
    df = igem.modify.make_categorical(df, only=["SES_LEVEL"])
    design = igem.survey.SurveyDesignSpec(
        survey_df,
        weights={
            "RHQ570": "WTMEC4YR",
            "first_degree_support": "WTMEC4YR",
            "URXUPT": "WTSHM4YR",
            "LBXV3A": "WTSVOC4Y",
            "LBXBEC": "WTMEC4YR",
        },
        cluster="SDMVPSU",
        strata="SDMVSTRA",
        fpc=None,
        nest=True,
    )
    calculated_result = igem.analyze.association_study(
        outcomes="BMXBMI",
        covariates=[
            "SES_LEVEL",
            "SDDSRVYR",
            "female",
            "black",
            "mexican",
            "other_hispanic",
            "other_eth",
            "RIDAGEYR",
        ],
        data=df,
        survey_design_spec=design,
    )
    igem.analyze.add_corrected_pvalues(calculated_result)
    return calculated_result


@pytest.fixture
def resultNHANESReal_multi(resultNHANESReal):
    top = resultNHANESReal.copy()
    bottom = resultNHANESReal.reset_index(drop=False)
    bottom["Outcome"] = "AlsoBMXBMI"
    bottom = bottom.set_index(resultNHANESReal.index.names)
    bottom["pvalue"] = bottom["pvalue"] / 10
    result = pd.concat([top, bottom])
    igem.analyze.add_corrected_pvalues(result)
    return result


@pytest.fixture
def resultNHANESsmall():
    # Load the data
    df = igem.load.from_csv(DATA_PATH / "nhanes_data.csv", index_col=None)
    # Process data
    df = igem.modify.make_binary(df, only=["HI_CHOL", "RIAGENDR"])
    df = igem.modify.make_categorical(df, only=["race", "agecat"])
    design = igem.survey.SurveyDesignSpec(
        df,
        weights="WTMEC2YR",
        cluster="SDMVPSU",
        strata="SDMVSTRA",
        fpc=None,
        nest=True,
    )
    df = igem.modify.colfilter(df, only=["HI_CHOL", "RIAGENDR", "race", "agecat"])  # noqa E501
    python_result = pd.concat(
        [
            igem.analyze.association_study(
                outcomes="HI_CHOL",
                covariates=["agecat", "RIAGENDR"],
                data=df,
                survey_design_spec=design,
            ),
            igem.analyze.association_study(
                outcomes="HI_CHOL",
                covariates=["race", "RIAGENDR"],
                data=df,
                survey_design_spec=design,
            ),
            igem.analyze.association_study(
                outcomes="HI_CHOL",
                covariates=["race", "agecat"],
                data=df,
                survey_design_spec=design,
            ),
        ],
        axis=0,
    )
    igem.analyze.add_corrected_pvalues(python_result)
    return python_result
