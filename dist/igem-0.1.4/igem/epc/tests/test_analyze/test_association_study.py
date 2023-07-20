"""
Note: relative tolerance must be set for some tests

I spent a lot of time trying to track these down, but couldn't find any good solutions.  # noqa E501
The results seem suitably close, and my best guess is that the difference is due to precision differences
or the no-exact-solution nature of GLMs.

* 1e-04 when comparing standardized data b/c of differences in R and python (due to precision?)
* 1e-04 for nhanes tests using survey information (large dataset may be susceptible to precision differences during more complicated calculations)
* 1 for nhanes realistic data using standardization (beta values differ, likely due to standardization calculation)
"""

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


import re
from pathlib import Path

# import igem
import epc as igem
import numpy as np
import pandas as pd
import pytest
from pandas._testing import assert_frame_equal

TESTS_PATH = Path(__file__).parent.parent
DATA_PATH = TESTS_PATH / "test_data_files"
RESULT_PATH = TESTS_PATH / "r_test_output" / "analyze"


def python_cat_to_r_cat(python_df):
    """
    Return the same dataframe with the 3rd level of the multiindex ("Category") updated # noqa E501
    so that the python-style name (e.g. race[T.3]) is converted to r-style (e.g. race3)
    """
    re_str = re.compile(r"(?P<var_name>.+)\[T\.(?P<cat_name>.+)\]")
    df = python_df.reset_index(level="Category", drop=False)
    df["Category"] = df["Category"].apply(
        lambda s: s if re_str.match(s) is None else "".join(re_str.match(s).groups()) # noqa E501
    )
    df = df.set_index("Category", append=True)
    return df


def compare_loaded(
    python_result, surveylib_result_file, compare_diffAIC=False, rtol=None
):
    """
    Compare surveylib results (run outside of clarite) to clarite results.
    Minor fixes for compatibility, such as ignoring order of rows/columns ('check_like') # noqa E501
    """
    # Load results run outside of CLARITE
    loaded_result = pd.read_csv(surveylib_result_file)
    loaded_result = loaded_result.set_index("Variable")
    loaded_result["N"] = loaded_result["N"].astype("Int64")
    loaded_result["Beta"] = loaded_result["Beta"].astype("float")

    # Drop DiffAIC unless a comparison makes sense
    if not compare_diffAIC:
        loaded_result = loaded_result[
            [c for c in loaded_result.columns if c != "Diff_AIC"]
        ]
        python_result = python_result[
            [c for c in python_result.columns if c != "Diff_AIC"]
        ]

    # Format python result to match
    python_result = python_result[loaded_result.columns]
    python_result = python_result.reset_index(level="Outcome", drop=True)

    # Compare
    if rtol is not None:
        assert_frame_equal(
            python_result, loaded_result, rtol=rtol, atol=0, check_like=True
        )
    else:
        assert_frame_equal(python_result, loaded_result, atol=0, check_like=True) # noqa E501


###############
# fpc Dataset #
###############
# continuous: ["x", "y"]
# --------------
# weights = "weight"
# strata = "stratid"
# cluster = "psuid"
# fpc = "Nh"
# nest = True


@pytest.mark.parametrize(
    "design_str,standardize",
    [
        ("withoutfpc", False),
        ("withoutfpc", True),
        ("withfpc", False),
        ("withfpc", True),
        ("nostrata", False),
        ("nostrata", True),
    ],
)
def test_fpc(data_fpc, design_str, standardize):
    """Use a survey design specifying weights, cluster, strata"""
    # Set data and design for each test
    if design_str == "withoutfpc":
        design = igem.survey.SurveyDesignSpec(
            data_fpc, weights="weight", cluster="psuid", strata="stratid", nest=True # noqa E501
        )
        df = igem.modify.colfilter(data_fpc, only=["x", "y"])
        surveylib_result_file = RESULT_PATH / "fpc_withoutfpc_result.csv"
    elif design_str == "withfpc":
        design = igem.survey.SurveyDesignSpec(
            data_fpc,
            weights="weight",
            cluster="psuid",
            strata="stratid",
            fpc="Nh",
            nest=True,
        )
        df = igem.modify.colfilter(data_fpc, only=["x", "y"])
        surveylib_result_file = RESULT_PATH / "fpc_withfpc_result.csv"
    elif design_str == "nostrata":
        # Load the data
        df = igem.load.from_csv(DATA_PATH / "fpc_nostrat_data.csv", index_col=None) # noqa E501
        # Process data
        df = igem.modify.make_continuous(df, only=["x", "y"])
        design = igem.survey.SurveyDesignSpec(
            df, weights="weight", cluster="psuid", strata=None, fpc="Nh", nest=True # noqa E501
        )
        df = igem.modify.colfilter(df, only=["x", "y"])
        surveylib_result_file = RESULT_PATH / "fpc_withfpc_nostrat_result.csv"
    else:
        raise ValueError(f"design_str unknown: '{design_str}'")

    # Get results
    regression_kinds = ["weighted_glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = igem.analyze.association_study(
            data=df,
            outcomes="y",
            covariates=[],
            survey_design_spec=design,
            regression_kind=rk,
            min_n=1,
            standardize_data=standardize,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file)
        assert_frame_equal(results[regression_kinds[0]], results[regression_kinds[1]]) # noqa E501
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04 # noqa E501
        )


###############
# api Dataset #
###############
# continuous: ["api00", "ell", "meals", "mobility"]  (there are others, but they weren't tested in R) # noqa E501
# --------------
# weights = "pw"
# strata = "stype"
# cluster = "dnum"
# fpc = "fpc"
# nest = False


@pytest.mark.parametrize(
    "design_str,standardize",
    [
        ("noweights", False),
        ("noweights", True),
        ("noweights_withNA", False),
        ("noweights_withNA", True),
        ("stratified", False),
        ("stratified", True),
        ("cluster", False),
        ("cluster", True),
    ],
)
def test_api(design_str, standardize):
    """Test the api dataset with no survey info"""
    if design_str == "noweights":
        df = igem.load.from_csv(DATA_PATH / "apipop_data.csv", index_col=None)
        df = igem.modify.make_continuous(
            df, only=["api00", "ell", "meals", "mobility"]
        )
        df = igem.modify.colfilter(df, only=["api00", "ell", "meals", "mobility"]) # noqa E501
        design = None
        surveylib_result_file = RESULT_PATH / "api_apipop_result.csv"
    elif design_str == "noweights_withNA":
        df = igem.load.from_csv(DATA_PATH / "apipop_withna_data.csv", index_col=None) # noqa E501
        df = igem.modify.make_continuous(
            df, only=["api00", "ell", "meals", "mobility"]
        )
        df = igem.modify.colfilter(df, only=["api00", "ell", "meals", "mobility"]) # noqa E501
        design = None
        surveylib_result_file = RESULT_PATH / "api_apipop_withna_result.csv"
    elif design_str == "stratified":
        df = igem.load.from_csv(DATA_PATH / "apistrat_data.csv", index_col=None) # noqa E501
        df = igem.modify.make_continuous(
            df, only=["api00", "ell", "meals", "mobility"]
        )
        design = igem.survey.SurveyDesignSpec(
            df, weights="pw", cluster=None, strata="stype", fpc="fpc"
        )
        df = igem.modify.colfilter(df, only=["api00", "ell", "meals", "mobility"]) # noqa E501
        surveylib_result_file = RESULT_PATH / "api_apistrat_result.csv"
    elif design_str == "cluster":
        df = igem.load.from_csv(DATA_PATH / "apiclus1_data.csv", index_col=None) # noqa E501
        df = igem.modify.make_continuous(
            df, only=["api00", "ell", "meals", "mobility"]
        )
        design = igem.survey.SurveyDesignSpec(
            df, weights="pw", cluster="dnum", strata=None, fpc="fpc"
        )
        df = igem.modify.colfilter(df, only=["api00", "ell", "meals", "mobility"]) # noqa E501
        surveylib_result_file = RESULT_PATH / "api_apiclus1_result.csv"
    else:
        raise ValueError(f"design_str unknown: '{design_str}'")

    # Run analysis and comparison
    if design is None:
        regression_kinds = ["glm", "r_survey"]
    else:
        regression_kinds = ["weighted_glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = pd.concat(
            [
                igem.analyze.association_study(
                    data=df,
                    outcomes="api00",
                    covariates=["meals", "mobility"],
                    survey_design_spec=design,
                    regression_kind=rk,
                    min_n=1,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    data=df,
                    outcomes="api00",
                    covariates=["ell", "mobility"],
                    survey_design_spec=design,
                    regression_kind=rk,
                    min_n=1,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    data=df,
                    outcomes="api00",
                    covariates=["ell", "meals"],
                    survey_design_spec=design,
                    regression_kind=rk,
                    min_n=1,
                    standardize_data=standardize,
                ),
            ],
            axis=0,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file)
        assert_frame_equal(results[regression_kinds[0]], results[regression_kinds[1]]) # noqa E501
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04 # noqa E501
        )


##################
# NHANES Dataset #
##################
# A data frame with 8591 observations on the following 7 variables.
# SDMVPSU - Primary sampling units
# SDMVSTRA - Sampling strata
# WTMEC2YR - Sampling weights
# HI_CHOL - Binary: 1 for total cholesterol over 240mg/dl, 0 under 240mg/dl
# race - Categorical (1=Hispanic, 2=non-Hispanic white, 3=non-Hispanic black, 4=other) # noqa E501
# agecat  - Categorical Age group(0,19] (19,39] (39,59] (59,Inf]
# RIAGENDR - Binary: Gender: 1=male, 2=female


@pytest.mark.parametrize("standardize", [False, True])
def test_nhanes_noweights(data_NHANES, standardize):
    """Test the nhanes dataset with no survey info"""
    # Process data
    df = igem.modify.colfilter(
        data_NHANES, only=["HI_CHOL", "RIAGENDR", "race", "agecat"]
    )
    # Get Results
    surveylib_result_file = RESULT_PATH / "nhanes_noweights_result.csv"
    # Run analysis and comparison
    regression_kinds = ["glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = pd.concat(
            [
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["agecat", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "agecat"],
                    data=df,
                    regression_kind=rk,
                    standardize_data=standardize,
                ),
            ],
            axis=0,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file)
        assert_frame_equal(results[regression_kinds[0]], results[regression_kinds[1]]) # noqa E501
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04 # noqa E501
        )


@pytest.mark.parametrize("standardize", [False, True])
def test_nhanes_noweights_withNA(data_NHANES_withNA, standardize):
    """Test the nhanes dataset with no survey info and some missing values in a categorical""" # noqa E501
    # Process data
    df = igem.modify.colfilter(
        data_NHANES_withNA, only=["HI_CHOL", "RIAGENDR", "race", "agecat"]
    )
    # Get Results
    surveylib_result_file = RESULT_PATH / "nhanes_noweights_withna_result.csv"
    # Run analysis and comparison
    regression_kinds = ["glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = pd.concat(
            [
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["agecat", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "agecat"],
                    data=df,
                    regression_kind=rk,
                    standardize_data=standardize,
                ),
            ],
            axis=0,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file)
        assert_frame_equal(results[regression_kinds[0]], results[regression_kinds[1]]) # noqa E501
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04 # noqa E501
        )


@pytest.mark.parametrize("standardize", [False, True])
def test_nhanes_fulldesign(data_NHANES, standardize):
    """Test the nhanes dataset with the full survey design"""
    # Make Design
    design = igem.survey.SurveyDesignSpec(
        data_NHANES,
        weights="WTMEC2YR",
        cluster="SDMVPSU",
        strata="SDMVSTRA",
        fpc=None,
        nest=True,
    )
    df = igem.modify.colfilter(
        data_NHANES, only=["HI_CHOL", "RIAGENDR", "race", "agecat"]
    )
    # Get Results
    surveylib_result_file = RESULT_PATH / "nhanes_complete_result.csv"
    # Run analysis and comparison
    regression_kinds = ["weighted_glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = pd.concat(
            [
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["agecat", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "agecat"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
            ],
            axis=0,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file, rtol=1e-04) # noqa E501
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04 # noqa E501
        )
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04 # noqa E501
        )


@pytest.mark.parametrize("standardize", [False, True])
def test_nhanes_fulldesign_withna(data_NHANES_withNA, standardize):
    """Test the nhanes dataset with the full survey design"""
    # Make Design
    design = igem.survey.SurveyDesignSpec(
        data_NHANES_withNA,
        weights="WTMEC2YR",
        cluster="SDMVPSU",
        strata="SDMVSTRA",
        fpc=None,
        nest=True,
    )
    df = igem.modify.colfilter(
        data_NHANES_withNA, only=["HI_CHOL", "RIAGENDR", "race", "agecat"]
    )
    # Get Results
    surveylib_result_file = RESULT_PATH / "nhanes_complete_withna_result.csv"
    # Run analysis and comparison
    regression_kinds = ["weighted_glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = pd.concat(
            [
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["agecat", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "agecat"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
            ],
            axis=0,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file, rtol=1e-04) # noqa E501
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )


@pytest.mark.parametrize("standardize", [False, True])
def test_nhanes_fulldesign_subset_category(data_NHANES, standardize):
    """Test the nhanes dataset with the full survey design, subset by dropping a category"""  # noqa E501
    # Make Design
    design = igem.survey.SurveyDesignSpec(
        data_NHANES,
        weights="WTMEC2YR",
        cluster="SDMVPSU",
        strata="SDMVSTRA",
        fpc=None,
        nest=True,
    )
    design.subset(data_NHANES["agecat"] != "(19,39]")
    df = igem.modify.colfilter(
        data_NHANES, only=["HI_CHOL", "RIAGENDR", "race", "agecat"]
    )
    # Get Results
    surveylib_result_file = RESULT_PATH / "nhanes_complete_result_subset_cat.csv"  # noqa E501
    # Run analysis and comparison
    regression_kinds = ["weighted_glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = pd.concat(
            [
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["agecat", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "agecat"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
            ],
            axis=0,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file, rtol=1e-03)  # noqa E501
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )


@pytest.mark.parametrize("standardize", [False, True])
def test_nhanes_fulldesign_subset_continuous(standardize):
    """Test the nhanes dataset with the full survey design and a random subset"""  # noqa E501
    # Load the data
    df = igem.load.from_csv(DATA_PATH / "nhanes_data_subset.csv", index_col=None)  # noqa E501
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
        drop_unweighted=True,
    )
    design.subset(df["subset"] > 0)
    df = df.drop(columns=["subset"])
    df = igem.modify.colfilter(df, only=["HI_CHOL", "RIAGENDR", "race", "agecat"])  # noqa E501
    # Get Results
    surveylib_result_file = RESULT_PATH / "nhanes_complete_result_subset_cont.csv"  # noqa E501
    # Run analysis and comparison
    regression_kinds = ["weighted_glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = pd.concat(
            [
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["agecat", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "agecat"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
            ],
            axis=0,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file, rtol=1e-04)  # noqa E501
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )


@pytest.mark.parametrize("standardize", [False, True])
def test_nhanes_weightsonly(data_NHANES, standardize):
    """Test the nhanes dataset with only weights in the survey design"""
    # Make Design
    design = igem.survey.SurveyDesignSpec(data_NHANES, weights="WTMEC2YR")
    df = igem.modify.colfilter(
        data_NHANES, only=["HI_CHOL", "RIAGENDR", "race", "agecat"]
    )
    # Get Results
    surveylib_result_file = RESULT_PATH / "nhanes_weightsonly_result.csv"
    # Run analysis and comparison
    regression_kinds = ["weighted_glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = pd.concat(
            [
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["agecat", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "agecat"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
            ],
            axis=0,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file)
        assert_frame_equal(results[regression_kinds[0]], results[regression_kinds[1]])  # noqa E501
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )


@pytest.mark.parametrize(
    "single_cluster,load_filename,standardize",
    [
        ("certainty", "nhanes_certainty_result.csv", False),
        ("certainty", "nhanes_certainty_result.csv", True),
        ("adjust", "nhanes_adjust_result.csv", False),
        ("adjust", "nhanes_adjust_result.csv", True),
        ("average", "nhanes_average_result.csv", False),
        ("average", "nhanes_average_result.csv", True),
    ],
)
def test_nhanes_lonely(data_NHANES_lonely, single_cluster, load_filename, standardize):  # noqa E501
    """Test the nhanes dataset with a lonely PSU and the value set to certainty"""  # noqa E501
    # Make Design
    design = igem.survey.SurveyDesignSpec(
        data_NHANES_lonely,
        weights="WTMEC2YR",
        cluster="SDMVPSU",
        strata="SDMVSTRA",
        fpc=None,
        nest=True,
        single_cluster=single_cluster,
    )
    df = igem.modify.colfilter(
        data_NHANES_lonely, only=["HI_CHOL", "RIAGENDR", "race", "agecat"]
    )
    # Get Results
    surveylib_result_file = RESULT_PATH / load_filename
    # Run analysis and comparison
    regression_kinds = ["weighted_glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = pd.concat(
            [
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["agecat", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "RIAGENDR"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
                igem.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "agecat"],
                    data=df,
                    regression_kind=rk,
                    survey_design_spec=design,
                    standardize_data=standardize,
                ),
            ],
            axis=0,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file, rtol=1e-04)  # noqa E501
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )


@pytest.mark.parametrize("standardize", [False, True])
def test_nhanes_realistic(standardize):
    """Test a more realistic set of NHANES data, specifically using multiple weights and missing values"""  # noqa E501
    # Load the data
    df = igem.load.from_tsv(
        DATA_PATH.parent / "test_data_files" / "nhanes_real.txt", index_col="ID"  # noqa E501
    )
    # Process data
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
    # Get Results
    surveylib_result_file = RESULT_PATH / "nhanes_real_result.csv"
    # Run analysis and comparison
    regression_kinds = ["weighted_glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = igem.analyze.association_study(
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
            regression_kind=rk,
            survey_design_spec=design,
            standardize_data=standardize,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file, rtol=1e-04)  # noqa E501
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=5e-03  # noqa E501
        )


@pytest.mark.parametrize("standardize", [False, True])
def test_nhanes_subset_singleclusters(standardize):
    """Test a partial nhanes dataset with the full survey design with a subset causing single clusters"""  # noqa E501
    # Load the data
    df = igem.load.from_tsv(
        DATA_PATH.parent / "test_data_files" / "nhanes_subset" / "data.txt"
    )
    survey_df = igem.load.from_tsv(
        DATA_PATH.parent / "test_data_files" / "nhanes_subset" / "design_data.txt"  # noqa E501
    )
    survey_df = survey_df.loc[df.index]
    # Process data
    df = igem.modify.make_binary(df, only=["LBXHBC", "black", "female"])
    df = igem.modify.make_categorical(df, only=["SES_LEVEL", "SDDSRVYR"])
    # Create design
    design = igem.survey.SurveyDesignSpec(
        survey_df,
        weights="WTMEC4YR",
        cluster="SDMVPSU",
        strata="SDMVSTRA",
        fpc=None,
        nest=True,
    )
    design.subset(df["black"] == 1)
    df = df.drop(columns="black")
    # Get Results
    surveylib_result_file = RESULT_PATH / "nhanes_subset_result.csv"
    covariates = ["female", "SES_LEVEL", "RIDAGEYR", "SDDSRVYR", "BMXBMI"]
    # Run analysis and comparison
    regression_kinds = ["weighted_glm", "r_survey"]
    results = dict()
    for rk in regression_kinds:
        results[rk] = igem.analyze.association_study(
            outcomes="LBXLYPCT",
            covariates=covariates,
            data=df,
            regression_kind=rk,
            survey_design_spec=design,
            min_n=50,
            standardize_data=standardize,
        )
        # Compare
    if not standardize:
        compare_loaded(results[regression_kinds[0]], surveylib_result_file, rtol=1e-04)  # noqa E501
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=1e-04  # noqa E501
        )
    else:
        assert_frame_equal(
            results[regression_kinds[0]], results[regression_kinds[1]], rtol=5e-03  # noqa E501
        )


def test_report_betas(data_NHANES):
    # Process data
    df = igem.modify.colfilter(
        data_NHANES, only=["HI_CHOL", "RIAGENDR", "race", "agecat"]
    )
    # Get Results
    normal_result = igem.analyze.association_study(
        outcomes="HI_CHOL", covariates=["agecat", "RIAGENDR"], data=df
    )
    betas_result_python = igem.analyze.association_study(
        outcomes="HI_CHOL",
        covariates=["agecat", "RIAGENDR"],
        data=df,
        report_categorical_betas=True,
    )
    betas_result_r = igem.analyze.association_study(
        outcomes="HI_CHOL",
        covariates=["agecat", "RIAGENDR"],
        data=df,
        regression_kind="r_survey",
        report_categorical_betas=True,
    )
    # Ensure including betas worked
    assert len(betas_result_python) == len(df["race"].cat.categories) - 1
    assert len(betas_result_python) == len(betas_result_r)
    # Ensure including betas did not change other values
    beta_sub = betas_result_python.groupby(level=[0, 1]).first()
    beta_sub[["Beta", "SE", "Beta_pvalue"]] = np.nan
    assert_frame_equal(beta_sub, normal_result)
    # Ensure python and R results match
    betas_result_python = python_cat_to_r_cat(betas_result_python)
    assert_frame_equal(
        betas_result_python,
        betas_result_r,
        check_dtype=False,
        check_exact=False,
        atol=0,
        rtol=1e-04,
    )


def test_report_betas_fulldesign(data_NHANES):
    design = igem.survey.SurveyDesignSpec(
        data_NHANES,
        weights="WTMEC2YR",
        cluster="SDMVPSU",
        strata="SDMVSTRA",
        fpc=None,
        nest=True,
    )
    # Process data
    df = igem.modify.colfilter(
        data_NHANES, only=["HI_CHOL", "RIAGENDR", "race", "agecat"]
    )
    # Get Results
    normal_result = igem.analyze.association_study(
        outcomes="HI_CHOL",
        covariates=["agecat", "RIAGENDR"],
        data=df,
        survey_design_spec=design,
    )
    betas_result_python = igem.analyze.association_study(
        outcomes="HI_CHOL",
        covariates=["agecat", "RIAGENDR"],
        data=df,
        report_categorical_betas=True,
        survey_design_spec=design,
    )
    betas_result_r = igem.analyze.association_study(
        outcomes="HI_CHOL",
        covariates=["agecat", "RIAGENDR"],
        data=df,
        report_categorical_betas=True,
        survey_design_spec=design,
        regression_kind="r_survey",
    )
    # Ensure including betas worked
    assert len(betas_result_python) == len(df["race"].cat.categories) - 1
    assert len(betas_result_python) == len(betas_result_r)
    # Ensure including betas did not change other values
    beta_sub = betas_result_python.groupby(level=[0, 1]).first()
    beta_sub[["Beta", "SE", "Beta_pvalue"]] = np.nan
    assert_frame_equal(beta_sub, normal_result)
    # Ensure python and R results match
    betas_result_python = python_cat_to_r_cat(betas_result_python)
    assert_frame_equal(
        betas_result_python,
        betas_result_r,
        check_dtype=False,
        check_exact=False,
        atol=0,
    )


def test_edge_encondig_logistic_regression():

    import clarite
    from pandas_genomics import sim

    # Additive Main Effect for SNP1 without interaction
    train = sim.BAMS.from_model(
        eff1=sim.SNPEffectEncodings.ADDITIVE,
        eff2=sim.SNPEffectEncodings.ADDITIVE,
        penetrance_base=0.45,
        main1=1,
        main2=0,
        interaction=0,
        random_seed=2021,
    )
    # test = sim.BAMS.from_model(
    #     eff1=sim.SNPEffectEncodings.ADDITIVE,
    #     eff2=sim.SNPEffectEncodings.ADDITIVE,
    #     penetrance_base=0.45,
    #     main1=1,
    #     main2=0,
    #     interaction=0,
    # )
    train_add = train.generate_case_control(n_cases=5000, n_controls=5000)
    # test_add = test.generate_case_control(n_cases=5000, n_controls=5000)
    edge_weights = train_add.genomics.calculate_edge_encoding_values(
        data=train_add["Outcome"], outcome_variable="Outcome"
    )

    # edge_results = igem.analyze.association_study(
    #     data=train_add,
    #     outcomes="Outcome",
    #     encoding="edge",
    #     edge_encoding_info=edge_weights,
    # )
    edge_results = clarite.analyze.association_study(
        data=train_add,
        outcomes="Outcome",
        encoding="edge",
        edge_encoding_info=edge_weights,
    )

    assert edge_results["Variable_type"][0] == "continuous"
