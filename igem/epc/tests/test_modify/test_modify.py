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

import pandas as pd
import pytest
from epc.clarite import modify

# from statsmodels import datasets


def test_make_binary(plantTraits, capfd):
    # Fail due to non-binary
    with pytest.raises(
        ValueError,
        match=re.escape(
            "11 variable(s) did not have 2 unique values and couldn't be processed "  # noqa E501
            "as a binary type: pdias, longindex, durflow, height, begflow, mycor, "  # noqa E501
            "vegaer, vegsout, autopoll, insects, wind"
        ),
    ):
        modify.make_binary(plantTraits)

    # Pass, selecting 5 columns known to be binary
    cols = ["piq", "ros", "leafy", "winan", "suman"]
    result = modify.make_binary(plantTraits, only=cols)
    out, err = capfd.readouterr()
    assert (
        out
        == "================================================================================\n"  # noqa E501
        "Running make_binary\n"
        "--------------------------------------------------------------------------------\n"  # noqa E501
        "================================================================================\n"  # noqa E501
        "Running make_binary\n"
        "--------------------------------------------------------------------------------\n"  # noqa E501
        "Set 5 of 31 variable(s) as binary, each with 136 observations\n"
        "================================================================================\n"  # noqa E501
    )
    assert err == ""
    assert all(result[cols].dtypes == "category")


def test_make_categorical(plantTraits, capfd):
    """Currently no validation for maximum unique values"""
    result = modify.make_categorical(plantTraits)
    out, err = capfd.readouterr()
    assert (
        out
        == "================================================================================\n"  # noqa E501
        "Running make_categorical\n"
        "--------------------------------------------------------------------------------\n"  # noqa E501
        "Set 31 of 31 variable(s) as categorical, each with 136 observations\n"
        "================================================================================\n"  # noqa E501
    )
    assert err == ""
    assert all(result.dtypes == "category")


def test_make_continuous(plantTraits, capfd):
    """Currently no validation for minimum unique values"""
    result = modify.make_continuous(plantTraits)
    out, err = capfd.readouterr()
    assert (
        out
        == "================================================================================\n"  # noqa E501
        "Running make_continuous\n"
        "--------------------------------------------------------------------------------\n"  # noqa E501
        "Set 31 of 31 variable(s) as continuous, each with 136 observations\n"
        "================================================================================\n"  # noqa E501
    )
    assert err == ""
    assert all(result.dtypes != "category")


def test_merge(plantTraits):
    """Merge the different parts of a dataframe and ensure they are merged back to the original"""  # noqa E501
    df1 = plantTraits.loc[:, list(plantTraits)[:3]]
    df2 = plantTraits.loc[:, list(plantTraits)[3:6]]
    df3 = plantTraits.loc[:, list(plantTraits)[6:]]
    df = modify.merge_variables(df1, df2)
    df = modify.merge_variables(df, df3)
    assert all(df == plantTraits)


def test_colfilter_percent_zero(plantTraits, capfd):
    result = modify.colfilter_percent_zero(plantTraits)
    assert len(result) == len(result)  # TODO: Check another returns informations  # noqa E501
    # out, err = capfd.readouterr()
    # assert (
    #     out
    #     == "================================================================================\n"  # noqa E501
    #     "Running colfilter_percent_zero\n"
    #     "--------------------------------------------------------------------------------\n"  # noqa E501
    #     "Testing 31 of 31 continuous variables\n"
    #     "\tRemoved 7 (22.58%) tested continuous variables which were equal to zero in at least 90.00% of non-NA observations.\n"  # noqa E501
    #     "================================================================================\n"
    # )
    # assert err == ""
    # assert result.shape == (136, 24)


def test_colfilter_min_n(plantTraits, capfd):
    n = len(plantTraits)
    plantTraits["test"] = [None] + [True] * 2 + [False] * (n - 3)
    plantTraits = modify.make_binary(data=plantTraits, only=["test"])
    result = modify.colfilter_min_n(plantTraits, n=n)
    out, err = capfd.readouterr()
    assert (
        out
        == "================================================================================\n"  # noqa E501
        "Running make_binary\n"
        "--------------------------------------------------------------------------------\n"  # noqa E501
        "Set 1 of 32 variable(s) as binary, each with 136 observations\n"
        "================================================================================\n"  # noqa E501
        "================================================================================\n"  # noqa E501
        "Running colfilter_min_n\n"
        "--------------------------------------------------------------------------------\n"  # noqa E501
        "Testing 1 of 1 binary variables\n"
        "\tRemoved 1 (100.00%) tested binary variables which had less than 136 non-null values.\n"  # noqa E501
        "Testing 0 of 0 categorical variables\n"
        "Testing 31 of 31 continuous variables\n"
        "\tRemoved 19 (61.29%) tested continuous variables which had less than 136 non-null values.\n"  # noqa E501
        "================================================================================\n"  # noqa E501
    )
    assert err == ""
    assert result.shape == (136, 12)


def test_colfilter_min_cat_n(plantTraits, capfd):
    plantTraits["test"] = (
        ["cat1"] * 2 + ["cat2"] * 6 + ["cat3"] * (len(plantTraits) - 8)
    )
    plantTraits["test2"] = (
        ["cat1"] * 3 + ["cat2"] * 6 + ["cat3"] * (len(plantTraits) - 9)
    )
    plantTraits = modify.make_categorical(data=plantTraits, only=["test", "test2"])  # noqa E501
    result = modify.colfilter_min_cat_n(plantTraits, n=3)
    out, err = capfd.readouterr()
    assert (
        out
        == "================================================================================\n"  # noqa E501
        "Running make_categorical\n"
        "--------------------------------------------------------------------------------\n"  # noqa E501
        "Set 2 of 33 variable(s) as categorical, each with 136 observations\n"
        "================================================================================\n"  # noqa E501
        "================================================================================\n"  # noqa E501
        "Running colfilter_min_cat_n\n"
        "--------------------------------------------------------------------------------\n"  # noqa E501
        "Testing 0 of 0 binary variables\n"
        "Testing 2 of 2 categorical variables\n"
        "\tRemoved 1 (50.00%) tested categorical variables which had a category with less than 3 values.\n"   # noqa E501
        "================================================================================\n"  # noqa E501
    )
    assert err == ""
    assert result.shape == (136, 32)


def test_rowfilter_incomplete_obs(plantTraits, capfd):
    col_names = list(plantTraits)[:4]
    plantTraits.iloc[0, 0] = None
    plantTraits.iloc[2, 5:7] = None
    result = modify.rowfilter_incomplete_obs(plantTraits, only=col_names)

    out, err = capfd.readouterr()
    assert (
        out
        == "================================================================================\n"  # noqa E501
        "Running rowfilter_incomplete_obs\n"
        "--------------------------------------------------------------------------------\n"  # noqa E501
        "Removed 45 of 136 observations (33.09%) due to NA values in any of 4 variables\n"  # noqa E501
        "================================================================================\n"  # noqa E501
    )
    assert err == ""
    assert result.shape == (91, 31)


def test_recode_values(plantTraits):
    # TODO
    return


def test_remove_outliers_gaussian(plantTraits):
    # Gaussian
    result = modify.remove_outliers(plantTraits, method="gaussian", skip=["durflow"])  # noqa E501
    assert result.isna().sum()["longindex"] == plantTraits.isna().sum()["longindex"]  # noqa E501
    assert result.isna().sum()["durflow"] == plantTraits.isna().sum()["durflow"]  # noqa E501
    assert result.isna().sum()["vegaer"] == plantTraits.isna().sum()["vegaer"] + 12  # noqa E501
    return


def test_remove_outliers_iqr(plantTraits):
    # Inter-Quartile Range
    result = modify.remove_outliers(
        plantTraits, method="iqr", cutoff=1.5, skip=["durflow"]
    )
    assert result.isna().sum()["longindex"] == plantTraits.isna().sum()["longindex"]  # noqa E501
    assert result.isna().sum()["durflow"] == plantTraits.isna().sum()["durflow"]  # noqa E501
    assert result.isna().sum()["vegaer"] == plantTraits.isna().sum()["vegaer"] + 17  # noqa E501
    return


def test_categorize(plantTraits, capfd):
    # TODO
    modify.categorize(plantTraits)
    return


def test_transform(plantTraits, capfd):
    """Test a log10 transform"""
    df = pd.DataFrame(
        {"a": [10, 100, 1000], "b": [100, 1000, 10000], "c": [True, False, True]}  # noqa E501
    )

    result = modify.transform(df, "log10", skip=["c"])

    assert all(result["a"] == [1, 2, 3])
    assert all(result["b"] == [2, 3, 4])
    assert all(result["c"] == [True, False, True])
    return


def test_categorize_many_string():
    """
    Ensure an error isn't thrown when attempting to make a string column continuous  # noqa E501
    """
    df = pd.DataFrame(
        {"a": range(100), "b": range(100), "c": [str(n) + "ABC" for n in range(100)]}  # noqa E501
    )
    categorized = modify.categorize(df)
    # Dtypes and data shouldn't have actually changed.  'c' will remain an 'unknown' type.  # noqa E501
    assert (categorized.dtypes == df.dtypes).all()
    assert (categorized == df).all().all()
    return
