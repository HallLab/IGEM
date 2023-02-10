from typing import List, Optional, Union

import clarite
import pandas as pd


def categorize(
    data,
    cat_min: int = 3,
    cat_max: int = 6,
    cont_min: int = 15
):
    df_result = clarite.modify.categorize(data, cat_min, cat_max, cont_min)
    return df_result


def colfilter(
    data,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.colfilter(data, skip, only)
    return df_result


def colfilter_min_cat_n(
    data,
    n: int = 200,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.colfilter_min_cat_n(data, n, skip, only)
    return df_result


def colfilter_min_n(
    data,
    n: int = 200,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.colfilter_min_n(data, n, skip, only)
    return df_result


def colfilter_percent_zero(
    data,
    filter_percent: int = 200,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.colfilter_percent_zero(data, filter_percent, skip, only)  # noqa E501
    return df_result


def make_binary(
    data,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.make_binary(data, skip, only)
    return df_result


def make_categorical(
    data,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.make_categorical(data, skip, only)
    return df_result


def make_continuous(
    data,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.make_continuous(data, skip, only)
    return df_result


def merge_observations(top: pd.DataFrame, bottom: pd.DataFrame):
    df_result = clarite.modify.merge_observations(top, bottom)
    return df_result


def merge_variables(
    left: Union[pd.DataFrame, pd.Series],
    right: Union[pd.DataFrame, pd.Series],
    how: str = "outer",
):
    df_result = clarite.modify.merge_variables(left, right, how)
    return df_result


def move_variables(
    left: pd.DataFrame,
    right: Union[pd.DataFrame, pd.Series],
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.merge_variables(left, right, skip, only)
    return df_result


def recode_values(
    data,
    replacement_dict,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.recode_values(data, replacement_dict, skip, only)  # noqa E501
    return df_result


def remove_outliers(
    data,
    method: str = "gaussian",
    cutoff=3,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.remove_outliers(data, method, cutoff, skip, only)  # noqa E501
    return df_result


def rowfilter_incomplete_obs(
    data,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.rowfilter_incomplete_obs(data, skip, only)
    return df_result


def transform(
    data,
    transform_method: str,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.modify.transform(data, transform_method, skip, only) # noqa E501
    return df_result
