"""
Modify
======

Functions used to filter and/or change some data, always taking in one set of
data and returning one set of data.

    .. autofunction:: categorize
    .. autofunction:: colfilter
    .. autofunction:: colfilter_percent_zero
    .. autofunction:: colfilter_min_n
    .. autofunction:: colfilter_min_cat_n
    .. autofunction:: make_binary
    .. autofunction:: make_categorical
    .. autofunction:: make_continuous
    .. autofunction:: merge_observations
    .. autofunction:: merge_variables
    .. autofunction:: move_variables
    .. autofunction:: recode_values
    .. autofunction:: remove_outliers
    .. autofunction:: rowfilter_incomplete_obs
    .. autofunction:: transform

"""

from typing import List, Optional, Union

import clarite
import pandas as pd


def categorize(data, cat_min: int = 3, cat_max: int = 6, cont_min: int = 15):
    """
    Classify variables into constant, binary, categorical, continuous, and
    'unknown'.  Drop variables that only have NaN values.

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be processed
    cat_min: int, default 3
        Minimum number of unique, non-NA values for a categorical variable
    cat_max: int, default 6
        Maximum number of unique, non-NA values for a categorical variable
    cont_min: int, default 15
        Minimum number of unique, non-NA values for a continuous variable

    Returns
    -------
    result: pd.DataFrame or None
        If inplace, returns None.  Changes the datatypes on the input DF.

    Examples
    --------
    >>> import igem
    >>> igem.epc.modify.categorize(nhanes)
    362 of 970 variables (37.32%) are classified as binary (2 unique values).
    47 of 970 variables (4.85%) are classified as categorical (3 to 6 unique).
    483 of 970 variables (49.79%) are classified as continuous (>= 15 unique).
    42 of 970 variables (4.33%) were dropped.
            10 variables had zero unique values (all NA).
            32 variables had one unique value.
    36 of 970 variables (3.71%) were not categorized and need to be set.
            36 variables had between 6 and 15 unique values
            0 variables had >= 15 values but couldn't be converted to
            continuous (numeric) values
    """

    df_result = clarite.modify.categorize(data, cat_min, cat_max, cont_min)
    return df_result


def colfilter(
    data,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Remove some variables (skip) or keep only certain variables (only)

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be processed and returned
    skip: str, list or None (default is None)
        List of variables to remove
    only: str, list or None (default is None)
        List of variables to keep

    Returns
    -------
    data: pd.DataFrame
        The filtered DataFrame

    Examples
    --------
    >>> import igem
    >>> f_logBMI = igem.epc.modify.colfilter(nhanes, only=['BMXBMI', 'female'])
    ================================================================================
    Running colfilter
    --------------------------------------------------------------------------------
    Keeping 2 of 945 variables:
            0 of 0 binary variables
            0 of 0 categorical variables
            2 of 945 continuous variables
            0 of 0 unknown variables
    ================================================================================
    """

    df_result = clarite.modify.colfilter(data, skip, only)
    return df_result


def colfilter_min_cat_n(
    data,
    n: int = 200,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Remove binary and categorical variables which have less than <n>
    occurences of each unique value

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be processed and returned
    n: int, default 200
        The minimum number of occurences of each unique value required in
        order for a variable not to be filtered
    skip: str, list or None (default is None)
        List of variables that the filter should *not* be applied to
    only: str, list or None (default is None)
        List of variables that the filter should *only* be applied to

    Returns
    -------
    data: pd.DataFrame
        The filtered DataFrame

    Examples
    --------
    >>> import igem
    >>> nhanes_filtered = igem.epc.modify.colfilter_min_cat_n(nhanes)
    ================================================================================
    Running colfilter_min_cat_n
    --------------------------------------------------------------------------------
    WARNING: 36 variables need to be categorized into a type manually
    Testing 362 of 362 binary variables
            Removed 248 (68.51%) tested binary variables which had a category
            with less than 200 values
    Testing 47 of 47 categorical variables
            Removed 36 (76.60%) tested categorical variables which had a
            category with less than 200 values
    """

    df_result = clarite.modify.colfilter_min_cat_n(data, n, skip, only)
    return df_result


def colfilter_min_n(
    data,
    n: int = 200,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Remove variables which have less than <n> non-NA values

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be processed and returned
    n: int, default 200
        The minimum number of unique values required in order for a variable
        not to be filtered
    skip: str, list or None (default is None)
        List of variables that the filter should *not* be applied to
    only: str, list or None (default is None)
        List of variables that the filter should *only* be applied to

    Returns
    -------
    data: pd.DataFrame
        The filtered DataFrame

    Examples
    --------
    >>> import igem
    >>> nhanes_filtered = igem.epc.modify.colfilter_min_n(nhanes)
    ================================================================================
    Running colfilter_min_n
    --------------------------------------------------------------------------------
    WARNING: 36 variables need to be categorized into a type manually
    Testing 362 of 362 binary variables
            Removed 12 (3.31%) tested binary variables which had less than 200
            non-null values
    Testing 47 of 47 categorical variables
            Removed 8 (17.02%) tested categorical variables which had less
            than 200 non-null values
    Testing 483 of 483 continuous variables
            Removed 8 (1.66%) tested continuous variables which had less than
            200 non-null values
    """

    df_result = clarite.modify.colfilter_min_n(data, n, skip, only)
    return df_result


def colfilter_percent_zero(
    data,
    filter_percent: int = 200,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Remove continuous variables which have <proportion> or more values of zero
    (excluding NA)

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be processed and returned
    filter_percent: float, default 90.0
            If the percentage of rows in the data with a value of zero is
            greater than or equal to this value, the variable is filtered out.
    skip: str, list or None (default is None)
        List of variables that the filter should *not* be applied to
    only: str, list or None (default is None)
        List of variables that the filter should *only* be applied to

    Returns
    -------
    data: pd.DataFrame
        The filtered DataFrame

    Examples
    --------
    >>> import igem
    >>> nhanes_filtered = igem.epc.modify.colfilter_percent_zero(
                                            nhanes_filtered
                                            )
    ================================================================================
    Running colfilter_percent_zero
    --------------------------------------------------------------------------------
    WARNING: 36 variables need to be categorized into a type manually
    Testing 483 of 483 continuous variables
            Removed 30 (6.21%) tested continuous variables which were equal to
            zero in at least 90.00% of non-NA observations.
    """

    df_result = clarite.modify.colfilter_percent_zero(
        data, filter_percent, skip, only
    )  # noqa E501
    return df_result


def make_binary(
    data,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Set variable types as Binary

    Checks that each variable has at most 2 values and converts the type to
    pd.Categorical.

    Note: When these variables are used in regression, they are ordered by
    value. For example, Sex (Male=1, Female=2) will encode "Male" as 0 and
    "Female" as 1 during the EWAS regression step.

    Parameters
    ----------
    data: pd.DataFrame or pd.Series
        Data to be processed
    skip: str, list or None (default is None)
        List of variables that should *not* be made binary
    only: str, list or None (default is None)
        List of variables that are the *only* ones to be made binary

    Returns
    -------
    data: pd.DataFrame
        DataFrame with the same data but validated and converted to binary

    Examples
    --------
    >>> import igem
    >>> nhanes = igem.epc.modify.make_binary(
                      nhanes,
                      only=['female', 'black', 'mexican', 'other_hispanic']
                      )
    ================================================================================
    Running make_binary
    --------------------------------------------------------------------------------
    Set 4 of 970 variable(s) as binary, each with 22,624 observations
    """

    df_result = clarite.modify.make_binary(data, skip, only)
    return df_result


def make_categorical(
    data,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Set variable types as Categorical

    Converts the type to pd.Categorical

    Parameters
    ----------
    data: pd.DataFrame or pd.Series
        Data to be processed
    skip: str, list or None (default is None)
        List of variables that should *not* be made categorical
    only: str, list or None (default is None)
        List of variables that are the *only* ones to be made categorical

    Returns
    -------
    data: pd.DataFrame
        DataFrame with the same data but validated and converted to
        categorical types

    Examples
    --------
    >>> import igem
    >>> df = igem.epc.modify.make_categorical(df)
    ================================================================================
    Running make_categorical
    --------------------------------------------------------------------------------
    Set 12 of 12 variable(s) as categorical, each with 4,321 observations
    """

    df_result = clarite.modify.make_categorical(data, skip, only)
    return df_result


def make_continuous(
    data,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Set variable types as Numeric

    Converts the type to numeric

    Parameters
    ----------
    data: pd.DataFrame or pd.Series
        Data to be processed
    skip: str, list or None (default is None)
        List of variables that should *not* be made continuous
    only: str, list or None (default is None)
        List of variables that are the *only* ones to be made continuous

    Returns
    -------
    data: pd.DataFrame
        DataFrame with the same data but validated and converted to numeric

    Examples
    --------
    >>> import igem
    >>> df = igem.epc.modify.make_continuous(df)
    ================================================================================
    Running make_categorical
    --------------------------------------------------------------------------------
    Set 128 of 128 variable(s) as continuous, each with 4,321 observations
    """
    df_result = clarite.modify.make_continuous(data, skip, only)
    return df_result


def merge_observations(top: pd.DataFrame, bottom: pd.DataFrame):
    """
    Merge two datasets, keeping only the columns present in both.
    Raise an error if a datatype conflict occurs.

    Parameters
    ----------
    top: pd.DataFrame
        "top" DataFrame
    bottom: pd.DataFrame
        "bottom" DataFrame

    Returns
    -------
    result: pd.DataFrame
    """

    df_result = clarite.modify.merge_observations(top, bottom)
    return df_result


def merge_variables(
    left: Union[pd.DataFrame, pd.Series],
    right: Union[pd.DataFrame, pd.Series],
    how: str = "outer",
):
    """
    Merge a list of dataframes with different variables side-by-side.
    Keep all observations ('outer' merge) by default.

    Parameters
    ----------
    left: pd.Dataframe or pd.Series
        "left" DataFrame or Series
    right: pd.DataFrame or pd.Series
        "right" DataFrame or Series which uses the same index
    how: merge method, one of {'left', 'right', 'inner', 'outer'}
        Keep only rows present in the left data, the right data, both datasets,
        or either dataset.

    Examples
    --------
    >>> import igem
    >>> df = igem.epc.modify.merge_variables(df_bin, df_cat, how='outer')
    """

    df_result = clarite.modify.merge_variables(left, right, how)
    return df_result


def move_variables(
    left: pd.DataFrame,
    right: Union[pd.DataFrame, pd.Series],
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Move one or more variables from one DataFrame to another

    Parameters
    ----------
    left: pd.Dataframe
        DataFrame containing the variable(s) to be moved
    right: pd.DataFrame or pd.Series
        DataFrame or Series (which uses the same index) that the variable(s)
        will be moved to
    skip: str, list or None (default is None)
        List of variables that will *not* be moved
    only: str, list or None (default is None)
        List of variables that are the *only* ones to be moved

    Returns
    -------
    left: pd.DataFrame
        The first DataFrame with the variables removed
    right: pd.DataFrame
        The second DataFrame with the variables added

    Examples
    --------
    >>> import igem
    >>> df_cat, df_cont = igem.epc.modify.move_variables(
                 df_cat, df_cont,
                 only=["DRD350AQ", "DRD350DQ", "DRD350GQ"]
                 )
    Moved 3 variables.
    >>> discovery_check, discovery_cont = igem.epc.modify.move_variables(
                        discovery_check,
                        discovery_cont
                        )
    Moved 39 variables.
    """
    df_result = clarite.modify.merge_variables(left, right, skip, only)
    return df_result


def recode_values(
    data,
    replacement_dict,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Convert values in a dataframe.  By default, replacement occurs in all
    columns but this may be modified with 'skip' or 'only'.
    Pandas has more powerful 'replace' methods for more complicated scenarios.

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be processed and returned
    replacement_dict: dictionary
        A dictionary mapping the value being replaced to the value being
        inserted
    skip: str, list or None (default is None)
        List of variables that the replacement should *not* be applied to
    only: str, list or None (default is None)
        List of variables that the replacement should *only* be applied to

    Examples
    --------
    >>> import igem
    >>> igem.epc.modify.recode_values(
            df,
            {7: np.nan, 9: np.nan},
            only=['SMQ077', 'DBD100']
            )
    ================================================================================
    Running recode_values
    --------------------------------------------------------------------------------
    Replaced 17 values from 22,624 observations in 2 variables
    >>> igem.epc.modify.recode_values(df, {10: 12}, only=['SMQ077', 'DBD100'])
    ================================================================================
    Running recode_values
    --------------------------------------------------------------------------------
    No occurences of replaceable values were found, so nothing was replaced.
    """

    df_result = clarite.modify.recode_values(
        data, replacement_dict, skip, only
    )  # noqa E501
    return df_result


def remove_outliers(
    data,
    method: str = "gaussian",
    cutoff=3,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Remove outliers from continuous variables by replacing them with np.nan

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be processed and returned
    method: string, 'gaussian' (default) or 'iqr'
        Define outliers using a gaussian approach (standard deviations from
        the mean) or inter-quartile range
    cutoff: positive numeric, default of 3
        Either the number of standard deviations from the mean
        (method='gaussian') or the multiple of the IQR (method='iqr')
        Any values equal to or more extreme will be replaced with np.nan
    skip: str, list or None (default is None)
        List of variables that the replacement should *not* be applied to
    only: str, list or None (default is None)
        List of variables that the replacement should *only* be applied to

    Examples
    --------
    >>> import igem
    >>> nhanes_rm_outliers = igem.epc.modify.remove_outliers(
                nhanes,
                method='iqr',
                cutoff=1.5,
                only=['DR1TVB1',
                'URXP07',
                'SMQ077']
                )
    ================================================================================
    Running remove_outliers
    --------------------------------------------------------------------------------
    WARNING: 36 variables need to be categorized into a type manually
    Removing outliers from 2 continuous variables with values < 1st Quartile -
            (1.5 * IQR) or > 3rd quartile + (1.5 * IQR)
            Removed 0 low and 430 high IQR outliers from URXP07
                (outside -153.55 to 341.25)
            Removed 0 low and 730 high IQR outliers from DR1TVB1
                (outside -0.47 to 3.48)

    >>> nhanes_rm_outliers = igem.epc.modify.remove_outliers(
                nhanes,
                only=['DR1TVB1',
                'URXP07']
                )
    ================================================================================
    Running remove_outliers
    --------------------------------------------------------------------------------
    WARNING: 36 variables need to be categorized into a type manually
    Removing outliers from 2 continuous variables with values more than 3
            standard deviations from the mean
            Removed 0 low and 42 high gaussian outliers from URXP07
                (outside -1,194.83 to 1,508.13)
            Removed 0 low and 301 high gaussian outliers from DR1TVB1
                (outside -1.06 to 4.27)
    """
    df_result = clarite.modify.remove_outliers(
        data, method, cutoff, skip, only
    )  # noqa E501
    return df_result


def rowfilter_incomplete_obs(
    data,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Remove rows containing null values

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be processed and returned
    skip: str, list or None (default is None)
        List of columns that are not checked for null values
    only: str, list or None (default is None)
        List of columns that are the only ones to be checked for null values

    Returns
    -------
    data: pd.DataFrame
        The filtered DataFrame

    Examples
    --------
    >>> import igem
    >>> nhanes_filtered = igem.epc.modify.rowfilter_incomplete_obs(
                    nhanes,
                    only=[outcome] + covariates
                    )
    ================================================================================
    Running rowfilter_incomplete_obs
    --------------------------------------------------------------------------------
    Removed 3,687 of 22,624 observations (16.30%) due to NA values in any of 8
    """

    df_result = clarite.modify.rowfilter_incomplete_obs(data, skip, only)
    return df_result


def transform(
    data,
    transform_method: str,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Apply a transformation function to a variable

    Parameters
    ----------
    data: pd.DataFrame or pd.Series
        Data to be processed
    transform_method: str
        Name of the transformation (Python function or NumPy ufunc to apply)
    skip: str, list or None (default is None)
        List of variables that will *not* be transformed
    only: str, list or None (default is None)
        List of variables that are the *only* ones to be transformed

    Returns
    -------
    data: pd.DataFrame
        DataFrame with variables that have been transformed

    Examples
    --------
    >>> import igem
    >>> df = igem.epc.modify.transform(df, 'log', only=['BMXBMI'])
    ================================================================================
    Running transform
    --------------------------------------------------------------------------------
    Transformed 'BMXBMI' using 'log'.
    """

    df_result = clarite.modify.transform(
        data, transform_method, skip, only
    )  # noqa E501
    return df_result


def drop_extra_categories(
    data: pd.DataFrame,
    skip: Optional[Union[str, List[str]]] = None,
    only: Optional[Union[str, List[str]]] = None,
):
    """
    Update variable types to remove categories that don't occur in the data

    Parameters
    ----------
    data: pd.DataFrame or pd.Series
        Data to be processed
    skip: str, list or None (default is None)
        List of variables that will *not* be checked
    only: str, list or None (default is None)
        List of variables that are the *only* ones to be checked

    Returns
    -------
    data: pd.DataFrame
        DataFrame with categorical types updated as needed

    Examples
    --------
    >>> import igem
    >>> df = igem.epc.modify.drop_extra_categories(df, only=['SDDSRVYR'])
    ================================================================================
    Running drop_extra_categories
    --------------------------------------------------------------------------------
    SDDSRVYR had categories with no occurrences: 3, 4
    """

    df_result = clarite.modify.drop_extra_categories(data, skip, only)  # noqa E501
    return df_result
