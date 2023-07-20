"""
Describe
========

Functions that are used to gather information about some data

     .. autofunction:: correlations
     .. autofunction:: freq_table
     .. autofunction:: get_types
     .. autofunction:: percent_na
     .. autofunction:: skewness
     .. autofunction:: summarize

"""


import clarite
import pandas as pd


def correlations(data: pd.DataFrame, threshold: float = 0.75):
    """
    Return variables with pearson correlation above the threshold

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be described
    threshold: float, between 0 and 1
        Return a dataframe listing pairs of variables whose absolute value of
        correlation is above this threshold

    Returns
    -------
    result: pd.DataFrame
        DataFrame listing pairs of correlated variables and their correlation
        value

    Examples
    --------
    >>> import igem
    >>> correlations = igem.epc.describe.correlations(df, threshold=0.9)
    >>> correlations.head()
                        var1      var2  correlation
    0  supplement_count  DSDCOUNT     1.000000
    1          DR1TM181  DR1TMFAT     0.997900
    2          DR1TP182  DR1TPFAT     0.996172
    3          DRD370FQ  DRD370UQ     0.987974
    4          DR1TS160  DR1TSFAT     0.984733
    """

    df_result = clarite.describe.correlations(data, threshold)
    return df_result


def freq_table(data: pd.DataFrame):
    """
    Return the count of each unique value for all binary and categorical
    variables.  Other variables will return a single row with a value of
    '<Non-Categorical Values>' and the number of non-NA values.

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be described

    Returns
    -------
    result: pd.DataFrame
        DataFrame listing variable, value, and count for each categorical
        variable

    Examples
    --------
    >>> import igem
    >>> igem.epc.describe.freq_table(df).head(n=10)
        variable value  count
    0                 SDDSRVYR                         2   4872
    1                 SDDSRVYR                         1   4191
    2                   female                         1   4724
    3                   female                         0   4339
    4  how_many_years_in_house                         5   2961
    5  how_many_years_in_house                         3   1713
    6  how_many_years_in_house                         2   1502
    7  how_many_years_in_house                         1   1451
    8  how_many_years_in_house                         4   1419
    9                  LBXPFDO  <Non-Categorical Values>   1032
    """
    df_result = clarite.describe.freq_table(data)
    return df_result


def get_types(data: pd.DataFrame):
    """
    Return the type of each variable

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be described

    Returns
    -------
    result: pd.Series
        Series listing the IGEM type for each variable

    Examples
    --------
    >>> import igem
    >>> igem.epc.describe.get_types(df).head()
    RIDAGEYR          continuous
    female                binary
    black                 binary
    mexican               binary
    other_hispanic        binary
    dtype: object
    """

    df_result = clarite.describe.get_types(data)
    return df_result


def percent_na(data: pd.DataFrame):
    """
    Return the percent of observations that are NA for each variable

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be described

    Returns
    -------
    result: pd.DataFrame
        DataFrame listing percent NA for each variable

    Examples
    --------
    >>> import igem
    >>> igem.epc.describe.percent_na(df)
       variable  percent_na
    0  SDDSRVYR     0.00000
    1    female     0.00000
    2    LBXHBC     4.99321
    3    LBXHBS     4.98730
    """
    df_result = clarite.describe.percent_na(data)
    return df_result


def skewness(data: pd.DataFrame, dropna: bool = False):
    """
    Return the skewness of each continuous variable

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be described
    dropna: bool
        If True, drop rows with NA values before calculating skew.  Otherwise
        the NA values propagate.

    Returns
    -------
    result: pd.DataFrame
        DataFrame listing three values for each continuous variable and NA for
        others: skew, zscore, and pvalue.
        The test null hypothesis is that the skewness of the samples
        population is the same as the corresponding normal distribution.
        The pvalue is the two-sided pvalue for the hypothesis test

    Examples
    --------
    >>> import igem
    >>> igem.epc.describe.skewness(df)
         Variable         type      skew    zscore        pvalue
    0       pdias  categorical       NaN       NaN           NaN
    1   longindex  categorical       NaN       NaN           NaN
    2     durflow   continuous  2.754286  8.183515  2.756827e-16
    3      height   continuous  0.583514  2.735605  6.226567e-03
    4     begflow   continuous -0.316648 -1.549449  1.212738e-01
    """

    df_result = clarite.describe.skewness(data, dropna)
    return df_result


def summarize(data: pd.DataFrame):
    """
    Print the number of each type of variable and the number of observations

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame to be described

    Returns
    -------
    result: None

    Examples
    --------
    >>> import igem
    >>> igem.epc.describe.get_types(df).head()
    RIDAGEYR          continuous
    female                binary
    black                 binary
    mexican               binary
    other_hispanic        binary
    dtype: object
    """

    result = clarite.describe.summarize(data)
    return result
