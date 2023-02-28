"""
Load
========

Load data from different formats or sources

     .. autofunction:: from_tsv
     .. autofunction:: from_csv
"""

from typing import Optional, Union

import clarite


def from_tsv(
    filename: str, index_col: Optional[Union[str, int]] = 0, **kwargs
):  # noqa E501
    """
    Load data from a tab-separated file into a DataFrame

    Parameters
    ----------
    filename: str or Path
        File with data to be used in IGEM
    index_col: int or string (default 0)
        Column to use as the row labels of the DataFrame.
    **kwargs:
        Other keyword arguments to pass to pd.read_csv

    Returns
    -------
    DataFrame
        The index column will be used when merging

    Examples
    --------
    Load a tab-delimited file with an "ID" column

    >>> import igem
    >>> df = igem.epc.load.from_tsv('nhanes.txt', index_col="SEQN")
    Loaded 22,624 observations of 970 variables
    """

    return clarite.load.from_tsv(filename, index_col, **kwargs)


def from_csv(
    filename: str, index_col: Optional[Union[str, int]] = 0, **kwargs
):  # noqa E501
    """
    Load data from a comma-separated file into a DataFrame

    Parameters
    ----------
    filename: str or Path
        File with data to be used in IGEM
    index_col: int or string (default 0)
        Column to use as the row labels of the DataFrame.
    **kwargs:
        Other keyword arguments to pass to pd.read_csv

    Returns
    -------
    DataFrame
        The index column will be used when merging

    Examples
    --------
    Load a tab-delimited file with an "ID" column

    >>> import igem
    >>> df = igem.epc.load.from_csv('nhanes.txt', index_col="SEQN")
    Loaded 22,624 observations of 970 variables
    """

    return clarite.load.from_csv(filename, index_col, **kwargs)
