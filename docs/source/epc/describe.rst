Describe
========

Functions that are used to gather information about some data.

.. autofunction:: correlations
.. autofunction:: freq_table
.. autofunction:: get_types
.. autofunction:: percent_na
.. autofunction:: skewness
.. autofunction:: summarize

.. autofunction:: correlations(data: pd.DataFrame, threshold: float = 0.75)
   :noindex:

   Return variables with Pearson correlation above the threshold.

   Parameters
   ----------
   data : pd.DataFrame
       The DataFrame to be described.
   threshold : float, optional (default 0.75)
       Return a DataFrame listing pairs of variables whose absolute value of
       correlation is above this threshold.

   Returns
   -------
   result : pd.DataFrame
       DataFrame listing pairs of correlated variables and their correlation value.

   Examples
   --------
   Calculate correlations between variables with a threshold of 0.9:

   .. code-block:: python

      import igem
      correlations = igem.epc.describe.correlations(df, threshold=0.9)
      correlations.head()

.. autofunction:: freq_table(data: pd.DataFrame)
   :noindex:

   Return the count of each unique value for all binary and categorical variables.
   Other variables will return a single row with a value of '<Non-Categorical Values>'
   and the number of non-NA values.

   Parameters
   ----------
   data : pd.DataFrame
       The DataFrame to be described.

   Returns
   -------
   result : pd.DataFrame
       DataFrame listing variable, value, and count for each categorical variable.

   Examples
   --------
   Generate a frequency table for categorical variables:

   .. code-block:: python

      import igem
      freq_table = igem.epc.describe.freq_table(df)
      freq_table.head(n=10)

.. autofunction:: get_types(data: pd.DataFrame)
   :noindex:

   Return the type of each variable.

   Parameters
   ----------
   data : pd.DataFrame
       The DataFrame to be described.

   Returns
   -------
   result : pd.Series
       Series listing the IGEM type for each variable.

   Examples
   --------
   Get the types of variables:

   .. code-block:: python

      import igem
      types = igem.epc.describe.get_types(df)
      types.head()

.. autofunction:: percent_na(data: pd.DataFrame)
   :noindex:

   Return the percent of observations that are NA for each variable.

   Parameters
   ----------
   data : pd.DataFrame
       The DataFrame to be described.

   Returns
   -------
   result : pd.DataFrame
       DataFrame listing percent NA for each variable.

   Examples
   --------
   Calculate the percent of missing values for each variable:

   .. code-block:: python

      import igem
      percent_na = igem.epc.describe.percent_na(df)
      percent_na

.. autofunction:: skewness(data: pd.DataFrame, dropna: bool = False)
   :noindex:

   Return the skewness of each continuous variable.

   Parameters
   ----------
   data : pd.DataFrame
       The DataFrame to be described.
   dropna : bool, optional (default False)
       If True, drop rows with NA values before calculating skew. Otherwise,
       the NA values propagate.

   Returns
   -------
   result : pd.DataFrame
       DataFrame listing three values for each continuous variable and NA for others:
       skew, zscore, and pvalue.
       The test null hypothesis is that the skewness of the samples population is
       the same as the corresponding normal distribution.
       The pvalue is the two-sided p-value for the hypothesis test.

   Examples
   --------
   Calculate skew

ness for continuous variables:

   .. code-block:: python

      import igem
      skewness = igem.epc.describe.skewness(df)
      skewness

.. autofunction:: summarize(data: pd.DataFrame)
   :noindex:

   Print the number of each type of variable and the number of observations.

   Parameters
   ----------
   data : pd.DataFrame
       The DataFrame to be described.

   Returns
   -------
   result : None

   Examples
   --------
   Print a summary of the DataFrame:

   .. code-block:: python

      import igem
      igem.epc.describe.summarize(df)