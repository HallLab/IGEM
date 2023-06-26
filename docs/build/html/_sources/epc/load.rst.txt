Load
====

Load data from different formats or sources.

.. autofunction:: from_tsv
.. autofunction:: from_csv

.. autofunction:: from_tsv(filename, index_col=0, **kwargs)
   :noindex:

   Load data from a tab-separated file into a DataFrame.

   Parameters
   ----------
   filename : str or Path
       File with data to be used in IGEM.
   index_col : int or str, optional (default 0)
       Column to use as the row labels of the DataFrame.
   **kwargs : dict
       Other keyword arguments to pass to pd.read_csv.

   Returns
   -------
   DataFrame
       The index column will be used when merging.

   Examples
   --------
   Load a tab-delimited file with an "ID" column:

   .. code-block:: python

      import igem
      df = igem.epc.load.from_tsv('nhanes.txt', index_col="SEQN")
      Loaded 22,624 observations of 970 variables.

.. autofunction:: from_csv(filename, index_col=0, **kwargs)
   :noindex:

   Load data from a comma-separated file into a DataFrame.

   Parameters
   ----------
   filename : str or Path
       File with data to be used in IGEM.
   index_col : int or str, optional (default 0)
       Column to use as the row labels of the DataFrame.
   **kwargs : dict
       Other keyword arguments to pass to pd.read_csv.

   Returns
   -------
   DataFrame
       The index column will be used when merging.

   Examples
   --------
   Load a comma-separated file with an "ID" column:

   .. code-block:: python

      import igem
      df = igem.epc.load.from_csv('nhanes.txt', index_col="SEQN")
      Loaded 22,624 observations of 970 variables.

In the above example, the `from_tsv` function loads data from a tab-separated file, while the `from_csv` function loads data from a comma-separated file. Both functions return a DataFrame, where the index column is used for merging. The examples demonstrate how to use these functions to load files and provide information about the number of observations and variables loaded.