Term Map
--------

.. function:: term_map(*args, **kwargs)
   :module: ge.filter

   TermMap table query function.

   Parameters
   ----------
   - ``path_in``: str
       parameter file path with filter information, aggregation, and result file path.
   - ``path_out``: str
       result file path.
   - ``term``: list[str]
       List of terms to filter passed through the function. If you inform the file with the parameters,
       the values passed by this parameter will be disregarded.

   Return
   ------
   It may return a boolean value if you have informed an output per file (``path_out``) or a DataFrame
   if you have not informed an output file.

   Examples:
   --------
   ::

      from igem.ge import filter
      filter.term_map(
            path_in="../../file.csv",
            path_out="../../outcome.csv"
            )
      df_result = filter.term_map(
            term=["gene:246126"]
            )

   This function queries the TermMap table in GE.db and retrieves relationships between terms. The results
   can be saved in a specified output file path or returned as a DataFrame.



