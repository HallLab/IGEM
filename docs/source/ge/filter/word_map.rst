Word Map
--------

.. function:: word_map(*args, **kwargs)
   :module: ge.filter

   Queries GE.db and returns links between words without terms.

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
      filter.word_map(
            path_in="../../file.csv",
            path_out="../../outcome.csv"
            )

   This function queries GE.db and generates results showing links between words without terms.
   
   The results can be saved in a specified output file path or returned as a DataFrame.



