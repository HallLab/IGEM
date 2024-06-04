Words to Terms
--------------

.. function:: word_to_term(path=None)
   :module: ge.filter

   Perform a search for terms from a string base with the same ETL engine.

   Parameters
   ----------
   - ``path``: str
       File with the strings for conversion into terms. Only the first column of the file will be processed.

   Return
   ------
   A file will be generated with the results in the same folder as the input strings file.

   Examples:
   --------
   ::

      from igem.ge import filter
      filter.word_to_term(
            path='../../file.csv'
            )

   This function searches for terms from a string base using the ETL engine. It takes a file path as input,
   reads the strings from the file, and converts them into terms. The results are saved in a CSV file in the
   same folder as the input file.



