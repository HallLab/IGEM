Parameters File
---------------

.. function:: parameters_file(path=None)
   :module: ge.filter

   generates a model file to be used as a parameter file in query functions

   Parameters
   ----------
   - ``path``: str
       path where the file will be generated.

   File layout
   -----------
   In the file structure, new lines for the index filter can be included with additional values,
   and each filter line must contain only a single value. The output index and path must be unique,
   as they will be applied to the entire corresponding field (parameter).

   In the example below, let's select all terms from two data sources from a single group. Also,
   the Datasource and Connector fields will be aggregated and will not appear on the results

   ::

      index,parameter,value
      filter,datasource,ds_01
      filter,datasource,ds_02
      filter,connector,
      filter,term_group,Chemical
      filter,term_category,
      filter,word,
      output,datasource,no
      output,connector,no
      output,term_group,
      output,term_category,
      output,term,
      output,word,no
      path,path,/../output_file.csv

   Return
   ------
   it return a boolean value if the file was created

   Examples:
   --------
   ::

      from igem.ge import filter
      filter.parameters_file(
            path="../../folder"
            )

   This function generates a file template with parameters created in the specified path.
