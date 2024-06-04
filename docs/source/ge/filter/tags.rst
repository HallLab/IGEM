Tags
----

A TAG in the context of the GE.Filter function is a unique identifier that helps you track and identify the version of the external dataset used in the IGEM query.
It serves as a reference to the specific dataset version, allowing you to reproduce the same query in the future with the same dataset version.

When you process (ETL) a specific external dataset, the TAG will indicate the version of the dataset used. For example, if you process one external dataset with version or ETAG 1, the TAG will show that the data comes from this dataset.

In case the IGEM database is updated with a newer version of the external dataset, the TAG will reflect the most recent version. This helps researchers know which external dataset was used in their IGEM query and enables them to control and ensure the consistency of results when they want to replicate the same query in the future.

By referring to the TAG, researchers can track and document the specific dataset version used, providing transparency and facilitating reproducibility in their research.

In summary, the TAG serves as a unique identifier that indicates the version of the external dataset used in the IGEM query, allowing researchers to reproduce the same query with the same dataset version in the future.

.. function:: create_tag(connectors)
   :module: ge.filter

   Function to create a TAG with Current connector in TermMap. The IDs generated in the TAG are the WFControl table IDs with Current status.

   Parameters
   ----------
   - ``connectors``: list
       List of connector names.

   Returns
   -------
   str
       Generated TAG string.

   This function generates a TAG string based on the current connector in TermMap. It takes a list of connector names as input and retrieves the last IDs with a "Current" status from the WFControl table. It then generates a TAG string using the retrieved IDs. The TAG string helps researchers know which external dataset was used in the IGEM query and provides control to use the same version of the dataset in future consultations to IGEM.

   The generated TAG string follows the format "GE.db-TAG:<tag_ids>", where "<tag_ids>" is a hyphen-separated string of the WFControl table IDs.

.. function:: get_tag(tag)
   :module: ge.filter

   Function to retrieve WFControl data based on a TAG.

   Parameters
   ----------
   - ``tag``: str
       TAG string.

   Returns
   -------
   pd.DataFrame
       DataFrame with WFControl data.

   This function retrieves WFControl data based on a TAG string. It takes a TAG string as input and parses the TAG to obtain the corresponding WFControl table IDs. It then retrieves the WFControl data for the specified IDs and returns it as a DataFrame.

.. function:: get_tag_data(tag, path)
   :module: ge.filter

   Function to save TermMap and WFControl data to CSV files based on a TAG.

   Parameters
   ----------
   - ``tag``: str
       TAG string.
   - ``path``: str
       Path to save the files.

   Returns
   -------
   bool
       True if the files were successfully created, False otherwise.

   This function saves the TermMap and WFControl data associated with a TAG string to CSV files. It takes a TAG string and a path as input. First, it retrieves the corresponding WFControl data using the ``get_tag`` function. Then, it retrieves the TermMap data related to the connector IDs in the WFControl data. Finally, it saves both the TermMap and WFControl data to separate compressed CSV files in the specified path. The function returns True if the files were successfully created and False otherwise.
