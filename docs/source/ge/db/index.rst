===================
Database Management
===================

With the DB process, it will be possible to carry out data extraction operations, data loading, deletion, and cleaning of GE.db tables

The available tables are:
   * datasource
   * connector
   * term_group
   * term_category
   * term
   * ds_column
   * prefix
   * wordterm
   * termmap
   * wordmap



Python function
---------------

**get_data**

   The get_data() function allows extracting data from the GE database
   and loading this data into a Pandas DataFrame structure or CSV File.

   It has an intelligent filter mechanism that allow you to perform data
   selections simply through a conversion layer of function arguments and SQL
   syntax. This allows the same input arguments regardless of implemented
   database management system.

   Parameters:
   
   Only the table parameter will be mandatory, the others being optional, and
   will model the data output. In the case of only informing the table, the
   function will return a DataFrame with all the columns and values of the
   table.

   - table: str
      datasource, connector, ds_column, term_group, term_category, term,
      prefix,  wordterm, termmap, wordmap
   - path: str
      With this parameter, the function will save the selected data
      in a file in the directory informed as the parameter argument. In this
      scenario, data will not be returned in the form of a Dataframe; only a
      Boolean value will be returned, informing whether the file was
      generated or not
   - columns: list[“str”]
      Columns that will be selected for output. They must be informed with
      the same name as the database. It is possible to load other data from
      other tables as long as it correlate. For example, suppose the table
      only has the term field and not the category field. In that case, you
      can inform as an argument: "term_id__term_category_id__category", the
      system selected the ID of the term, consulted the ID of the category
      in the Term table, and went to the Category table to choose the
      category
   - columns_out: list[“str”]
      If you want to rename the header of the output fields to more familiar
      names, you can use this parameter, passing the desired names in the
      same sequential sequence in the parameter columns
   - datasource: Dict{“str”:list[”str”]}
      Filter argument. It is used to filter datasource, with the dictionary
      key being the selection argument and the dictionary value being the
      datasources selected as the filter. Without this parameter, the
      function will return all datasources
   - connector: Dict{“str”:list[”str”]}
      Filter argument. It uses the same logic as the datasource, but applied
      to the connector field
   - word: Dict{“str”:list[”str”]}
      Filter argument. It uses the same logic as the datasource, but applied
      to the word field
   - term: Dict{“str”:list[”str”]}
      Filter argument. It uses the same logic as the datasource, but applied
      to the term field
   - term_category: Dict{“str”:list[”str”]}
      Filter argument. It uses the same logic as the datasource, but applied
      to the term_categorty field
   - term_group: Dict{“str”:list[”str”]}
      Filter argument. It uses the same logic as the datasource, but applied
      to the term_group field


   Return:
   
   Pandas Dataframe or Boolean (If the parameter path is informed, the
   function will generate the file; if successful, it will return the
   TRUE. Otherwise, it will return FALSE)

   Examples:
   
   >>> from igem.ge import db
   >>> db.get_data(
         table=”datasource”,
         datasource={“datasource__in”: [“ds_01”,”ds_02”]},
         columns=[“id”,”datasource”],
         columns_out=[“Datasource ID”, “Datasource Name”],
         path=”{your_path}/datasource.csv”
         )

   >>> df = db.get_data(
         table="connector",
         connector={"connector__start": ["conn_ds"]},
         datasource={"datasource_id__datasource__in": ["ds_01"]},
         columns=["connector", "status"]
         )

   >>> x = db.get_data(
         table="termmap",
         term={"term_id__term": "chem:c112297"},
         path="{your_path},
         )
      If x:
         print("file created")


**load_data**

   Loads data from a CSV file into the IGEM database. This process does
   not update existing data, it only inserts new records.

   Parameters:
   
   - table: str
      datasource, connector, ds_column, term_group, term_category, term,
      prefix, wordterm, termmap, wordmap
   - path: str
      full path and file name to load

   Layout of data file:

   - Datasource:
      (datasource, description, category, website)
   - Connector:
      (connector, datasource, description, update_ds, source_path,
      source_web, source_compact, source_file_name, source_file_format,
      source_file_sep, source_file_skiprow, target_file_name,
      target_file_format)
   - Ds_column:
      (connector, status, column_number, column_name, pre_value, single_word)
   - Term_group:
      (term_group, description)
   - Term_category:
      (term_category, description)
   - Term:
      (term, category, group, description)
   - Prefix:
      (pre_value)
   - Wordterm:
      (term, word, status, commute)
   - Termmap:
      (ckey, connector, term_1, term_2, qtd_links)
   - Wordmap:
      (cword, datasource, connector, term_1, term_2, word_1, word_2,
      qtd_links)

   We can generate an example file with the get_data() function and
   manipulate and load it with the new data.

   Return:
  
   Boolean: (TRUE if the process occurred without errors and FALSE if had
   some errors).

   Examples:
   
   >>> from igem.ge import db
   >>> db.load_data(
         table="datasource”
         path=”{your_path}/datasource.csv”
         )

**delete_data**

   Allows deleting a record from the given table. The deletion will be
   carried out in all records related to the informed parameter. For example,
   if we delete a datasource, the connectors, ds_columns, and termmap
   associated with the datasource will be deleted.

   Parameters:

   Only the table parameter will always be requested, the others will depend
   on the selected table, functioning as a record that will be eliminated.

   - table: str
   (datasource, connector, ds_column, term_group, term_category, term,
   prefix, wordterm, termmap, wordmap, workflow)
   - datasource: Dict{“str”:list[”str”]}
   - connector: Dict{“str”:list[”str”]}
   - word: Dict{“str”:list[”str”]}
   - term: Dict{“str”:list[”str”]}
   - term_category: Dict{“str”:list[”str”]}
   - term_group: Dict{“str”:list[”str”]}
   - prefix: Dict{“str”:list[”str”]}

   (Filter argument. It is used to filter the field, with the dictionary
   key being the selection argument and the dictionary value being the
   field selected as the filter. Without this parameter, the
   function will return all values of the field.)

   Return:

   Boolean: (TRUE if the process occurred without errors and FALSE if had
   some errors).

   Examples:

   >>> from igem.ge import db
   >>> db.delete_data(
         table='datasource',
         datasource={'datasource__in': [ds_01]}
         )


**truncate_table**

   will delete all records from a table, never use this function, with excess
   if the need is to restart a new instance of the database, free up log
   table space or in test environments.

   Parameters:
   
   - table: str
      (datasource, connector, dst, term_group, term_category, term,
      prefix,  wordterm, termmap, wordmap, workflow, logs)

   If inform table="all", the function will truncate all table on GE database.
   The other tables of the IGEM system will be maintained.

   Return:
   
   Boolean: (TRUE if the process occurred without errors and FALSE if had
   some errors).

   Examples:
   
   >>> from igem.ge import db
   >>> db.truncate_table(
            table='datasource'
            )


**backup**

   Backup the database with the internal keys. It can be performed at once
   for all GE.db tables

   Parameters:

   - table: str
      (datasource, connector, dst, term_group, term_category, term,
      prefix,  wordterm, termmap, wordmap, workflow, logs)
   - path_out: str
      Folder path to store the generated backup files

   If inform table="all", the function will backup all table on GE database.

   Return:

   Boolean: (TRUE if the process occurred without errors and FALSE if had
   some errors).

   Examples:

   >>> import igem
   >>> igem.ge.db.backup(
            table="",
            path_out="/root/back")


**restore**

   Restore the database with the internal keys. It can be performed at once
   for all GE.db tables

   Parameters:

   - table: str
      (datasource, connector, dst, term_group, term_category, term,
      prefix,  wordterm, termmap, wordmap, workflow, logs)
   - path_out: str
      Folder path to store the generated backup files

   If inform table="all", the function will restore all table on GE database.

   Return:

   Boolean: (TRUE if the process occurred without errors and FALSE if had
   some errors).

   Examples:

   >>> import igem
   >>> igem.ge.db.restore(
            table="",
            path_out="/root/back")



Command Line
------------

Within the parameters, inform the same ones used for the functions, as well as the arguments, example::

$ $ python manage.py db --get_data 'table="datasource", datasource={“datasource__in”: [“ds_01”,”ds_02”]}'



Get data::

$ python manage.py db --get_data {parameters}
    

Load data::

$ python manage.py db --load_data {parameters}


Delete data::

$ python manage.py db --delete_data {parameters}
    

Delete all table::

$ python manage.py db --truncate_table {parameters}
    

Backup (get data with internal ID)::   

$ python manage.py db --backup {parameters}
    

Restore (load data with internal ID)::

$ python manage.py db --restore {parameters}




