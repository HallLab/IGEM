===================
Database Management
===================

The Database Management within the GE module provides two main functions:
   * Direct access to database tables for retrieving information
   * Synchronization of the IGEM Client DB with the latest data from the Hall Lab DB Server.


Direct Access to GE.db Tables
-----------------------------

Enables direct access to the database tables, allowing users to retrieve information directly from the IGEM Client DB.

This functionality provides a convenient way to query and analyze the data stored in the database tables. 

By leveraging this function, users can efficiently retrieve specific information from the IGEM Client DB and utilize it for their research and analysis purposes.

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


Command Line

Within the parameters, inform the same ones used for the functions, as well as the arguments, example::

$ $ python manage.py db --get_data 'table="datasource", datasource={“datasource__in”: [“ds_01”,”ds_02”]}'


Get data::

$ python manage.py db --get_data {parameters}
    


Synchronization with the Hall Lab DB Server
-------------------------------------------

The second function of the Database Management is to synchronize the IGEM Client DB with the latest data from the Hall Lab DB Server.

This synchronization process ensures that the IGEM Client DB is up to date with the most recent information available.

The function offers both offline and online synchronization options.

Offline Sync:
   In the offline synchronization mode, users manually acquire the necessary DB files from a designated source. They can obtain the latest versions of the DB files from an authorized repository and update the IGEM Client DB accordingly. This mode is suitable for situations where internet connectivity is limited or when users prefer to have full control over the synchronization process.
   Examples:
   
   >>> from igem.ge import db
   >>> db.db.sync_db(table="all", source="{your_path}")


Online Sync:
   The online synchronization mode automates the process of fetching the latest data from the web repository. The submodule accesses the web repository and retrieves the most recent versions of the DB files, ensuring that the IGEM Client DB is synchronized with the Hall Lab DB Server. This mode is ideal for users who prefer a seamless and automated synchronization process, without the need for manual intervention.
   Examples:
   
   >>> from igem.ge import db
   >>> db.db.sync_db(table="all")


The GE.db submodule provides researchers with a comprehensive set of tools to access and synchronize the IGEM Client DB. Whether it's directly querying database tables or ensuring up-to-date information through synchronization, this submodule facilitates efficient data management and enhances the research capabilities of users.
