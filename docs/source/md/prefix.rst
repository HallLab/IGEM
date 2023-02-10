Prefix
------

Prefixes play a fundamental role in the logic of the IGEM system for the correct identification of ref:`KEYGE`.

Prefixes are assigned as input structure columns by :ref:`DATASET`. Necessary due to identification only by the code of terms mapped to :ref:`KEYGE` and that however conflict with other categories of :ref:`KEYGE`.

For the correct identification of the :ref:`KEYGE`, the system will add a prefix to the code located in the source DATASET before MAPREDUCE processing.

Keeping without a record in none, it is important that the record is used during the IGEM ETL where additional cases from prefix to source code do not occur.

The PREFIX data will be stored in the ge_prefixopc table of the IGEM DB defined in the initial parameters. The available fields are:
    * *pre_value*: prefix name


The inclusion of new data can be performed via the process :code:`db` . On the command line::

$ python manage.py db --load prefix --path {path/filename}.csv


Example of the load file can be found in the folder src/load/md/prefix.csv


To list the PREFIX already registered, type the command line::
    
$ python manage.py db --show prefix


To download the PREFIX already registered, type the command line::
    
$ python manage.py db --download prefix --path {path/filename}.csv


To delete a specific PREFIX, type the command line::
    
$ python manage.py db --delete prefix --prefix {Prefix}


To delete all PREFIX Table. type the command line::
    
$ python manage.py db --truncate prefix

CAUTION: As GE.db is a correlational base with key integrity, all records linked to the deleted data will also be deleted, which includes DATASET columns rules.



PREFIX Web Interface
^^^^^^^^^^^^^^^^^^^^^^

Through IGEM's friendly web interface, it will be possible to carry out GROUP management activities.

Activate the IGEM web service if you have not already done so. Go to the /src/ folder and type the command line::

$ python manage.py runserver

.. image:: /_static/pictures/md_database_01.png
  :alt: Alternative text

If it returns a port error, you can specify a different port::

$ python manage.py runserver 8080

Access the address in the link provided in Starting development server. Significantly, this address may vary depending on the initial settings performed during installation.



After user authentication and on the initial administration screen, select an option Keyge-Prefix.

.. image:: /_static/pictures/md_database_02.png
  :alt: Alternative text

On the Prefix screen, we will have options to consult, modify, add and eliminate PREFIX.

.. image:: /_static/pictures/md_prefix_01.png
  :alt: Alternative text


On the first screen, we have a view of all available PREFIX. To consult, click a desired PREFIX.

.. image:: /_static/pictures/md_prefix_02.png
  :alt: Alternative text


On the next screen, we have all the PREFIX fields open for modifications. To modify, change the desired information and select one of the three button options:
    * :code:`Save and add another`: Will save the changes and open a blank PREFIX screen to add a new PREFIX record.
    * :code:`Save and Continue editing`: Will save the changes and continue on the PREFIX screen.
    * :code:`Save`: Will save the changes and return to the screen with the list of PREFIX.

In the History button, we can consult all the modifications carried out in the PREFIX, this function will be important to track modifications and audit the process.

.. image:: /_static/pictures/md_prefix_03.png
  :alt: Alternative text

The :code:`DELETE` button will permanently delete the PREFIX record.

Caution: when deleting a PREFIX, the system will also delete all records dependent on that PREFIX, which include DATASET Columns Rules

Deletion can also be performed en bloc. On the PREFIX List screen, select all the PREFIX you want to delete, choose the Delete Selected Keyge - Prefix action and click on the :code:`GO` button.

Be careful, this elimination operation will be definitive for the PREFIX and for all other records dependent on it, as already explained.

To add new PREFIX, we will have three different ways:
    * by the :code:`+ Add` button on the left sidebar.
    * Through the :code:`ADD PREFIX OPC +` button in the right field of the PREFIX list.
    * Via the :code:`Save and add another` button located within a PREFIX record.
