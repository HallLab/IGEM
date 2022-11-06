Category
--------

Category master data acts as a grouping of :ref:`KEYGE` at a lower level than the :ref:`GROUP`.

The system uses the CATEGORY information as a filter in queries and other interfaces. An example of the use of CATEGORY will be the :ref:`Gene Exposome Report`, in which the system will use the CATEGORY to select all Gene :ref:`KEYGE`.


The CATEGORY data will be stored in the ge_category table of the IGEM DB defined in the initial parameters. The available fields are:
    * *ID*: GE.db internal key
    * *group*: Abbreviated name of the CATEGORY
    * *Description*: Description for identifying and consulting the CATEGORY


The inclusion of new data can be performed via the process :code:`db` . On the command line::

$ python manage.py db --load category --path {path/filename}.csv


Example of the load file can be found in the folder src/load/md/category.csv


To list the CATEGORY already registered, type the command line::
    
$ python manage.py db --show category


To download the CATEGORY already registered, type the command line::
    
$ python manage.py db --download category --path {path/filename}.csv


To delete a specific CATEGORY, type the command line::
    
$ python manage.py db --delete category --category {Category Abbreviated Name}


To delete all CATEGORY Table. type the command line::
    
$ python manage.py db --truncate category

CAUTION: As GE.db is a correlational base with key integrity, all records linked to the deleted data will also be deleted, which includes KEYGE and KEYGELINKS information



CATEGORY Web Interface
^^^^^^^^^^^^^^^^^^^^^^

Through IGEM's friendly web interface, it will be possible to carry out GROUP management activities.

Activate the IGEM web service if you have not already done so. Go to the /src/ folder and type the command line::

$ python manage.py runserver

.. image:: /_static/pictures/md_database_01.png
  :alt: Alternative text

If it returns a port error, you can specify a different port::

$ python manage.py runserver 8080

Access the address in the link provided in Starting development server. Significantly, this address may vary depending on the initial settings performed during installation.



After user authentication and on the initial administration screen, select an option Keyge-CATEGORY.

.. image:: /_static/pictures/md_database_02.png
  :alt: Alternative text

On the Category screen, we will have options to consult, modify, add and eliminate Category.

.. image:: /_static/pictures/md_category_01.png
  :alt: Alternative text


On the first screen, we have a view of all available CATEGORY. To consult, click a desired CATEGORY.

.. image:: /_static/pictures/md_category_02.png
  :alt: Alternative text


On the next screen, we have all the CATEGORY fields open for modifications. To modify, change the desired information and select one of the three button options:
    * :code:`Save and add another`: Will save the changes and open a blank CATEGORY screen to add a new CATEGORY record.
    * :code:`Save and Continue editing`: Will save the changes and continue on the CATEGORY screen.
    * :code:`Save`: Will save the changes and return to the screen with the list of CATEGORY.

In the History button, we can consult all the modifications carried out in the CATEGORY, this function will be important to track modifications and audit the process.

.. image:: /_static/pictures/md_category_03.png
  :alt: Alternative text

The :code:`DELETE` button will permanently delete the CATEGORY record.

Caution: when deleting a CATEGORY, the system will also delete all records dependent on that CATEGORY, which include KEYGE, and KEYLINKS

Deletion can also be performed en bloc. On the CATEGORY List screen, select all the CATEGORY you want to delete, choose the Delete Selected Keyge - Category action and click on the :code:`GO` button.

Be careful, this elimination operation will be definitive for the CATEGORY and for all other records dependent on it, as already explained.

.. image:: /_static/pictures/md_category_04.png
  :alt: Alternative text

To add new CATEGORY, we will have three different ways:
    * by the :code:`+ Add` button on the left sidebar.
    * Through the :code:`ADD CATEGORY +` button in the right field of the CATEGORY list.
    * Via the :code:`Save and add another` button located within a CATEGORY record.

.. image:: /_static/pictures/md_category_05.png
  :alt: Alternative text
