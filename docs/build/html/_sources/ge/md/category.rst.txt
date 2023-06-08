Category
--------

Category master data acts as a grouping of :ref:`Term` at a lower level than the :ref:`Group`.

The system uses the Category information as a filter in queries and other interfaces. An example of the use of Category will be the :ref:`Gene Exposome Report`, in which the system will use the Category to select all Gene :ref:`KEYGE`.


The Category data will be stored in the ge_category table of the IGEM DB defined in the initial parameters. The available fields are:
    
    * *ID*: GE.db internal key
    * *Category*: Abbreviated name of the Category
    * *Description*: Description for identifying and consulting the Category


The inclusion of new data can be performed via the process :code:`db` . On the command line::

$ $ python manage.py db --load_data "table='term_category, path='{your_path}/term_category.csv'"


Other commands and functions for manipulating master data can be found in the database management tab.


CAUTION: As GE.db is a correlational base with key integrity, all records linked to the deleted data will also be deleted, which includes Term and TermMap information



**Web Interface**

Through IGEM's friendly web interface, it will be possible to carry out GROUP management activities.

Activate the IGEM web service if you have not already done so. Go to the igem folder and type the command line::

$ python manage.py runserver
>>> Watching for file changes with StatReloader
  Performing system checks...
  System check identified no issues (0 silenced).
  March 24, 2023 - 12:56:26
  Django version 4.1.5, using settings 'src.settings'
  Starting development server at http://127.0.0.1:8000/
  Quit the server with CONTROL-C.

If it returns a port error, you can specify a different port::

$ python manage.py runserver 8080

Access the address in the link provided in Starting development server. Significantly, this address may vary depending on the initial settings performed during installation.



After user authentication and on the initial administration screen, select an option Term-Category.

.. image:: /_static/pictures/md_01_01_datasource.png
  :alt: Alternative text

On the Category screen, we will have options to consult, modify, add and eliminate Category.

.. image:: /_static/pictures/md_03_01_category.png
  :alt: Alternative text


On the first screen, we have a view of all available Category. To consult, click a desired Category.

.. image:: /_static/pictures/md_03_02_category.png
  :alt: Alternative text


On the next screen, we have all the Category fields open for modifications. To modify, change the desired information and select one of the three button options:
    * :code:`Save and add another`: Will save the changes and open a blank Category screen to add a new Category record.
    * :code:`Save and Continue editing`: Will save the changes and continue on the Category screen.
    * :code:`Save`: Will save the changes and return to the screen with the list of Category.

In the History button, we can consult all the modifications carried out in the Category, this function will be important to track modifications and audit the process.


The :code:`DELETE` button will permanently delete the Category record.

Caution: when deleting a Category, the system will also delete all records dependent on that Category, which include KEYGE, and KEYLINKS

Deletion can also be performed en bloc. On the Category List screen, select all the Category you want to delete, choose the Delete Selected Keyge - Category action and click on the :code:`GO` button.

Be careful, this elimination operation will be definitive for the Category and for all other records dependent on it, as already explained.

.. image:: /_static/pictures/md_03_03_category.png
  :alt: Alternative text

To add new Category, we will have three different ways:
    * by the :code:`+ Add` button on the left sidebar.
    * Through the :code:`ADD Category +` button in the right field of the Category list.
    * Via the :code:`Save and add another` button located within a Category record.
