Group
-----

The group master data acts as a qualitative characteristic for the :ref:`Term` being the highest level of the hierarchical structure, followed by the :ref:`CATEGORY` and then the :ref:`Term`.

The system uses the Group information as a filter in queries and other interfaces. An example of the use of Group will be the :ref:`Gene Exposome Report`, in which the system will use the Group to select which Term will be considered as Exposome.


The Group data will be stored in the ge_group table of the IGEM DB defined in the initial parameters. The available fields are:
    
    * *ID*: GE.db internal key
    * *group*: Abbreviated name of the Group
    * *Description*: Description for identifying and consulting the Group

The inclusion of new data can be performed via the process :code:`db` . On the command line::

$ python manage.py db --load_data "table='term_group, path='{your_path}/term_group.csv'"


Other commands and functions for manipulating master data can be found in the database management tab.


CAUTION: As GE.db is a correlational base with key integrity, all records linked to the deleted data will also be deleted, which includes Term and TermMap information




**Web Interface**

Through IGEM's friendly web interface, it will be possible to carry out Group management activities.

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



After user authentication and on the initial administration screen, select an option Keyge-Group.

.. image:: /_static/pictures/md_01_01_datasource.png
  :alt: Alternative text

On the Group screen, we will have options to consult, modify, add and eliminate Group.

.. image:: /_static/pictures/md_04_01_group.png
  :alt: Alternative text


On the first screen, we have a view of all available Group. To consult, click a desired Group.

.. image:: /_static/pictures/md_04_01_group.png
  :alt: Alternative text


On the next screen, we have all the Group fields open for modifications. To modify, change the desired information and select one of the three button options:
    * :code:`Save and add another`: Will save the changes and open a blank Group screen to add a new Group record.
    * :code:`Save and Continue editing`: Will save the changes and continue on the Group screen.
    * :code:`Save`: Will save the changes and return to the screen with the list of Group.

In the History button, we can consult all the modifications carried out in the Group, this function will be important to track modifications and audit the process.


The :code:`DELETE` button will permanently delete the Group record.

Caution: when deleting a Group, the system will also delete all records dependent on that Group, which include Term, and KEYLINKS

Deletion can also be performed en bloc. On the Group List screen, select all the GroupS you want to delete, choose the Delete Selected Keyge - Groups action and click on the :code:`GO` button.

Be careful, this elimination operation will be definitive for the GroupS and for all other records dependent on it, as already explained.

.. image:: /_static/pictures/md_04_03_group.png
  :alt: Alternative text

To add new Group, we will have three different ways:
    * by the :code:`+ Add` button on the left sidebar.
    * Through the :code:`ADD Group +` button in the right field of the GOUP list.
    * Via the :code:`Save and add another` button located within a Group record.


