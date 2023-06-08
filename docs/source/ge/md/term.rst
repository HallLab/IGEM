Term
-----

Term is the main component in GE.db and GE.filter and was created to specify a search term in external data sources. A Term can be assigned to a gene, a chromosome, an SNP, a disease, a chemical, an environmental factor, or any other term necessary to keep in the GE.db knowledge base.

A Term will have as attributes the Group and Category records to qualify and group, helping during searches, queries, and analysis of the GE.db knowledge base.

A Term inside GE.db can be kept as a code number, a prefix + code, or even a word, depending exclusively on the initial planning adopted. Thus allowing high flexibility in the use of the IGEM system.

The system has an interface for mapping external words to a Term, with this link having several external combinations for a single Term. The system does not allow mapping the same external word to more than one Term, a process necessary to guarantee the integrity of the knowledge base.

As described in the introduction, the purpose of GE.db will be to search an external record for all Terms found, correlate these Terms and maintain a frequency and origin, allowing, like GE.filter, to perform searches for combinations between Term in different external data sources quickly and easily.

The Term data will be stored in the ge_keyge table of the IGEM DB defined in the initial parameters. The available fields are:

    * *ID*: GE.db internal key
    * *Term*: Abbreviated name of the Term
    * *Description*: Description for identifying and consulting the Term
    * *Category_id*: foreign_key from ge_category 
    * *Group_id*: foreign_key from ge_group



The inclusion of new data can be performed via the process :code:`db` . On the command line::

$ python manage.py db --load_data "table='term, path='{your_path}/term.csv'"


Other commands and functions for manipulating master data can be found in the database management tab.


CAUTION: As GE.db is a correlational base with key integrity, all records linked to the deleted data will also be deleted, which includes TermMap amd WordMap information



**Web Interface**

Through IGEM's friendly web interface, it will be possible to carry out Term management activities.

Activate the IGEM web service if you have not already done so. Go to the IGEM folder and type the command line::

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


After user authentication and on the initial administration screen, select an option Database.

.. image:: /_static/pictures/md_01_01_datasource.png
  :alt: Alternative text

On the Database screen, we will have options to consult, modify, add and eliminate Term.

.. image:: /_static/pictures/md_05_01_term.png
  :alt: Alternative text


On the first screen, we have a view of all available Term. To consult, click a desired Term.

.. image:: /_static/pictures/md_05_02_term.png
  :alt: Alternative text


On the next screen, we have all the Term fields open for modifications. To modify, change the desired information and select one of the three button options:
    * :code:`Save and add another`: Will save the changes and open a blank Term screen to add a new Term record.
    * :code:`Save and Continue editing`: Will save the changes and continue on the Term screen.
    * :code:`Save`: Will save the changes and return to the screen with the list of Term.

In the History button, we can consult all the modifications carried out in the Term, this function will be important to track modifications and audit the process.


The :code:`DELETE` button will permanently delete the Term record.

Caution: when deleting a Term, the system will also delete all records dependent on that Term, which include KEYWORDs, and KEYLINKs

Deletion can also be performed en bloc. On the Term List screen, select all the Term you want to delete, choose the Delete Selected Term action and click on the :code:`GO` button.

Be careful, this elimination operation will be definitive for the Term and for all other records dependent on it, as already explained.

.. image:: /_static/pictures/md_05_03_term.png
  :alt: Alternative text

To add new Term, we will have three different ways:
    * by the :code:`+ Add` button on the left sidebar.
    * Through the :code:`ADD Term +` button in the right field of the Term list.
    * Via the :code:`Save and add another` button located within a Term record.


For the Term, we will have two filter locations:
    * First located at the top of the Term List screen where we can search broadly.
    * Second on the right sidebar, being able to select by Category and Group of Term.
