Keyge
-----

KEYGE is the main component in GE.db and GE.filter and was created to specify a search term in external data sources. A KEYGE can be assigned to a gene, a chromosome, an SNP, a disease, a chemical, an environmental factor, or any other term necessary to keep in the GE.db knowledge base.

A KEYGE will have as attributes the GROUP and CATEGORY records to qualify and group, helping during searches, queries, and analysis of the GE.db knowledge base.

A KEYGE inside GE.db can be kept as a code number, a prefix + code, or even a word, depending exclusively on the initial planning adopted. Thus allowing high flexibility in the use of the IGEM system.

The system has an interface for mapping external words to a KEYGE, with this link having several external combinations for a single KEYGE. The system does not allow mapping the same external word to more than one KEYGE, a process necessary to guarantee the integrity of the knowledge base. < < add KEYWORD link > >

As described in the introduction < < add link > >, the purpose of GE.db will be to search an external record for all KEYGEs found, correlate these KEYGEs and maintain a frequency and origin, allowing, like GE.filter, to perform searches for combinations between KEYGE in different external data sources quickly and easily.

The KEYGE data will be stored in the ge_keyge table of the IGEM DB defined in the initial parameters. The available fields are:
    * *ID*: GE.db internal key
    * *keyge*: Abbreviated name of the KEYGE
    * *Description*: Description for identifying and consulting the KEYGE
    * *Category_id*: foreign_key from ge_category 
    * *Group_id*: foreign_key from ge_group



The inclusion of new data can be performed via the process :code:`db` . On the command line::

$ python manage.py db --load keyge --path {path/filename}.csv


Example of the load file can be found in the folder src/load/md/keyge.csv


To list the KEYGE already registered, type the command line::
    
$ python manage.py db --show keyge


To download the KEYGE already registered, type the command line::
    
$ python manage.py db --download keyge --path {path/filename}.csv


To delete a specific KEYGE, type the command line::
    
$ python manage.py db --delete keyge --keyge {keyge Name}


To delete all KEYGE Table. type the command line::
    
$ python manage.py db --truncate keyge

CAUTION: As GE.db is a correlational base with key integrity, all records linked to the deleted data will also be deleted, which includes KEYLINK amd KEYWORD information



KEYGE Web Interface
^^^^^^^^^^^^^^^^^^^^^^

Through IGEM's friendly web interface, it will be possible to carry out KEYGE management activities.

Activate the IGEM web service if you have not already done so. Go to the /src/ folder and type the command line::

$ python manage.py runserver

.. image:: /_static/pictures/md_database_01.png
  :alt: Alternative text

If it returns a port error, you can specify a different port::

$ python manage.py runserver 8080

Access the address in the link provided in Starting development server. Significantly, this address may vary depending on the initial settings performed during installation.


After user authentication and on the initial administration screen, select an option Database.

.. image:: /_static/pictures/md_database_02.png
  :alt: Alternative text

On the Database screen, we will have options to consult, modify, add and eliminate KEYGE.

.. image:: /_static/pictures/md_keyge_01.png
  :alt: Alternative text


On the first screen, we have a view of all available KEYGE. To consult, click a desired KEYGE.

.. image:: /_static/pictures/md_keyge_02.png
  :alt: Alternative text


On the next screen, we have all the KEYGE fields open for modifications. To modify, change the desired information and select one of the three button options:
    * :code:`Save and add another`: Will save the changes and open a blank KEYGE screen to add a new KEYGE record.
    * :code:`Save and Continue editing`: Will save the changes and continue on the KEYGE screen.
    * :code:`Save`: Will save the changes and return to the screen with the list of KEYGE.

In the History button, we can consult all the modifications carried out in the KEYGE, this function will be important to track modifications and audit the process.

.. image:: /_static/pictures/md_keyge_03.png
  :alt: Alternative text

The :code:`DELETE` button will permanently delete the KEYGE record.

Caution: when deleting a KEYGE, the system will also delete all records dependent on that KEYGE, which include KEYWORDs, and KEYLINKs

Deletion can also be performed en bloc. On the KEYGE List screen, select all the KEYGE you want to delete, choose the Delete Selected Keyge action and click on the :code:`GO` button.

Be careful, this elimination operation will be definitive for the KEYGE and for all other records dependent on it, as already explained.

.. image:: /_static/pictures/md_keyge_04.png
  :alt: Alternative text

To add new KEYGE, we will have three different ways:
    * by the :code:`+ Add` button on the left sidebar.
    * Through the :code:`ADD KEYGE +` button in the right field of the KEYGE list.
    * Via the :code:`Save and add another` button located within a KEYGE record.

.. image:: /_static/pictures/md_keyge_05.png
  :alt: Alternative text

For the KEYGE, we will have two filter locations:
    * First located at the top of the KEYGE List screen where we can search broadly.
    * Second on the right sidebar, being able to select by CATEGORY and GROUP of KEYGE.
