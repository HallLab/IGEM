

------------


------------

IGEM: 
==============================================================================

* Free software: 3-clause BSD license

Examples
--------
**Main screen for master data registration:**

.. image:: docs/source/_static/example/web_add.png

|

**The query for links between terms found while ingesting an external dataset:**

.. image:: docs/source/_static/example/web_key.png


Installation
------------
The IGEM system ran on Python and was built on DJANGO for database management and web interface.

To run the system, copy the /src folder to the desired location with access to Python > 3.7 and the packages described in the requirements.

The IGEM system can use any database supported by Django. You will need to set the database in the /src/src/settings.py file.

To initialize the database, type in the /src/src folder:
    > python manage.py migration

To start the sytem on web, type in the /src/src folder:
    > python manage.py runserver

Questions
---------

feel free to open an `Issue <https://github.com/HallLab/igem/issues>`_.

Citing IGEM
--------------


https://igem.readthedocs.io/en/latest/