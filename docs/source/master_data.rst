=========================
Master Data Customization
=========================

The master data have a primordial function for the system's functioning, being responsible for directing the origin of the data and for filtering and linking the words to the knowledge base.

Before starting the data collection, it will be necessary to parameterize the Master Data, which can be done via batch or individually in a web interface.

Master data will configure in the following order:
    * DATABASE
    * KEYGE-PREFIX
    * DATASET
    * KEYGE-GROUP
    * KEYGE-CATEGORY
    * KEYGE
    * KEYGE-WORD

Next, we will detail each master data.



DATABASE
--------

A DATABASE record points to a specific external dataset, having authentication information, hosted data category, and description. It will reference in the DATASET (subsequent master data).

Access via the web will be at *http://127.0.0.1:8000/admin/ge/database/*

Batch data import will be through the command::

    $ Python manage.py db load --database --path …/__.csv

Before running the batch import command, the file must be saved in */loader/database.csv*

Batch import will disregard all repeated or existing records in GE.db.



DATABASE Web Interface
^^^^^^^^^^^^^^^^^^^^^^
Descrever como realizar a atualizazao via web interface

.. image:: docs/source/_static/example/web_add.png



DATASET
-------

DATASET Web Interface
^^^^^^^^^^^^^^^^^^^^^



KEYGE-PREFIX
------------

KEYGE-PREFIX Web Interface
^^^^^^^^^^^^^^^^^^^^^^^^^^



KEYGE-GROUP
------------

KEYGE-GROUP Web Interface
^^^^^^^^^^^^^^^^^^^^^^^^^