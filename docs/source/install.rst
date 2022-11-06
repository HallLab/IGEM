Install
-------


The IGEM system has a modular architecture divided into two groups: 1. Components that host services such as File System Management, Database, and WEB Interfaces; 2. APPs have been developed until now, the APP GE.
The GE APP hosts processes which are currently available:
•	Data Workflow
o	collector
o	
prepare
o	map
o	reduce
•	Maintenance
o	show
o	load
o	download
o	delete
o	truncate
o	runserver
o	createsuperuser
o	makemigrations
o	migrate
•	Filter and Queries
o	filter
We don't have an installation package yet; we need to copy the source files to a directory. IGEM is available in the ICDS environment at /gpfs/group/mah546/default/sw/igem, with the following folder structure:
•	/igem_db/: hosts the POSTGRES database structure and the system base in igemdb.
•	/igem_env/: hosts the conda virtual environment files.
•	/src/: hosts the system and supports folders.
Inside the src directory, we will have:
•	/ge/: all source codes and interfaces for the functioning of APP GE.
•	/loader/: all input files for loading master data and output directory of the FILTER process.
•	/psa/: Persist Store Area to store the database files downloaded and processed by the ETL process. Each DATASET will have its subfolder within the PSA.
•	/src/: hosts the source code of IGEM components, configurations and parameterizations.
•	/templates/: hosts the common web interfaces in IGEM.
The IGEM system is modular in terms of the database choice. The only problem is that it needs a setting, and it has simple user behavior. It is always necessary to instantiate it before its use. But in a cloud-as-a-service environment, it delivers better performance and multi-users compared to SQLite. For the ICDS project, the choice was POSTGRES, which has better characteristics for the need for GE.db, allowing better performance and security in HPC environments.
The database configuration is in the settings.py source file at :


- Install
    - BD Schema
    - MakeMigrations and Migrate


.. image:: /_static/pictures/install_01.png
  :alt: Alternative text