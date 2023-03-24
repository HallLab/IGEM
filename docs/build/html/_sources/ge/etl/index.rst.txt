===
ETL
===

The ETL process (Extraction, Transformation, and Loader) will be the step in which the system will fetch data from external sources, treat this data to a compatible standard, search for term relationships and write to GE.db. The entire process takes place based on briefly configured parameters and master data, so it is essential to pay attention to parameterization and master data entries.
For better resource management during the ETL phase, the process workflow was divided into five distinct phases:


.. toctree::

   collect
   prepare
   map
   reduce
   workflow