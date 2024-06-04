===
ETL
===

The ETL process is responsible for fetching data from external sources, transforming it into a compatible standard, searching for term relationships, and writing the data to GE.db. It consists of five distinct phases to efficiently manage resources and ensure successful execution:

Collect: This phase involves gathering data from external sources.

Prepare: In this phase, the collected data is processed and prepared for further transformation and loading.

Map: The data is mapped to relevant terms and categories, establishing relationships between them.

Reduce: Unnecessary or redundant data is filtered out, ensuring that only relevant information is retained.

Workflow: This phase coordinates the entire ETL process, orchestrating the execution of the preceding phases.

Each phase is explained in detail in the respective files:


.. toctree::

   collect
   prepare
   map
   reduce
   workflow

To ensure a successful ETL process, proper parameterization and accurate master data entries are crucial. These files will guide you through each phase, helping you understand and execute the ETL process effectively.