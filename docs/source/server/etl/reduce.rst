Reduce
------

Last step of the process. It has a mechanism to find Term (terms) per line called Mapper and then activate the Reducer subprocess that will count the number of links found in the connector. After all processing, the result will be recorded in the Keylinks table. It is important to note that the new data will fully replace the previous data in the processed connector.::
    $ python manage.py etl --reduce {all or connector}

Essential to have the file in PSA. Otherwise, the system will display a warning. It will start the MapReduce phase of data terms for all Connector or just a specific one. In this phase, there is a large consumption of memory and processing, so it will be essential to allocate resources compatible with the size of the processed data.::

Reset option will Press the control to all or a specific connector in the current phase.

In all commands with run argument, possible multiprocessing, and control for file chunks. However, it will be necessary or necessary between the size of the extracted files and the resources allocated, such as memory and the amount of proposed balancing.
