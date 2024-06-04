Collect
=======

The "Collect" process is responsible for selecting active connectors and checking if new versions of data are available. It performs the following tasks:

Connector Selection: The process selects active connectors to fetch data from various sources.

Data Extraction: If a new version of the data is available, the process extracts the latest data.

File Handling: If necessary, the extracted file is uncompressed and stored in the Persists Storage Area (PSA).

Logs and Version Controls: The process updates logs and version controls to track the execution and status of each connector.

Currently, the execution version of the steps in the web interface is still under development. 

The process is executed through the command line using the following script::
    $ python manage.py etl --collect {all or connector}

If the "all" option is used, the process collects data for all active connectors in the master data table.

If a specific connector is provided, only that connector's data will be collected.



