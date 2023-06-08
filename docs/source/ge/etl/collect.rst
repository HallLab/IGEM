Collect
=======

Process responsible for selecting the active connectorS and for each one to check if a new version of the data is available. If so, it will extract the latest data and, if necessary, unzip the extracted file, leaving the file in a PSA structure (Persists Storage Area).


The process also performs the update of logs and version controls. The execution version of the steps in the web interface is still under development. 

Execution of the process occurs in the command line, with script support to run all phases. Always run the commands inside the src folder of the program::
    
$ python manage.py etl --collect {all or connector}

It starts collecting the new connectors, downloading the latest data files from the internet or another location, and making them available in the PSA folder for the subsequent phases. If the call option is informed, the process will execute for all active connectors in the master data table or inform the specific connector for isolated execution::
    


The reset option will eliminate all phase control, including managing the last processed version, allowing reprocessing of the connector again. If you inform all choices, all active connectors will reset or notify only one connector for reset::
    


The show option will show all the registration connector and their status::
    


The activate option will activate a specific connector, allowing data extraction::
    


The deactivate option will deactivate a specific Connector, preventing it from being part of future downloaded files.
