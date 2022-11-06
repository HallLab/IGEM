GE.filter
---------



Command Line
We can perform queries in GE.db through the db and filter processes, with each one acting with a different focus:
DB: we can query and download the master data as shown above
Filter
It allows performing queries on Keylink and Wordmap data with the support of a parameter table.
To generate a parameter table, run the command:
$ python manage.py filter –parameters {directory}
The system will generate a file with the parameters to perform queries in Keylink and Wordmap.

To query the wordmap:
$ python manage.py filter –wordmap {parameters_file}
For consultation on Keylink:
$ python manage.py filter –keylink {parameters_file}
