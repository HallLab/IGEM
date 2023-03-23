GE.filter
---------


It allows performing queries on Keylink and Wordmap data with the support of a parameter table.

To generate a parameter table, run the command::
    $ python manage.py filter –-parameters {directory}

The system will generate a file with the parameters to perform queries in Keylink and Wordmap.

To query the wordmap::
    $ python manage.py filter --wordmap {parameters_file}
For consultation on Keylink::
    $ python manage.py filter --keylink {parameters_file}
