import os

import igem
from igem.ge import db

"""
Deployment Strategy
1 - Create a Deploy folder
2 - Change DB to SQLite without data
3 - Clean the Migrations folders
4 - Perform the poetry deply
5 - Update the deploy_db folder with new files
"""

path_igem = os.path.dirname(igem.__file__)
print("IGEM package folder:", path_igem)
print(" ")

print("Start Makegrigations")
v_command = str(path_igem) + str("/manage.py makemigrations")
os.system(v_command)
print(" ")

print("Start Migrate")
v_command = str(path_igem) + str("/manage.py migrate")
os.system(v_command)
print(" ")

print("Start New User")
v_command = str(path_igem) + str("/manage.py createsuperuser")
os.system(v_command)
print(" ")

print("Start Data Load")
path_data = str(os.path.dirname(__file__)) + "/data"
v_chk = db.restore(table="datasource", path_out=path_data)
v_chk = db.restore(table="connector", path_out=path_data)
v_chk = db.restore(table="prefixopc", path_out=path_data)
v_chk = db.restore(table="dstcolumn", path_out=path_data)
v_chk = db.restore(table="termgroup", path_out=path_data)
v_chk = db.restore(table="termcategory", path_out=path_data)
v_chk = db.restore(table="term", path_out=path_data)
v_chk = db.restore(table="wordterm", path_out=path_data)
v_chk = db.restore(table="termmap", path_out=path_data)
v_chk = db.restore(table="snpgene", path_out=path_data)

print(" ")

print("Start Server")
v_command = str(path_igem) + str("/manage.py runserver")
os.system(v_command)
print(" ")
