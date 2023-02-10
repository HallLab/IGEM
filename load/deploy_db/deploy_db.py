import os
import sys

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

# TODO: change process to reads IDs / but need change load_data function

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

v_chk = db.load_data(
    table="datasource",
    path=(path_data + "/datasource.csv"),
)
if not v_chk:
    print("erro on datasource load")
    sys.exit(2)

v_chk = db.load_data(
    table="connector",
    path=(path_data + "/connector.csv"),
)
if not v_chk:
    print("erro on connector load")
    sys.exit(2)

v_chk = db.load_data(
    table="prefix",
    path=(str(path_data) + "/prefix.csv"),
)
if not v_chk:
    print("erro on prefix load")
    sys.exit(2)

v_chk = db.load_data(
    table="ds_column",
    path=(str(path_data) + "/ds_column.csv"),
)
if not v_chk:
    print("erro on ds_column load")
    sys.exit(2)

v_chk = db.load_data(
    table="term_group",
    path=(str(path_data) + "/term_group.csv"),
)
if not v_chk:
    print("erro on term_group load")
    sys.exit(2)

v_chk = db.load_data(
    table="term_category",
    path=(str(path_data) + "/term_category.csv"),
)
if not v_chk:
    print("erro on term_category load")
    sys.exit(2)

v_chk = db.load_data(
    table="term",
    path=(str(path_data) + "/term.csv"),
)
if not v_chk:
    print("erro on term load")
    sys.exit(2)

v_chk = db.load_data(
    table="wordterm",
    path=(str(path_data) + "/wordterm.csv"),
)
if not v_chk:
    print("erro on wordterm load")
    sys.exit(2)

print(" ")


print("Start Server")
v_command = str(path_igem) + str("/manage.py runserver")
os.system(v_command)
print(" ")
