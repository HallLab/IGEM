import os

import igem

path_igem = os.path.dirname(igem.__file__)
print("IGEM package folder:", path_igem)

v_command = str(path_igem) + str("/manage.py runserver")
v_kill = str("kill -9 $(lsof -ti:8000)")
os.system(v_kill)
os.system(v_command)
