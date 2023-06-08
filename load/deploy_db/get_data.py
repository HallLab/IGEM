try:
    import os
    import sys
    from pathlib import Path

    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("erro: ", e)
    raise

from igem.ge import db

path_data = os.path.dirname(__file__) + "/data"

db.backup(path_out=path_data)
