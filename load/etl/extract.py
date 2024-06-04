try:
    import os
    import sys
    from pathlib import Path

    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("erro: ", e)
    raise

from igem.ge import etl

con = False
pre = False
map = False
red = False

con = etl.collect(connector="ctdcgint")
print("-------->collect finish")

if con:
    pre = etl.prepare(connector="ctdcgint")
    print("-------->prepare finish")
else:
    print("error")
    sys.exit(2)

if pre:
    map = etl.map(connector="ctdcgint")
    print("-------->map finish")
else:
    print("error")
    sys.exit(2)

if map:
    red = etl.reduce(connector="ctdcgint")
    print("-------->reduce finish")
else:
    print("error")
    sys.exit(2)
