# import pandas as pd
# DF = pd.read_xml('data.xml')
# DF.to_csv('teste.csv')

from os.path import splitext
def splitext_(path):
    if len(path.split('.')) > 2:
        return path.split('.')[0],'.'.join(path.split('.')[-2:])
    return splitext(path)

file_name,ext = splitext('data.xml')

print(ext)