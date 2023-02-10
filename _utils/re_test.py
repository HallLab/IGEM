import pandas as pd
import re

data = {'file': ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15'],
        'observations': ['text one address', 
                                'text 2 some', 
                                'text home 3', 
                                'notified text 4',
                                'text 5 add',
                                'text 6 homer',
                                'text 6 homer teste ----',
                                'mesh',
                                'mesh c537494', 'mesh:c537495', 'mesh:537496', 'mesh:c537497', 'mesh:c537498', 'mesh:c537500',
                                'teste macaco']}

df = pd.DataFrame(data=data)

conditions = ['homer teste','not','address','macaco','mesh','mesh:537496'] # linha de analise


# dse = df[df['observations'].str.contains('(?:\s|^|[,;])mesh(?:\s|$|[,;])')]

dsa = df[df['observations'].str.contains(r'\b(?:\s|^)(?:{})(?:\s|$\b)'.format('|'.join(conditions)))]




print(dsa)
print('')
# print(dse)

# dfs = df[df['observations'].str.contains(fr"\b\s(?:{'|'.join(conditions)})\b")]ß
# dsa = df[df['observations'].str.contains(r'\b(?:\s|^)home(?:\s|$)\b')]
# dsa = df[df['observations'].str.contains(r'(?:\b(?=\w)|(?!\w))|()(?:{})(?:\b(?<=\w)|(?<!\w))'.format('|'.join(conditions)))]
"""  (?=\S*['-])|([a-zA-Z'-]+)'
"""
dfs = df[df['observations'].str.contains(fr"\b(?:{'|'.join(conditions)})\b")]








"""text = ['San', 'Francisco', 'is', 'foggy', '.','Viva', 'Las', 'Las', 'Vegas','.']


replacements = {'san_francisco':['San Francisco'],
                'las_vegas': ['Las Vegas'],
                'teste' : ['Las'],
                'teste-pos' : ['Francisco']
                }

text2= ' '.join(text)
print(text2)

for key, value in replacements.items():
    print(value)
    text2=text2.replace(value[0],key)

final=text2.split(' ')

print(final)"""




import re

s = '98787This is correct'
for words in ['This is correct', 'This is', 'is correct', 'correct']:
    if re.search(r'\b' + words + r'\b', s):
        print('{0} found'.format(words))