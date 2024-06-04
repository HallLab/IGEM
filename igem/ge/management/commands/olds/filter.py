""" 
AUTHOR:         Andre Rico
LAST UPDATE:    2022/10/14 


parameters
=========================================================================================================
Example:
$ python manage.py filter --parameters ~/../
----------------------------------------------------------------------------------------------------------
- Generates an example file with parameters for using the Keylink and Wordmap subprocesses.
- In the file we will have three groups of parameters.
  - The Filter group informs the values to be searched in the database.
  - The output group if "no" is entered, the corresponding field will not be displayed in the result, 
    with the grouped values for this field.
  - The group path will be the file that will receive the results values.
OUTPUT CSV Structure
    index,parameter,value
    filter,database,*
    filter,dataset,*
    filter,group,*
    filter,category,*
    filter,keyge,dise:d006509
    filter,word,*
    output,database,*
    output,dataset,*
    output,group,*
    output,category,*
    output,keyge,no
    output,word,no
    path,path,/.../outcome_to_dise:d006509.csv
=========================================================================================================


word_to_keyge
=========================================================================================================
Example:
$ python manage.py filter --word_to_keyge ~/../file_with_words.csv
----------------------------------------------------------------------------------------------------------
- The subprocess has the objective of locating ALL KEYGE starting from a LIST of words. 
- The data INPUT will be a LIST of lines containing words or sets of words (phrase).
- The subprocess will separate the words of each line and will seek a better correlation 
  of KEYGE in the database through the data registered in the KEYWORD table.
- The subprocess will use multiprocessor resources, dynamically allocating to available processors.
- The output will be a file containing the INPUT lines (strings), which word or words 
  were associated with a KEYGE and the KEYGE found.
OUTPUT CSV Structure
    / row / string / word / keyge_id / keyge / keyge_descr / qtd_terms / qtd_loops / time
    - row: row number on source file. Help to map the string before search process
    - string: source string/word used to base the search process
    - word: One or more word of string used to search the keyge
    - keyge_id: DB id on KEYGE table
    - keyge: KEYGE code
    - keyge_descr: Description of KEYGE (that can be the same of word, or not if the word was map)
    - qtd_terms: Number of registers select of KEYWORD to process / actual source row (contain terms)
    - qtd_loops: Number ot interation over selected registers to match the word to keyge
    - time: time to process the source string (row on source file)

IMPROVEMENT: Diminir a quantidade de qtde_term reduz o tempo de processamento,
     porem nao sei com afazer isso ainda
=========================================================================================================


keylink
=========================================================================================================
Example:
$ python manage.py filter --keylink ~/.../{file this the parameters}.csv
----------------------------------------------------------------------------------------------------------
- From a parameter file (see how to generate the file in the parameters subprocess). 
- The keylink subprocess will filter previously ingested links in GE.db.
- The KEYLINK table is the primary source of information for the filter.
- In the parameters, we can inform the filters of as many elements as necessary,
- The filter occurs with the clause "or," thus being broad,will obey the filters with greater amplitude.
- All the structure columns will be presented in the output file, even informing the output parameter 
  of a field as "no." In these cases, we will not have the elements open with this field, and the values 
  will be grouped at that level.
OUTPUT CSV Structure
database/dataset/group_1/category_1/keyge_1/description_1/group_2/category_2/keyge_2/description_2/count
=========================================================================================================


gene_snp
=========================================================================================================
Example:
$ python manage.py filter --gene_snp ~/.../{}.csv
----------------------------------------------------------------------------------------------------------

OUTPUT CSV Structure
    gene / snp / exposome / count / qtd sources / (sources ID)
=========================================================================================================


"""


import os
import re
import sys
import warnings
from concurrent.futures import as_completed
from datetime import datetime

import numpy as np
import pandas as pd
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand
from django.db.models import Q, Sum
from django_thread import ThreadPoolExecutor
from ge.models import KeyLink, KeyWord, WordMap

warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression') # uncomment to suppress the UserWarning

def search_keyges(lines):
    v_msm = False # Use on Debug (if true will show messagens and processes data)

    # Structure to keep processed data
    df_mapper = pd.DataFrame(columns=['row','string','word','keyge','keyge_id','keyge_descr','qtd_terms','qtd_loops','time'])
    v_iloc = 0

    for idx, line in lines.iterrows():                  
        v_chk = False               # Check if found keyge / if not will be False and create a row with this information
        v_line = line[0]            # v_line used to fix the String Column on output file
        v_row = line.name           # Keep the source number row to used in Row Column on output file
        tm_start = datetime.now()   # set the start time of the line
        try: 
            # It splits the entire string (line) for a list of values, where each value 
            # will be a parameter for the search for combinations in the KeyWords table.    
            v_str = re.split(r"[\^\ ]", str(line[0]))   
            if v_msm:
                print("--> List value to search: ", v_str)
            
            # All records in the list value are searched for each element of the list.
            # The contains option results from a more significant number of records than 
            # the Match option; when using the Match option, we would leave out the 
            # combinations of words that in the list are in different elements, and in 
            # KeyWord, we will have them together.
            DF_KY_WD_TEMP = DF_KY_WD[DF_KY_WD['word'].str.contains(r'\b(?:\s|^)(?:{})(?:\s|$\b)'.format('|'.join(v_str)))] # CONTAINS
            # DF_KY_WD_TEMP = DF_KY_WD[DF_KY_WD['word'].str.match(r'\b(?:\s|^)(?:{})(?:\s|$\b)'.format('|'.join(v_str)))]  # MARCH
            if v_msm:
                print("--> Number of records found that match the list of searched elements: ", len(DF_KY_WD_TEMP.index)) 
                print("--> Selected Records to Association: ") 
                print(DF_KY_WD_TEMP) 
                print("   ") 
            
            # Sort the list from longest string to shortest. This process makes it possible
            # to associate compound words with simple words later.
            s = DF_KY_WD_TEMP.word.str.len().sort_values(ascending=False).index
            DF_KY_WD_TEMP = DF_KY_WD_TEMP.reindex(s)
            DF_KY_WD_TEMP = DF_KY_WD_TEMP.reset_index(drop=True) 
            #DF_KY_WD_TEMP.to_csv('Key_word_temp.csv') # DEBUG USE: Views of records found on KeyWord with list filter
            
            # For each record found in KeyWord based on List as a filter, a loop will be 
            # performed to find the best match with the List.
            #        
            # We have some issues and improvements in this process, such as:
            # - very complex strings with special characters are not working correctly.
            # - We have to run the iteration for all the records found, even though we 
            #     have already finished the possible combinations.
            # - It is not considering the whole word; it can only replace the part found.
            line_pull = []
            v_seq = 0        
            for index, row in DF_KY_WD_TEMP.iterrows():         
                # In this first IF, find part or all of the word.
                if line[0].find(row['word']) != -1:
                    if v_msm:
                        print("  --> First loop, interaction number: ", v_seq, " found: ", row['word']) 

                    # In this second IF, it checks if the term is complete, avoiding associating 
                    # only part of a word from the list with a KeyWord record
                    # if 1==1:  # Disable this check
                    if re.search(r'\b' + row['word'] + r'\b', line[0]):
                        if v_msm:
                            print("  --> Second loop found: ", row['word'])                             
                            print("  --> String before association: ", line[0])

                        v_key = str(row['keyge_id__keyge'])
                        line[0] = line[0].replace(row['word'], '') 
                        line_pull.append(v_key)
                        if v_msm:
                            print("    --> Associated KEYGE: ", v_key) 
                            print("    --> Word or Words Associated: ", row['word'])
                            print("    --> String after association: ", line[0])
                            print("    --> End of interation: ", v_seq)
                            print(" ")
                        
                        tm_end =   datetime.now() - tm_start
                        df_mapper.loc[v_iloc] = [v_row,v_line,row['word'],v_key,row['keyge_id'],row['keyge_id__description'],len(DF_KY_WD_TEMP.index),v_seq,tm_end]
                        v_chk = True # Set True to NOT create a empty output row
                        v_iloc += 1
                v_seq +=1
                        
            # Did not find any KEYGE in the process, add a EMPTY line in df_mapper.
            if v_chk == False:
                tm_end =   datetime.now() - tm_start             
                df_mapper.loc[v_iloc] = [v_row,v_line,'','','','',len(DF_KY_WD_TEMP.index),v_seq,tm_end]
                v_iloc += 1
            if v_msm:
                print("--> Remaininf String: ", line[0])
            # line_pull = " ".join(str(x) for x in set(line_pull)) # Process A
            # line[0] = line_pull # Process A
        except:
            if v_msm:
                print("Unable to process registration", idx, line)
                line[0] = 'ERROR ON PROCESS '
            # ERROR: Output line will be created with the error message
            tm_end =   datetime.now() - tm_start
            df_mapper.loc[v_iloc] = [v_row,v_line,'error','error','error','error','error','error',tm_end]
            v_iloc += 1 
    #lines_return = pd.DataFrame(lines) # Process A
    if v_msm:
        print(df_mapper)
    # return lines_return, df_mapper # process A
    return df_mapper



class Command(BaseCommand):
    help = 'Get data from Igem Database'

    def add_arguments(self, parser):

        parser.add_argument(
            '--keylink',
            type=str,
            metavar='file path',
            action='store',
            default=None,
            help='group value',
        )

        parser.add_argument(
            '--wordmap',
            type=str,
            metavar='file path',
            action='store',
            default=None,
            help='group value',
        ) 

        parser.add_argument(
            '--parameters',
            type=str,
            metavar='path',
            action='store',
            default=None,
            help='group value',
        ) 

        parser.add_argument(
            '--word_to_keyge',
            type=str,
            metavar='file path',
            action='store',
            default=None,
            help='Run search for words to KEYGE',
        )

        
    def handle(self, *args, **options):

# KEYLINK  
        if options['keylink']:
            v_path_in = str(options['keylink']).lower()
            v_path_out = os.path.dirname(v_path_in) + "/output_keylink.csv"
            
            if v_path_in == None:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path to load'))
                sys.exit(2)
            if not os.path.isfile(v_path_in) :
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  File not found'))
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path and the file in CSV format to load'))
                sys.exit(2)
            
            try:
                DFP = pd.read_csv(v_path_in)
                DFP = DFP.apply(lambda x: x.astype(str).str.lower()) 
            except IOError as e:
                self.stdout.write(self.style.ERROR('ERRO:')) 
                print(e)
                sys.exit(2)

            v_database  = []
            v_dataset   = []
            v_group     = []
            v_category  = []
            v_keyge     = []

            v_ck_database   = True
            v_ck_dataset    = True
            v_ck_group      = True
            v_ck_category   = True
            v_ck_keyge      = True
           
            for index, row in DFP.iterrows():
                if row['index'] == 'filter':
                    if row['parameter'] == 'database':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_database.append(row['value'])                    
                    if row['parameter'] == 'dataset':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_dataset.append(row['value'])
                    if row['parameter'] == 'group':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_group.append(row['value'])
                    if row['parameter'] == 'category':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_category.append(row['value'])
                    if row['parameter'] == 'keyge':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_keyge.append(row['value'])    

                if row['index'] == 'output':
                    if row['parameter'] == 'database':
                        if row['value'] == 'no':
                            v_ck_database   = False
                    if row['parameter'] == 'dataset':
                        if row['value'] == 'no':
                            v_ck_dataset   = False
                    if row['parameter'] == 'group':
                        if row['value'] == 'no':
                            v_ck_group   = False
                    if row['parameter'] == 'category':
                        if row['value'] == 'no':
                            v_ck_category   = False
                    if row['parameter'] == 'keyge':
                        if row['value'] == 'no':
                            v_ck_keyge   = False

                if row['index'] == 'path':
                    if row['value']:
                        v_path_out_tmp = row['value']
                        if not os.path.isdir(os.path.dirname(v_path_out_tmp)) :
                            self.stdout.write(self.style.HTTP_BAD_REQUEST('  Output path not found'))
                            self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path to results download'))
                            sys.exit(2)
                        v_path_out = v_path_out_tmp      
   
            v_filter = {}
            if  v_database:
                v_filter['dataset__database__database__in'] = v_database
            if v_dataset:
                v_filter['dataset__dataset__in'] = v_dataset 
            if v_group:
                v_filter['keyge1__group_id__group__in'] = v_group
                v_filter['keyge2__group_id__group__in'] = v_group
            if v_category:
                v_filter['keyge1__category_id__category__in'] = v_category
                v_filter['keyge2__category_id__category__in'] = v_category
            if v_keyge:
                v_filter['keyge1__keyge__in'] = v_keyge # if we have performance issues, switch to keyge_id and convert the input
                v_filter['keyge2__keyge__in'] = v_keyge # if we have performance issues, switch to keyge_id and convert the input
            
            v_aggr = []
            if v_ck_database:
                v_aggr.append('dataset__database__database')
            if v_ck_dataset:
                v_aggr.append('dataset__dataset')
            if v_ck_group:
                v_aggr.append('keyge1__group_id__group')
                v_aggr.append('keyge2__group_id__group')
            if v_ck_category:
                v_aggr.append('keyge1__category_id__category')
                v_aggr.append('keyge2__category_id__category')
            if v_ck_keyge:
                v_aggr.append('keyge1__keyge')
                v_aggr.append('keyge2__keyge')
                v_aggr.append('keyge1__description')
                v_aggr.append('keyge2__description')

            if  v_database:             
                v_database.append('dummy')
                query_database = (Q(dataset__database__database__in=(v_database)))
            else:
                query_database = (Q(dataset_id__gt=(0)))
            if v_dataset:
                v_dataset.append('dummy')
                query_dataset = (Q(dataset__dataset__in=(v_dataset)))
            else:
                query_dataset = (Q(dataset_id__gt=(0)))
            if v_group:
                v_group.append('dummy')
                query_group = (Q(keyge1__group_id__group__in=(v_group)))
                query_group.add(Q(keyge2__group_id__group__in=(v_group)), Q.OR)
            else:
                query_group = (Q(dataset_id__gt=(0)))
            if v_category:
                v_category.append('dummy')
                query_category = (Q(keyge1__category_id__category__in=(v_category)))
                query_category.add(Q(keyge2__category_id__category__in=(v_category)), Q.OR)
            else:
                query_category = (Q(dataset_id__gt=(0)))      
            if v_keyge:
                v_keyge.append('dummy')
                query_keyge = (Q(keyge1__keyge__in=(v_keyge)))
                query_keyge.add(Q(keyge2__keyge__in=(v_keyge)), Q.OR)
            else:
                query_keyge = (Q(dataset_id__gt=(0)))  


            try:
                # DFR = pd.DataFrame(KeyLink.objects.filter(**v_filter). \
                DFR = pd.DataFrame(KeyLink.objects.filter(query_database, query_dataset, query_group, query_category, query_keyge). \
                    values(*v_aggr).annotate(count=Sum("count")))

            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  GE.db query error'))
                sys.exit(2)
            if DFR.empty:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data found with the given parameters'))
                sys.exit(2)

            DFR.rename(columns={'dataset__database__database':'database', \
                                'dataset__dataset':'dataset', \
                                'keyge1__group_id__group':'group_1', 'keyge2__group_id__group':'group_2', \
                                'keyge1__category_id__category':'category_1', 'keyge2__category_id__category':'category_2',     
                                'keyge1__keyge':'keyge_1','keyge2__keyge':'keyge_2','keyge1__description':'description_1',
                                'keyge2__description':'description_2'}, inplace=True)

            DFR = DFR.reindex(columns=['database','dataset','group_1','category_1','keyge_1','description_1', \
                                       'group_2','category_2','keyge_2','description_2','count'])
   
            DFR.to_csv(v_path_out, index=False)
            self.stdout.write(self.style.SUCCESS('File with the results sucessfully created in %s' % str(v_path_out)))

# WORDMAP
        if options['wordmap']:
            v_path_in = str(options['wordmap']).lower()

            if v_path_in == None:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path to load'))
                sys.exit(2)
            if not os.path.isfile(v_path_in) :
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  File not found'))
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path and the file in CSV format to load'))
                sys.exit(2)
            
            try:
                DFP = pd.read_csv(v_path_in)
                DFP = DFP.apply(lambda x: x.astype(str).str.lower()) 
            except IOError as e:
                self.stdout.write(self.style.ERROR('ERRO:')) 
                print(e)
                sys.exit(2)

            v_database  = []
            v_dataset   = []
            v_group     = []
            v_category  = []
            v_keyge     = []
            v_word      = []

            v_ck_database   = True
            v_ck_dataset    = True
            v_ck_group      = True
            v_ck_category   = True
            v_ck_keyge      = True
            v_ck_word       = True

            v_path_out = os.path.dirname(v_path_in) + "/output_wordmap.csv"

            for index, row in DFP.iterrows():
                if row['index'] == 'filter':
                    if row['parameter'] == 'database':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_database.append(row['value'])                    
                    if row['parameter'] == 'dataset':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_dataset.append(row['value'])
                    if row['parameter'] == 'group':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_group.append(row['value'])
                    if row['parameter'] == 'category':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_category.append(row['value'])
                    if row['parameter'] == 'keyge':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_keyge.append(row['value'])    
                    if row['parameter'] == 'word':
                        if row['value'] == 'nan' or row['value'] == '*':
                            pass
                        else:
                            v_word.append(row['value'])  

                if row['index'] == 'output':
                    if row['parameter'] == 'database':
                        if row['value'] == 'no':
                            v_ck_database   = False
                    if row['parameter'] == 'dataset':
                        if row['value'] == 'no':
                            v_ck_dataset   = False
                    if row['parameter'] == 'group':
                        if row['value'] == 'no':
                            v_ck_group   = False
                    if row['parameter'] == 'category':
                        if row['value'] == 'no':
                            v_ck_category   = False
                    if row['parameter'] == 'keyge':
                        if row['value'] == 'no':
                            v_ck_keyge   = False
                    if row['parameter'] == 'word':
                        if row['value'] == 'no':
                            v_ck_word   = False

                if row['index'] == 'path':
                    if row['value']:
                        v_path_out_tmp = row['value']
                        if not os.path.isdir(os.path.dirname(v_path_out_tmp)) :
                            self.stdout.write(self.style.HTTP_BAD_REQUEST('  Output path not found'))
                            self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path to results download'))
                            sys.exit(2)
                        v_path_out = v_path_out_tmp      

            v_aggr = []
            if v_ck_database:
                v_aggr.append('database__database')
            if v_ck_dataset:
                v_aggr.append('dataset__dataset')
            if v_ck_group:
                v_aggr.append('keyge1__group_id__group')
                v_aggr.append('keyge2__group_id__group')
            if v_ck_category:
                v_aggr.append('keyge1__category_id__category')
                v_aggr.append('keyge2__category_id__category')
            if v_ck_keyge:
                v_aggr.append('keyge1__keyge')
                v_aggr.append('keyge2__keyge')
            if v_ck_word:
                v_aggr.append('word1')
                v_aggr.append('word2')

            if  v_database:             
                v_database.append('dummy')
                query_database = (Q(database__database__in=(v_database)))
            else:
                query_database = (Q(database_id__gt=(0)))
            if v_dataset:
                v_dataset.append('dummy')
                query_dataset = (Q(dataset__dataset__in=(v_dataset)))
            else:
                query_dataset = (Q(dataset_id__gt=(0)))
            if v_group:
                v_group.append('dummy')
                query_group = (Q(keyge1__group_id__group__in=(v_group)))
                query_group.add(Q(keyge2__group_id__group__in=(v_group)), Q.OR)
            else:
                query_group = (Q(dataset_id__gt=(0)))
            if v_category:
                v_category.append('dummy')
                query_category = (Q(keyge1__category_id__category__in=(v_category)))
                query_category.add(Q(keyge2__category_id__category__in=(v_category)), Q.OR)
            else:
                query_category = (Q(dataset_id__gt=(0)))      
            if v_keyge:
                v_keyge.append('dummy')
                query_keyge = (Q(keyge1__keyge__in=(v_keyge)))
                query_keyge.add(Q(keyge2__keyge__in=(v_keyge)), Q.OR)
            else:
                query_keyge = (Q(dataset_id__gt=(0)))  
            if v_word:
                v_word.append('dummy')
                query_word = (Q(word1__in=(v_word)))
                query_word.add(Q(word2__in=(v_word)), Q.OR)
            else:
                query_word = (Q(dataset_id__gt=(0)))  

            try:
                DFR = pd.DataFrame(WordMap.objects.filter(query_database, query_dataset, query_group, query_category, query_keyge, query_word) \
                    .values(*v_aggr).annotate(count=Sum("count")))

            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  GE.db query error'))
                sys.exit(2)
            if DFR.empty:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data found with the given parameters'))
                sys.exit(2)

            DFR.rename(columns={'database__database':'database', \
                                'dataset__dataset':'dataset', \
                                'keyge1__group_id__group':'group_1', 'keyge2__group_id__group':'group_2', \
                                'keyge1__category_id__category':'category_1', 'keyge2__category_id__category':'category_2',     
                                'keyge1__keyge':'keyge_1','keyge2__keyge':'keyge_2',   
                                }, inplace=True)

            DFR = DFR.reindex(columns=['database','dataset','word1','group_1','category_1','keyge_1','word2','group_2','category_2','keyge_2','count'])

            DFR.to_csv(v_path_out, index=False)
    
# PARAMETERS
        if options['parameters']:
            v_path_in = str(options['parameters']).lower()

            if not os.path.isdir(v_path_in):
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Output path not found'))
                sys.exit(2)
            else:
                v_path_out = v_path_in + "/filter_parameters.csv"

            v_index = ['filter','filter','filter','filter','filter','filter','output','output','output','output','output','output','path']
            v_parameter = ['database','dataset','group','category','keyge','word','database','dataset','group','category','keyge','word','path']
            v_value = ['*','*','*','*','*','*','*','*','*','*','*','*','/../file.csv']
            v_list = list(zip(v_index,v_parameter,v_value))
            DFR = pd.DataFrame(v_list, columns=['index','parameter','value'])

            DFR.to_csv(v_path_out, index=False)

            self.stdout.write(self.style.SUCCESS('File template with parameters created in {0}'.format(v_path_out)))
    
# WORD_TO_KEYGE
        if options['word_to_keyge']:
            v_input = str(options['word_to_keyge']).lower()
            v_file_out = os.path.dirname(v_input) + "/output_word_to_keyge.csv"
            
            if v_input == None:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path to load'))
                sys.exit(2)
            if not os.path.isfile(v_input) :
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  File not found'))
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path and the file in CSV format to load'))
                sys.exit(2)
                    
            self.stdout.write(self.style.HTTP_NOT_MODIFIED('Start: Search for WORDS to KEYGE'))

            # Only keywords with status and commute true
            # KeyWord table search the relationships between active words and key
            global DF_KY_WD
            DF_KY_WD = pd.DataFrame(list(KeyWord.objects.values('word','keyge_id','keyge_id__keyge','keyge_id__description').filter(status=True, commute=True).order_by('word')))

            # Check Keyword has data
            if DF_KY_WD.empty:
                self.stdout.write(self.style.HTTP_NOT_FOUND('  No data on the relationship words and keyge'))
                sys.exit(2)

            # Read file with list of WORD to search (each row will consider as a string)
            DF_INPUT = pd.read_csv(v_input, index_col=False)
            v_row = len(DF_INPUT.index)
            self.stdout.write(self.style.HTTP_NOT_FOUND('    Rows to process: %s rows' % v_row))
            self.stdout.write(self.style.HTTP_NOT_FOUND('    Rows on KeyWord: %s rows' % len(DF_KY_WD.index)))

            df_combiner = pd.DataFrame()
            df_reducer = pd.DataFrame()
            
            df_temp = DF_INPUT.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
            list_df = np.array_split(df_temp, os.cpu_count()-1) # Convert to n list depend on number of cores

            # Multiprocess with dinamic distribution
            try:
                with ThreadPoolExecutor() as executor:
                    future = {executor.submit(search_keyges, list_df[i]) for i in range(len(list_df))}

                for future_to in as_completed(future):
                    df_combiner = future_to.result()
                    # data = future_to.result() # with has more than 1 return from MAPPER
                    # df_combiner = pd.DataFrame(data[0]) # with has more than 1 return from MAPPER
                    # df_mapper = pd.DataFrame(data[1]) # with has more than 1 return from MAPPER
                    df_reducer = pd.concat([df_reducer, df_combiner], axis=0)  # trocar pot lista para ganhar performance / exemplo ncbi
            except:
                    self.stdout.write(self.style.ERROR('      Error on search multiprocess'))
                    
            df_reducer.sort_values(by=['row'], ascending=True, inplace=True)                                     
            df_reducer.reset_index(drop=True, inplace=True)

            df_reducer = df_reducer[['row','string','word','keyge_id','keyge','keyge_descr','qtd_terms','qtd_loops','time']]
            df_reducer.to_csv(v_file_out, index=False)
            self.stdout.write(self.style.SUCCESS('File with the results sucessfully created in %s' % str(v_file_out)))