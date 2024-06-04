import os
import re
import sys
import time
import warnings
from concurrent.futures import as_completed

import numpy as np
import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand
from django_thread import ThreadPoolExecutor
from ge.models import Dataset, DSTColumn, KeyWord, WFControl

warnings.filterwarnings("ignore", category=UserWarning)

""" 
Second process in the data flow and aims to preparing the source data in an improved format before the MapReduce process

Subprocess:
    1. Elimination of header lines
    2. Deleting unnecessary columns
    3. Transforming ID columns with identifiers
    4. Replacement of terms
    5. Optional, delete source file

Pendencies:

"""


def mapper(lines):

    for idx, line in lines.iterrows():                  
        try:       
            v_str = re.split(r"[\^\ \[\]]", str(line[0]))   
             
            DF_KY_WD_TEMP = DF_KY_WD[DF_KY_WD['word'].str.contains(r'\b(?:\s|^)(?:{})(?:\s|$\b)'.format('|'.join(v_str)))]

            s = DF_KY_WD_TEMP.word.str.len().sort_values(ascending=False).index
            DF_KY_WD_TEMP = DF_KY_WD_TEMP.reindex(s)
            DF_KY_WD_TEMP = DF_KY_WD_TEMP.reset_index(drop=True) 

            line_pull = []            
            for index, row in DF_KY_WD_TEMP.iterrows():              
                if line[0].find(row['word']) != -1:
                    
                    v_key = str(row['keyge_id__keyge'])
                    line[0] = line[0].replace(row['word'], '') 
                    line_pull.append(v_key)
            
            
            line_pull = " ".join(str(x) for x in set(line_pull))
            line[0] = line_pull
        except:
            print("Unable to process registration", idx, line)
            line[0] = 'ERROR ON COMMUTE'

    lines_return = pd.DataFrame(lines)

    return lines_return





class Command(BaseCommand):
    help = 'Preparation source data do MapReduce'

    def add_arguments(self, parser):
       
        # Named (optional) arguments
        parser.add_argument(
            '--run',
            type=str,
            metavar='dataset',
            action='store',
            default=None,
            help='Will process active Datasets and with new version',
        )

        parser.add_argument(
            '--chunk',
            type=int,
            metavar='chunk size',
            action='store',
            default=1000000,
            help='Rows will be processed per cycle',
        ) 

        parser.add_argument(
            '--reset',
            type=str,
            metavar='dataset',
            action='store',
            default=None,
            help='Will reset dataset version control',
        )

    def handle(self, *args, **options):
        #config PSA folder (persistent staging area)
        v_path_file = str(settings.BASE_DIR) + "/psa/"

        if options['run']:
            v_time_process = time.time() 
            v_chunk = options['chunk']
            v_opt_ds = str(options['run']).lower()

            self.stdout.write(self.style.HTTP_NOT_MODIFIED('Start: Process to prepare and transformation external databases'))
            

            if  v_opt_ds == 'all': 
                v_where_cs = {'update_ds': True}
            else:
                v_where_cs = {'update_ds': True, 'dataset': v_opt_ds}
            try:
                qs_queryset = Dataset.objects.filter(**v_where_cs)
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Datasets not found or disabled'))
                sys.exit(2)
            if not qs_queryset:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Datasets not found or disabled'))
                sys.exit(2)


            # Only keywords with status and commute true
            # KeyWord table search the relationships between active words and key
            # DF_KY_WD = pd.DataFrame(list(KeyWord.objects.values('word','keyge_id__keyge').filter(status=True, commute=True).order_by('word')))
            global DF_KY_WD
            DF_KY_WD = pd.DataFrame(list(KeyWord.objects.values('word','keyge_id__keyge').filter(status=True, commute=True).order_by('word')))
            # adicionar um check se nao tem informacao nessa tabela
            if DF_KY_WD.empty:
                self.stdout.write(self.style.HTTP_NOT_FOUND('  No data on the relationship words and keyge'))
                # sys.exit(2)

            # Start process dataset 
            for qs in qs_queryset:
                self.stdout.write(self.style.HTTP_NOT_MODIFIED ('  Start: Run database {0} on dataset {1}'.format(qs.database, qs.dataset)))
                v_time_ds = time.time()
                

                # Check Workflow 
                try:
                    qs_wfc = WFControl.objects.get(dataset_id = qs.id, chk_collect=True, chk_prepare=False)
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_NOT_FOUND('    Dataset without workflow to process'))
                    continue

                # Variables                    
                v_dir = v_path_file + str(qs.database) + "/" + qs.dataset
                v_source = v_dir + "/" + qs.target_file_name
                v_target = v_dir  + "/" + qs.dataset + ".csv"
                v_skip = qs.source_file_skiprow
                v_tab = str(qs.source_file_sep)
                header = True

                # Check if file is available 
                if not os.path.exists(v_source):
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('    File not available to:  "%s"' % qs.dataset))
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('       path:  "%s"' % v_source))
                    continue
               
               # Delete exiting file
                if os.path.exists(v_target):
                    os.remove(v_target)

                try:
                    if v_skip >= 1:  
                        v_read_file = {'sep': v_tab, 'skiprows': v_skip, 'engine': 'python', 'chunksize': v_chunk}
                        self.stdout.write(self.style.HTTP_SUCCESS('    Open file with {0} skipped rows and {1} rows per block process'.format(v_skip, v_chunk))) 
                    else:
                        v_read_file = {'sep': v_tab, 'engine': 'python', 'chunksize': v_chunk}
                        self.stdout.write(self.style.HTTP_SUCCESS('    Open file without skips rows and {0} rows per block process'.format(v_chunk))) 

                    v_idx = 1
                    for df_source in pd.read_csv(v_source, **v_read_file):                                                                      
                            
                            v_col = len(df_source.columns)
                            v_row = len(df_source.index)
                            df_target = pd.DataFrame()

                            for n in range(v_col):  # Read transformations columns

                                try:
                                    try:
                                        qs_col = DSTColumn.objects.get(dataset_id=qs.id, column_number=n)
                                        v_col_idx = str(qs_col.column_name)
                                    except ObjectDoesNotExist:
                                        qs_col = None
                                except:
                                    self.stdout.write(self.style.HTTP_BAD_REQUEST('    Error reading DST table to %s. Check dataset settings and ID duplicity.' % qs.dataset))
  


                                if not qs_col:
                                    # Rule 1: Columns not configured on Dataset master data. The process will run and perform the Commute
                                    if v_idx == 1:
                                        self.stdout.write(self.style.HTTP_NOT_FOUND('    No rules defines to column: %s. This column will consider on process.' % n))
                                        self.stdout.write(self.style.HTTP_SUCCESS('      If necessary, correct the column through the Dataset settings')) 
                                    df_target[n] = df_source.iloc[:,n]
                                    df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                    df_target[v_col_idx] = df_target.set_index(v_col_idx).index.map(DF_KY_WD.set_index("word")["keyge_id__keyge"]) # Commute Process
                                else:       
                                    if qs_col.status:  
                                        
                                        if str(qs_col.pre_value) != 'none':             
                                            # Rule 2: columns with defined prefixes / does not perform the commute process
                                            df_target[qs_col.column_name] = df_source.iloc[:,n].apply(lambda y: "{}{}".format(qs_col.pre_value,y))
                                            df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                            continue        
                                        
                                        # Rule 3: Columns configured for the process with prefix None /Does not add prefix / Performs the Commute process in a single word
                                        if qs_col.single_word:
                                            df_target[qs_col.column_name] = df_source.iloc[:,n]
                                            df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                            df_target[v_col_idx] = df_target.set_index(v_col_idx).index.map(DF_KY_WD.set_index("word")["keyge_id__keyge"]) # Commute Process
                                            continue
                                        

                                        # Rule 4: Columns configured for the process with prefix None /Does not add prefix / Performs the Commute process WORD by WORD on sentence
                                        df_temp = pd.DataFrame()
                                        df_combiner = pd.DataFrame()
                                        df_reducer = pd.DataFrame(columns=[qs_col.column_name])
                                        df_temp[qs_col.column_name] = df_source.iloc[:,n]
                                        df_temp = df_temp.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                        list_df = np.array_split(df_temp, os.cpu_count()-1)
                                        

                                        try:
                                            with ThreadPoolExecutor() as executor:
                                                future = {executor.submit(mapper, list_df[i]) for i in range(len(list_df))}

                                            for future_to in as_completed(future):
                                                df_combiner = future_to.result()
                                                df_reducer = pd.concat([df_reducer, df_combiner], axis=0)  
                                        except:
                                                self.stdout.write(self.style.ERROR('      Error on commute word by word on sentence'))
                                        df_reducer = df_reducer.sort_index()                                     
                                        df_target[qs_col.column_name] = df_reducer                                     
                                        continue    

                                # Rule 5: Columns configured and not activated will not be processed      
                                
                            df_target.to_csv(v_target, header=header, mode='a') # Write the file
                            header = False # Prevent creating new header lines
                            self.stdout.write(self.style.HTTP_SUCCESS('      Block {0} with {1} records processed'.format(v_idx, v_row)))

                            v_idx += 1

                    # Update WorkFlow Control Process
                    qs_wfc.chk_prepare = True
                    qs_wfc.save()

                    # Delete source file
                    if not qs.target_file_keep: 
                        os.remove(v_source)
                        self.stdout.write(self.style.HTTP_SUCCESS('    Deleted source file in PSA'))
                    else:
                        self.stdout.write(self.style.HTTP_SUCCESS('    Kept the source file in PSA'))
    
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('    Error when process the : %s' % qs.dataset))
                    continue
               
                self.stdout.write(self.style.HTTP_REDIRECT('    Dataset loaded in {0} seconds'.format(int(time.time() - v_time_ds))))             

            self.stdout.write(self.style.SUCCESS('End of process in {0} seconds'.format(int(time.time() - v_time_process)))) 


        if options['reset']:
            v_opt_ds = str(options['reset']).lower()
            if  v_opt_ds == 'all':
                qs_wfc = WFControl.objects.all()
                qs_wfc.update(  chk_prepare = False,
                                chk_map = False,
                                chk_reduce = False)                  
                self.stdout.write(self.style.SUCCESS('All datasets are defined for the prepare step'))
            else:
                try:
                    qs_wfc = WFControl.objects.get(dataset_id__dataset = v_opt_ds)
                    qs_wfc.chk_prepare = False
                    qs_wfc.chk_map = False
                    qs_wfc.chk_reduce = False
                    qs_wfc.save()                  
                    self.stdout.write(self.style.SUCCESS('Dataset {0} is defined for the prepare step'.format(v_opt_ds)))
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_NOT_FOUND('dataset {0} not fount'.format(v_opt_ds)))