import sys, time, re, math, os
import pandas as pd
from django.conf import settings
from ge.models import Dataset, Keyge, WordMap, WFControl
from django.core.exceptions import ObjectDoesNotExist
from concurrent.futures import as_completed
from django_thread import ThreadPoolExecutor
from itertools import combinations
from django.core.management.base import BaseCommand



""" 
Process in the data flow and aims to run MapReduce process to link to words in same column



Pendencies:

"""


def chunkify(df: pd.DataFrame, chunk_size: int):
    start = 0
    length = df.shape[0]
    # If DF is smaller than the chunk, return the DF
    if length <= chunk_size:
        yield list(df[:])
        return
    # Yield individual chunks
    while start + chunk_size <= length:
        yield (df[start:chunk_size + start])
        start = start + chunk_size
    # Yield the remainder chunk, if needed
    if start < length:
        yield (df[start:])


def mapper(lines):
        df_mapper = pd.DataFrame(columns=["word1", "word2", "count"])
        tmp = []     
        for line in lines.itertuples(name=None, index=False):
            
            line = str(list(line)) # transf iterrows in string list     
            # Data Cleaning
            line = line.replace("'","") # delete ' between words inside string
            RE_DIGIT = re.compile(r"\b(?<![0-9-])(\d+)(?![0-9-])\b")      
            words = WORD_RE.findall(line)
            digits = RE_DIGIT.findall(str(words)) # Delete Numbers
            words.sort()
            words = list(set(words)) 
            words.sort()
            words = list(filter(lambda w: w not in digits, words)) #Delete words with only number
            # Mapping
            for (x, y) in combinations(words, 2):
                if x < y:
                    tmp.append([x, y, 1])
                else:
                    tmp.append([y, x, 1])
        df_mapper = pd.DataFrame(tmp, columns=["word1", "word2", "count"])

        return df_mapper



class Command(BaseCommand):
    help = 'Run MapReducer from files after prepare to integrate on WordMap and Keylink'

    def add_arguments(self, parser):
    
        parser.add_argument(
            '--run',
            type=str,
            metavar='dataset',
            action='store',
            default=None,
            help='Will process active Datasets and with new version',
        )

        parser.add_argument(
            '--reset',
            type=str,
            metavar='dataset',
            action='store',
            default=None,
            help='Will reset dataset version control',
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
            '--schema',
            type=int,
            metavar='shema opt',
            action='store',
            default=1,
            help='schema for data storage',
        )  

    def handle(self, *args, **options):

        if options['run']:
            
            v_time_process = time.time()                   
            v_opt_ds = str(options['run']).lower()
            v_chunk = options['chunk']
            
            if options['schema'] in [1,2]:
                v_schema = options['schema']
            else:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('Only schemes 1 and 2 available'))
                sys.exit(2)
            
            self.stdout.write(self.style.HTTP_NOT_MODIFIED('Start: Process to map words from databases'))

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



            DF_KEY = pd.DataFrame(list(Keyge.objects.values('id','keyge').order_by('keyge')))
            if DF_KEY.empty:
                self.stdout.write(self.style.HTTP_NOT_FOUND('  The KEYGE table has no records.'))
                if v_schema == 2:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  It will not be possible to perform MapReduce without data in the KEYGE table with schema 2. \
                            Register new KEYGE or change to schema 1 in which all the words will save on WORDMAP'))
                    sys.exit(2)


            # config PSA folder (persistent staging area)
            v_path_file = str(settings.BASE_DIR) + "/psa/"

            global WORD_RE
            WORD_RE = re.compile(r"[\w'\:\#]+") # WORD_RE = re.compile(r"\b\d*[^\W\d_][^\W_]*\b")

            v_cores = os.cpu_count()
            self.stdout.write(self.style.HTTP_SUCCESS('  Process run with {0} cores on multiprocess'.format(v_cores)))  


            for qs in qs_queryset:
                self.stdout.write(self.style.HTTP_NOT_MODIFIED ('  Start: Run database {0} on dataset {1}'.format(qs.database, qs.dataset)))
                v_time_ds = time.time()
                v_erro = False

                # Check control proccess
                try:
                    qs_wfc = WFControl.objects.get(dataset_id = qs.id, chk_collect=True, chk_prepare=True, chk_map=False)
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_NOT_FOUND('   Dataset without workflow to process'))
                    continue

                v_dir = v_path_file + str(qs.database) + "/" + qs.dataset
                v_target = v_dir + "/" + qs.dataset + ".csv"
                
                if not os.path.exists(v_target):
                    print("  File to process not available in " + v_target)
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('    File not available to:  "%s"' % qs.dataset))
                    self.stdout.write(self.style.HTTP_SUCCESS('      check  on:  "%s"' % v_target))
                    continue

                
                if v_schema == 2:
                    self.stdout.write(self.style.HTTP_SUCCESS('     Option to eliminate words with no keyge relationship is active (schema = 2)')) 
                
                v_idx = 1
                for fp in pd.read_csv(v_target, chunksize = v_chunk, low_memory=False, skipinitialspace=True):
                    if v_idx == 1:
                        self.stdout.write(self.style.HTTP_SUCCESS('    Start mapper on {0} rows per block'.format(v_chunk)))
                        
                    df_reducer = pd.DataFrame(columns=["word1", "word2", "count"])

                    v_rows = math.ceil( len(fp.index) / v_cores )        

                    
                    try:
                        with ThreadPoolExecutor() as executor:
                            future = {executor.submit(mapper, lines) for lines in chunkify(fp, v_rows)}

                        for future_to in as_completed(future):
                            df_combiner = future_to.result()
                            df_reducer = pd.concat([df_reducer, df_combiner], axis=0)      
                    except:
                        self.stdout.write(self.style.ERROR('    Error on map sub-process. Check for data in the file generated by the Prepare Process.'))
                        v_erro = True
                        continue

                    DFR = df_reducer.groupby(["word1", "word2"], as_index=False)["count"].sum() 
                    DFR["database_id"] = qs.database_id
                    DFR["dataset_id"] = qs.id
                    if DF_KEY.empty:
                        DFR["keyge1_id"] = ""
                        DFR["keyge2_id"] = ""
                    else:
                        DFR["keyge1_id"] = DFR.set_index("word1").index.map(DF_KEY.set_index("keyge")["id"])
                        DFR["keyge2_id"] = DFR.set_index("word2").index.map(DF_KEY.set_index("keyge")["id"])

                    if v_schema == 2:
                        DFR.dropna(axis=0, inplace=True)
                        
                    DFR = DFR.where(pd.notnull(DFR), '')
                    DFR.insert(loc=0, column="index", value=DFR.reset_index().index)
                    
                    if v_idx == 1: # first loop will delete all dataset registers on WORDMAP table
                        WordMap.objects.filter(dataset_id = qs.id).delete() 
                    v_idx += 1

                    model_instances = [WordMap(
                        cword = str(record.dataset_id) + '-' + str(v_idx) + '-' + str(record.index),
                        word1 = record.word1,
                        word2 = record.word2,   
                        count = record.count,
                        dataset_id = record.dataset_id,
                        database_id = record.database_id,
                        keyge1_id = record.keyge1_id,
                        keyge2_id = record.keyge2_id,
                        ) for record in DFR.itertuples()]

                    WordMap.objects.bulk_create(model_instances)
                    
                    v_row = len(DFR.index)
                    self.stdout.write(self.style.HTTP_SUCCESS('      Block {0} with {1} combinations processed'.format(v_idx, v_row)))
                    
                    #if idx > 1:
                        # sys.exit(2)
                        #break
                
                # Update WorkFlow Control Process
                if v_erro:
                    continue

                qs_wfc.chk_map = True
                qs_wfc.save()

                self.stdout.write(self.style.HTTP_REDIRECT('    Dataset loaded in {0} seconds'.format(int(time.time() - v_time_ds))))             

            self.stdout.write(self.style.SUCCESS('End of process in {0} seconds'.format(int(time.time() - v_time_process)))) 


        if options['reset']:
            v_opt_ds = str(options['reset']).lower()
            if  v_opt_ds == 'all':
                qs_wfc = WFControl.objects.all()
                qs_wfc.update(  chk_map = False,
                                chk_reduce = False)                  
                self.stdout.write(self.style.SUCCESS('All datasets are defined for the map step'))
            else:
                try:
                    qs_wfc = WFControl.objects.get(dataset_id__dataset = v_opt_ds)
                    qs_wfc.chk_map = False
                    qs_wfc.chk_reduce = False
                    qs_wfc.save()                  
                    self.stdout.write(self.style.SUCCESS('Dataset {0} is defined for the map step'.format(v_opt_ds)))
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_NOT_FOUND('dataset {0} not fount'.format(v_opt_ds)))