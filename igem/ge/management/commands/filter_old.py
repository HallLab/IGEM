import sys
import os
import pandas as pd
from django.db.models import Sum
from ge.models import KeyLink, WordMap
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q 


""" 

"""

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

    def handle(self, *args, **options):
            
        if options['keylink']:
            v_path_in = str(options['keylink']).lower()

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

            v_path_out = os.path.dirname(v_path_in) + "/output_keylink.csv"

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
                                'keyge1__keyge':'keyge_1','keyge2__keyge':'keyge_2'    
                                }, inplace=True)

            DFR = DFR.reindex(columns=['database','dataset','group_1','category_1','keyge_1','group_2','category_2','keyge_2','count'])
   
            DFR.to_csv(v_path_out, index=False)




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

        self.stdout.write(self.style.SUCCESS('  File saved on {0}'.format(v_path_out)))
            