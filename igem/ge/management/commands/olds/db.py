import os
from queue import Empty
import sys
import pandas as pd
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from ge.models import Category, DSTColumn, Group, KeyHierarchy, KeyLink, Keyge, Dataset, KeyWord, Database, LogsCollector, PrefixOpc, WFControl, WordMap

# Uptades aren't works:
# from django_bulk_update import bulk_update   https://pypi.org/project/django-bulk-update/

""" 
Process to maintain the content of the Igem Database

Pendencies:
- Create subprocess to update
- Create subprocess to delete with args
- Uptade the docs


--subprocess:
    1. show
    2. truncate
    3. download
    4. load

{tables}
    1. database
    2. dataset
    3. ds_columns
    4. workflow
    5. keyge
    6. key_category
    7. key_group
    8. key_prefix
    9. keyword
    10. link_key
    11. link_word

--field
    1. database
    2. dataset
    3. keyge
    4. category
    5. group
    6. word

{field_value}
    Open values

--path
    file path and name to read and write from ge.db

Syntaxes:
python manage.py db {--subprocess} {table} {--field} {field_value} {--path} {file_path}

    python manage.py db --show workflow --dataset all
    python manage.py db --truncate dataset --dataset all
    python manage.py db --download keyge --keyge all --path xxxx
    python manage.py db --load dataset --path xxxxx

Pendencies:
-- Update option (on load)
"""




class Command(BaseCommand):
    help = 'Process to maintain the content of the Igem Database'

    def add_arguments(self, parser):

        # subpreocess
        parser.add_argument(
            '--show',
            type=str,
            metavar='table',
            action='store',
            default=None,
            help='show data on tables',
        )
        parser.add_argument(
            '--truncate',
            type=str,
            metavar='table',
            action='store',
            default=None,
            help='Delete data on tables',
        )
        parser.add_argument(
            '--delete',
            type=str,
            metavar='table',
            action='store',
            default=None,
            help='Delete data on tables',
        )
        parser.add_argument(
            '--download',
            type=str,
            metavar='table',
            action='store',
            default=None,
            help='read data on tables and create a file output',
        )
        parser.add_argument(
            '--load',
            type=str,
            metavar='table',
            action='store',
            default=None,
            help='write data on tables from a file',
        )

        # Fields
        parser.add_argument(
            '--database',
            type=str,
            metavar='database',
            action='store',
            default='all',
            help='database value',
        )
        parser.add_argument(
            '--dataset',
            type=str,
            metavar='dataset',
            action='store',
            default='all',
            help='dataset value',
        )
        parser.add_argument(
            '--keyge',
            type=str,
            metavar='keyge',
            action='store',
            default='all',
            help='keyge value',
        )
        parser.add_argument(
            '--category',
            type=str,
            metavar='category',
            action='store',
            default='all',
            help='category value',
        )
        parser.add_argument(
            '--group',
            type=str,
            metavar='group',
            action='store',
            default='all',
            help='group value',
        )
        parser.add_argument(
            '--word',
            type=str,
            metavar='word',
            action='store',
            default='all',
            help='group value',
        )
        parser.add_argument(
            '--prefix',
            type=str,
            metavar='word',
            action='store',
            default='all',
            help='prefix value',
        )

        # File Path
        parser.add_argument(
            '--path',
            type=str,
            metavar='file path',
            action='store',
            default=None,
            help='group value',
        )


    def handle(self, *args, **options):


        def get_model_field_names(model, ignore_fields=['content_object']):
                    model_fields = model._meta.get_fields()
                    model_field_names = list(set([f.name for f in model_fields if f.name not in ignore_fields]))
                    return model_field_names

        def get_lookup_fields(model, fields=None):
            model_field_names = get_model_field_names(model)
            if fields is not None:
                lookup_fields = []
                for x in fields:
                    if "__" in x:
                        # the __ is for ForeignKey lookups
                        lookup_fields.append(x)
                    elif x in model_field_names:
                        lookup_fields.append(x)
            else:
                lookup_fields = model_field_names
            return lookup_fields

        def qs_to_dataset(qs, fields=None):       
            lookup_fields = get_lookup_fields(qs.model, fields=fields)
            return list(qs.values(*lookup_fields))

        def convert_to_dataframe(qs, fields=None, index=None):
            lookup_fields = get_lookup_fields(qs.model, fields=fields)
            index_col = None
            if index in lookup_fields:
                index_col = index
            elif "id" in lookup_fields:
                index_col = 'id'
            values = qs_to_dataset(qs, fields=fields)
            df = pd.DataFrame.from_records(values, columns=lookup_fields, index=index_col)
            return df

        def get_database(*args):
            if v_database != 'all': 
                v_where_cs = {'database': v_database}
            else:
                v_where_cs = {}
            try:
                qs_database = Database.objects.filter(**v_where_cs).order_by('database')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Database not found'))
                sys.exit(2)
            if not qs_database:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Database table'))
                sys.exit(2)
            return qs_database

        def get_dataset(*args):
            if v_database != 'all':
                try:
                    QS_DB = Database.objects.filter(database = v_database)
                    for qs in QS_DB:
                        v_db_id = qs.id
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Database not found'))
                    sys.exit(2)
                if not QS_DB:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Dataset table'))
                    sys.exit(2)
            if v_database != 'all' and v_dataset != 'all': 
                v_where_cs = {'database': v_db_id, 'dataset': v_dataset}
            elif v_database == 'all' and v_dataset != 'all': 
                v_where_cs = {'dataset': v_dataset}
            elif v_database != 'all' and v_dataset == 'all': 
                v_where_cs = {'database': v_db_id}                
            else:
                v_where_cs = {}
            try:
                QS = Dataset.objects.filter(**v_where_cs).order_by('database','dataset')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Dataset table'))
                sys.exit(2)
            return QS

        def get_ds_column(*args):
            if v_dataset != 'all':
                try:
                    QS_DB = Dataset.objects.filter(dataset = v_dataset)
                    for qs in QS_DB:
                        v_db_id = qs.id
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                    sys.exit(2)
                if not QS_DB:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Dataset table'))
                    sys.exit(2)
            if v_dataset != 'all': 
                v_where_cs = {'dataset': v_db_id}
            else:
                v_where_cs = {}
            try:
                QS = DSTColumn.objects.filter(**v_where_cs).order_by('dataset')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in DSTColumn table'))
                sys.exit(2)
            return QS

        def get_workflow(*args):
            if v_dataset != 'all':
                try:
                    QS_DB = Dataset.objects.filter(dataset = v_dataset)
                    for qs in QS_DB:
                        v_db_id = qs.id
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                    sys.exit(2)
                if not QS_DB:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Dataset table'))
                    sys.exit(2)
            if v_dataset != 'all': 
                v_where_cs = {'dataset': v_db_id}
            else:
                v_where_cs = {}
            try:
                QS = WFControl.objects.filter(**v_where_cs).order_by('dataset')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in WorkFlow table'))
                sys.exit(2)
            return QS            

        def get_keyge(*args):
            if v_group != 'all':
                try:
                    QS_DB = Group.objects.filter(group = v_group)
                    for qs in QS_DB:
                        v_id_group = qs.id     
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Group not found'))
                    sys.exit(2)
                if not QS_DB:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Group table'))
                    sys.exit(2)
            if v_category != 'all':
                try:
                    QS_DB = Category.objects.filter(category = v_category)
                    for qs in QS_DB:
                        v_id_cat = qs.id     
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Category not found'))
                    sys.exit(2)
                if not QS_DB:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Category table'))
                    sys.exit(2)
            if v_group != 'all' and v_category != 'all': 
                v_where_cs = {'group': v_id_group, 'category': v_id_cat}
            elif v_group == 'all' and v_category != 'all': 
                v_where_cs = {'category': v_id_cat}
            elif v_group != 'all' and v_category == 'all': 
                v_where_cs = {'group': v_id_group}                
            else:
                v_where_cs = {}
            try:
                QS = Keyge.objects.filter(**v_where_cs).order_by('group','category','keyge')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Keyge not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Keyge table'))
                sys.exit(2)
            return QS

        def get_category(*args):
            try:
                QS = Category.objects.all().order_by('category')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Category not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Category table'))
                sys.exit(2)
            return QS

        def get_group(*args):
            try:
                QS = Group.objects.all().order_by('group')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Group not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Group table'))
                sys.exit(2)
            return QS

        def get_prefix(*args):
            try:
                QS = PrefixOpc.objects.all().order_by('pre_value')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Prefix not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Prefix table'))
                sys.exit(2)
            return QS



        def get_keyword(*args):
            # if v_word != 'all':
            #     v_where_cs = {'word__contains': v_word } # %like%
            # else:
            #     v_where_cs = {}
            
            if v_word != 'all' and v_keyge != 'all': 
                v_where_cs = {'word__contains': v_word, 'keyge_id__keyge': v_keyge}
            
            elif v_word == 'all' and v_keyge != 'all': 
                v_where_cs = {'keyge_id__keyge': v_keyge}
            
            elif v_word != 'all' and v_keyge == 'all': 
                v_where_cs = {'word__contains': v_word }               
            
            else:
                v_where_cs = {}
                     
            
            
            try:
                QS = KeyWord.objects.filter(**v_where_cs).order_by('keyge','word')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Word not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in KeyWord table'))
                sys.exit(2)
            return QS



        def get_link_key(*args):
            if v_word != 'all':
                v_where_cs = {'word__contains': v_word } # %like%
            else:
                v_where_cs = {}
            try:
                QS = KeyLink.objects.values('ckey', 'dataset', 'keyge1', 'keyge2', 'count', 'keyge1__keyge', 'keyge2__keyge').filter(**v_where_cs).order_by('keyge1__keyge','keyge2__keyge')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Word not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in KeyLinks table'))
                sys.exit(2)
            return QS

        def get_wordmap(*args):
            if v_word != 'all':
                v_where_cs = {'word1__contains': v_word } # %like% melhorar esse processo
            else:
                v_where_cs = {}
            try:
                QS = WordMap.objects.filter(**v_where_cs).order_by('word1','word2')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Word not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in WordMap table'))
                sys.exit(2)
            return QS


        # SHOW BLOCK
        if options['show']:
            v_table = str(options['show']).lower()

            if v_table == 'database':
                v_database = str(options['database']).lower()
                QS = get_database(v_database)
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<5}{f"DATABASE":<15}{f"CATEGORY":<15}{f"DESCRIPTION":<50}{f"WEBSITE":<50}'))
                for qs in QS:                   
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<5}{f"{qs.database}":<15}{f"{qs.category}":<15}{f"{qs.description}":<50}{f"{qs.website}":<50}')) 

            elif v_table == 'dataset':
                v_database =  str(options['database']).lower()
                v_dataset = str(options['dataset']).lower()
                QS = get_dataset(v_database, v_dataset)
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<5}{f"DATABASE":<15}{f"DATASET":<15}{f"STATUS":<10}{f"DESCRIPTION":<50}')) 
                for qs in QS:
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<5}{f"{qs.database}":<15}{f"{qs.dataset}":<15}{f"{qs.update_ds}":<10}{f"{qs.description}":<50}')) 
                  
            elif v_table == 'ds_column':
                v_dataset = str(options['dataset']).lower()
                QS = get_ds_column(v_dataset)
                v_ds = ''
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<5}{f"DATASET":<15}{f"COL SEQ":<10}{f"COL NAME":<25}{f"STATUS":<10}{f"PREFIX":<10}'))
                for qs in QS:
                    if v_ds != str(qs.dataset):
                        print('')
                    if str(qs.pre_value) == 'none':
                        v_pre = ''
                    else:
                        v_pre = qs.pre_value
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<5}{f"{qs.dataset}":<15}{f"{qs.column_number}":<10}{f"{qs.column_name}":<25}{f"{qs.status}":<10}{f"{v_pre}":<10}')) 
                    v_ds = str(qs.dataset)
            
            elif v_table == 'workflow':
                v_dataset = str(options['dataset']).lower()
                QS = get_workflow(v_dataset)
                self.stdout.write(self.style.HTTP_INFO(f'{f"DATASET":<15}{f"DT UPDATE":<25}{f"VERSION":<40}{f"SIZE":<15}{f"COLLECT":<10}{f"PREPARE":<10}{f"MAP":<10}{f"REDUCE":<10}'))
                for qs in QS:
                    if str(qs.last_update) != '':
                        v_upd = str(qs.last_update)[:19]
                    v_col = ''
                    v_pre = ''
                    v_red = ''
                    v_map = ''
                    if qs.chk_collect:
                        v_col = 'pass'
                    if qs.chk_prepare:
                        v_pre = 'pass'
                    if qs.chk_map:
                        v_map = 'pass'
                    if qs.chk_reduce:
                        v_red = 'pass'
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.dataset}":<15}{f"{v_upd}":<25}{f"{qs.source_file_version}":<40}{f"{qs.source_file_size}":<15}{f"{v_col}":<10}{f"{v_pre}":<10}{f"{v_map}":<10}{f"{v_red}":<10}       ')) 
  
            elif v_table == 'keyge':
                v_group = str(options['group']).lower()
                v_category = str(options['category']).lower()
                QS = get_keyge(v_group, v_category)
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<15}{f"GROUP":<15}{f"CATEGORY":<15}{f"KEYGE":<20}{f"DESCRIPTION":<50}')) 
                for qs in QS:         
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<15}{f"{qs.group}":<15}{f"{qs.category}":<15}{f"{qs.keyge}":<20}{f"{qs.description}":<50}')) 

            elif v_table == 'category':
                QS = get_category()
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<5}{f"CATEGORY":<15}{f"DESCRIPTION":<50}')) 
                for qs in QS:
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<5}{f"{qs.category}":<15}{f"{qs.description}":<50}'))      

            elif v_table == 'group':
                QS = get_group()
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<5}{f"GROUP":<15}{f"DESCRIPTION":<50}')) 
                for qs in QS:
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<5}{f"{qs.group}":<15}{f"{qs.description}":<50}'))    
                 
            elif v_table == 'prefix':
                QS = get_prefix()
                self.stdout.write(self.style.HTTP_INFO(f'{f"pre_value":<15}')) 
                for qs in QS:                      
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.pre_value}":<15}')) 

            elif v_table == 'keyword':
                v_word = str(options['word']).lower()
                v_keyge = str(options['keyge']).lower()
                QS = get_keyword(v_word, v_keyge)          
                self.stdout.write(self.style.HTTP_INFO(f'{f"STATUS":<10}{f"COMMUTE":<10}{f"KEYGE":<40}{f"WORD":<50}')) 
                for qs in QS:                   
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.status}":<10}{f"{qs.commute}":<10}{f"{qs.keyge}":<40}{f"{qs.word}":<50}')) 
                              
            elif v_table == 'link_key':
                self.stdout.write(self.style.HTTP_NOT_FOUND('function not implemented'))
            
            elif v_table == 'wordmap':
                self.stdout.write(self.style.HTTP_NOT_FOUND('function not implemented'))
                
            else:
                self.stdout.write(self.style.HTTP_NOT_FOUND('Table not recognized in the system. Choose one of the options: '))
                self.stdout.write(self.style.HTTP_NOT_FOUND('   database | dataset | ds_column | workflow | keyge | category | group | prefix | key_word | link_key | wordmap'))


        # DOWNLOAD BLOCK
        if options['download']:

            v_path = str(options['path']).lower()
            v_table = str(options['download']).lower()

            if v_path == None:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path to download'))
                sys.exit(2)
            if not os.path.isdir(v_path) :
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Path not found'))
                sys.exit(2)
            v_file = v_path + "/" + v_table + ".csv"

            if v_table == 'database':
                v_database = str(options['database']).lower()
                QS = get_database(v_database)
                DF = convert_to_dataframe(QS, fields=['database','description','website','category'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))

            elif v_table == 'dataset':
                v_database =  str(options['database']).lower()
                v_dataset = str(options['dataset']).lower()
                QS = get_dataset(v_database, v_dataset)             
                DF = convert_to_dataframe(QS, fields=['database','dataset','update_ds','source_path','source_web','source_compact','source_file_name','source_file_format','source_file_sep','source_file_skiprow','target_file_name','target_file_format','description'], index=False)
                # Data transformations rules
                 # Rule 1: Transform Database ID to Database Name
                try:
                    DF_DB = pd.DataFrame(list(Database.objects.values('id', 'database').order_by('id')))
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Database not found'))
                    sys.exit(2)
                if DF_DB.empty:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Dataset table'))
                    sys.exit(2)
                DF["database"] = DF.set_index("database").index.map(DF_DB.set_index("id")["database"])
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))  

            elif v_table == 'ds_column':
                v_dataset = str(options['dataset']).lower()
                QS = get_ds_column(v_dataset)
                DF = convert_to_dataframe(QS, fields=['dataset','status','column_number','column_name','pre_value','single_word'], index=False)
                # Data transformations rules
                 # Rule 1: Transform Dataset ID to Dataset Name
                try:
                    DF_DB = pd.DataFrame(list(Dataset.objects.values('id', 'dataset').order_by('id')))
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                    sys.exit(2)
                if DF_DB.empty:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Dataset table'))
                    sys.exit(2)
                DF["dataset"] = DF.set_index("dataset").index.map(DF_DB.set_index("id")["dataset"])
                
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))

            elif v_table == 'workflow':
                v_dataset = str(options['dataset']).lower()
                QS = get_workflow(v_dataset)
                DF = convert_to_dataframe(QS, fields=['dataset','last_update','source_file_version','source_file_size','target_file_size','chk_collect','chk_prepare','chk_map','chk_reduce'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))

            elif v_table == 'keyge':
                v_group = str(options['group']).lower()
                v_category = str(options['category']).lower()
                QS = get_keyge(v_group, v_category)
                DF = convert_to_dataframe(QS, fields=['keyge','group','category','description'], index=False)
                # Data transformations rules
                 # Rule 1: Transform Group ID to Group Name
                try:
                    DF_DB = pd.DataFrame(list(Group.objects.values('id', 'group').order_by('id')))
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Group not found'))
                    sys.exit(2)
                if DF_DB.empty:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Group table'))
                    sys.exit(2)
                DF["group"] = DF.set_index("group").index.map(DF_DB.set_index("id")["group"])
                # Rule 2: Transform Category ID to Category Name
                try:
                    DF_DB = pd.DataFrame(list(Category.objects.values('id', 'category').order_by('id')))
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Category not found'))
                    sys.exit(2)
                if DF_DB.empty:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Category table'))
                    sys.exit(2)
                DF["category"] = DF.set_index("category").index.map(DF_DB.set_index("id")["category"])
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))

            elif v_table == 'category':
                QS = get_category()
                DF = convert_to_dataframe(QS, fields=['category','description'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))

            elif v_table == 'group':
                QS = get_group()
                DF = convert_to_dataframe(QS, fields=['group','description'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))

            elif v_table == 'prefix':
                QS = get_prefix()
                DF = convert_to_dataframe(QS, fields=['pre_value'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))

            elif v_table == 'keyword':
                v_word = str(options['word']).lower()
                QS = get_keyword(v_word)
                DF = convert_to_dataframe(QS, fields=['status','commute','word','keyge'], index=False)
                # Data transformations rules
                 # Rule 1: Transform keyge ID to keyge Name
                try:
                    DF_DB = pd.DataFrame(list(Keyge.objects.values('id', 'keyge').order_by('id')))
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Keyge not found'))
                    sys.exit(2)
                if DF_DB.empty:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Keyge table'))
                    sys.exit(2)
                DF["keyge"] = DF.set_index("keyge").index.map(DF_DB.set_index("id")["keyge"])
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))

            elif v_table == 'link_key':
                v_word = str(options['word']).lower()
                QS = get_link_key(v_word)
                DF = convert_to_dataframe(QS, fields=['ckey', 'dataset', 'keyge1', 'keyge1__keyge', 'keyge2', 'keyge2__keyge', 'count', ], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))
            
            elif v_table == 'wordmap':
                v_word = str(options['word']).lower()
                QS = get_wordmap(v_word)
                DF = convert_to_dataframe(QS, fields=['cword','database','dataset','keyge1','keyge2','word1','word2','count'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('  File generated successfully'))
            
            else:
                self.stdout.write(self.style.HTTP_NOT_FOUND('Table not recognized in the system. Choose one of the options: '))
                self.stdout.write(self.style.HTTP_NOT_FOUND('   database | dataset | ds_column | workflow | keyge | category | group | prefix | key_word | link_key | wordmap'))


        # LOAD BLOCK
        if options['load']:
            v_table = str(options['load']).lower()
            v_path = str(options['path']).lower()

            if v_path == None:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path to load'))
                sys.exit(2)
            if not os.path.isfile(v_path) :
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  File not found'))
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Inform the path and the file in CSV format to load'))
                sys.exit(2)

            if v_table == 'database':
                try:
                    DFR = pd.read_csv(v_path)
                    DFR = DFR.apply(lambda x: x.astype(str).str.lower()) 
                except IOError as e:
                    self.stdout.write(self.style.ERROR('ERRO:')) 
                    print(e)
                    sys.exit(2)
                model_instances = [Database(
                    database = record.database,
                    description = record.description,
                    category = record.category,
                    website = record.website,
                    ) for record in DFR.itertuples()]
                Database.objects.bulk_create(model_instances, ignore_conflicts=True)        
                self.stdout.write(self.style.SUCCESS('  Load with success to Database'))

            elif v_table == 'dataset':                  
                try:
                    DFR = pd.read_csv(v_path)
                    DFR['database'] = DFR['database'].str.lower()
                    DFR['dataset'] = DFR['dataset'].str.lower()
                except IOError as e:
                    self.stdout.write(self.style.ERROR('ERRO:')) 
                    print(e)
                    sys.exit(2)
                DFDB = pd.DataFrame(list(Database.objects.values()))
                DFR["db_id"] = DFR.set_index("database").index.map(DFDB.set_index("database")["id"])
                # tratar se nao localizar
                model_instances = [Dataset(
                    dataset = record.dataset,
                    database_id = record.db_id,
                    description = record.description,
                    update_ds = record.update_ds,
                    source_path = record.source_path,
                    source_web = record.source_web,
                    source_compact = record.source_compact,
                    source_file_name = record.source_file_name,
                    source_file_format = record.source_file_format,
                    source_file_sep = record.source_file_sep,
                    source_file_skiprow = record.source_file_skiprow,
                    target_file_name = record.target_file_name,
                    target_file_format = record.target_file_format,
                ) for record in DFR.itertuples()]
                Dataset.objects.bulk_create(model_instances, ignore_conflicts=True)        
                self.stdout.write(self.style.SUCCESS('  Load with success to Dataset'))
                
            elif v_table == 'ds_column':                  
                try:
                    DFR = pd.read_csv(v_path)
                    DFR = DFR.apply(lambda x: x.astype(str).str.lower()) 
                except IOError as e:
                    self.stdout.write(self.style.ERROR('ERRO:')) 
                    print(e)
                    sys.exit(2)
                DFG = pd.DataFrame(list(Dataset.objects.values()))
                DFR["dataset"] = DFR.set_index("dataset").index.map(DFG.set_index("dataset")["id"])
                DFR['status'] = DFR['status'].replace('false', 'False')
                DFR['status'] = DFR['status'].replace('true', 'True')
                DFR['single_word'] = DFR['single_word'].replace('false', 'False')
                DFR['single_word'] = DFR['single_word'].replace('true', 'True')
                if DFR.isnull().values.any():
                    self.stdout.write(self.style.ERROR('  Dataset was not match. Check log file')) 
                    DFR.to_csv(str(v_path + ".log"))
                    sys.exit(2)
                model_instances = [DSTColumn(
                    dataset_id = record.dataset,
                    status = record.status,
                    column_number = record.column_number,
                    column_name = record.column_name,
                    pre_value_id = record.pre_value,
                    single_word = record.single_word,
                    ) for record in DFR.itertuples()]
                DSTColumn.objects.bulk_create(model_instances, ignore_conflicts=True) 
                self.stdout.write(self.style.SUCCESS('  Load with success to DSTColumn'))

            elif v_table == 'keyge':  
                try:
                    DFR = pd.read_csv(v_path)
                    DFR = DFR.apply(lambda x: x.astype(str).str.lower()) 
                except IOError as e:
                    self.stdout.write(self.style.ERROR('ERRO:')) 
                    print(e)
                    sys.exit(2)
                DFG = pd.DataFrame(list(Group.objects.values()))
                DFC = pd.DataFrame(list(Category.objects.values()))
                DFR["group_id"] = DFR.set_index("group").index.map(DFG.set_index("group")["id"])
                DFR["category_id"] = DFR.set_index("category").index.map(DFC.set_index("category")["id"])
                if DFR.isnull().values.any():
                    self.stdout.write(self.style.ERROR('  Group and/or Category was not match. Check log file')) 
                    DFR.to_csv(str(v_path + ".log"))
                    sys.exit(2)
                model_instances = [Keyge(
                    keyge = record.keyge,
                    category_id = record.category_id,
                    group_id = record.group_id,
                    description = record.description,
                    ) for record in DFR.itertuples()]
                Keyge.objects.bulk_create(model_instances, ignore_conflicts=True) 
                self.stdout.write(self.style.SUCCESS('  Load with success to Keyge'))

            elif v_table == 'category':  
                try:
                    DFR = pd.read_csv(v_path)
                    DFR = DFR.apply(lambda x: x.astype(str).str.lower()) 
                except IOError as e:
                    self.stdout.write(self.style.ERROR('ERRO:')) 
                    print(e)
                    sys.exit(2)
                model_instances = [Category(
                    category = record.category,
                    description = record.description,
                    ) for record in DFR.itertuples()]
                Category.objects.bulk_create(model_instances, ignore_conflicts=True)
                self.stdout.write(self.style.SUCCESS('  Load with success to Category'))

            elif v_table == 'group':  
                try:
                    DFR = pd.read_csv(v_path)
                    DFR = DFR.apply(lambda x: x.astype(str).str.lower()) 
                except IOError as e:
                    self.stdout.write(self.style.ERROR('ERRO:')) 
                    print(e)
                    sys.exit(2)
                model_instances = [Group(
                    group = record.group,
                    description = record.description,
                    ) for record in DFR.itertuples()]
                Group.objects.bulk_create(model_instances, ignore_conflicts=True) 
                self.stdout.write(self.style.SUCCESS('  Load with success to Group'))

            elif v_table == 'prefix':  
                try:
                    DFR = pd.read_csv(v_path)
                    DFR = DFR.apply(lambda x: x.astype(str).str.lower()) 
                except IOError as e:
                    self.stdout.write(self.style.ERROR('ERRO:')) 
                    print(e)
                    sys.exit(2)
                model_instances = [PrefixOpc(
                    pre_value = record.pre_value,
                    ) for record in DFR.itertuples()]
                PrefixOpc.objects.bulk_create(model_instances, ignore_conflicts=True) 
                self.stdout.write(self.style.SUCCESS('  Load with success to Prefix'))

            elif v_table == 'keyword':       
                try:
                    DFR = pd.read_csv(v_path)
                    DFR = DFR.apply(lambda x: x.astype(str).str.lower()) 
                except IOError as e:
                    self.stdout.write(self.style.ERROR('ERRO:')) 
                    print(e)
                    sys.exit(2)  
                DFK = pd.DataFrame(list(Keyge.objects.values()))
                DFR["keyge_id"] = DFR.set_index("keyge").index.map(DFK.set_index("keyge")["id"])
                DFR['status'] = DFR['status'].replace('false', 'False')
                DFR['status'] = DFR['status'].replace('true', 'True')
                DFR['commute'] = DFR['commute'].replace('false', 'False')
                DFR['commute'] = DFR['commute'].replace('true', 'True')
                if DFR.isnull().values.any():
                    self.stdout.write(self.style.ERROR('  Keyge was not match. Check log file')) 
                    DFR.to_csv(str(v_path + ".log"))
                    sys.exit(2)
                    
                model_instances = [KeyWord(
                    keyge_id = record.keyge_id,
                    word = record.word,
                    status = record.status,
                    commute = record.commute,
                    ) for record in DFR.itertuples()]
                KeyWord.objects.bulk_create(model_instances, ignore_conflicts=True) 
                self.stdout.write(self.style.SUCCESS('  Load with success to KeyWords'))

            else:
                self.stdout.write(self.style.HTTP_NOT_FOUND('Table not recognized in the system. Choose one of the options: '))
                self.stdout.write(self.style.HTTP_NOT_FOUND('   database | dataset | ds_column | keyge | category | group | prefix | keywords'))


        # TRUNCATE BLOCK
        if options['truncate']:
            v_table = str(options['truncate']).lower()

            if v_table == 'all':   
                KeyLink.truncate()
                WordMap.truncate()
                KeyWord.truncate()
                KeyHierarchy.truncate()
                Keyge.truncate()
                Category.truncate()
                Group.truncate()
                LogsCollector.truncate()
                WFControl.truncate()
                DSTColumn.truncate()
                PrefixOpc.truncate()
                Dataset.truncate()
                Database.truncate()
                self.stdout.write(self.style.ERROR('  All tables deleted'))

            elif v_table == 'keylinks':  
                KeyLink.truncate()
                self.stdout.write(self.style.ERROR('  Keylinks table deleted'))

            elif v_table == 'wordmap':  
                WordMap.truncate()
                self.stdout.write(self.style.ERROR('  WordMap table deleted'))

            elif v_table == 'keyword':                  
                KeyWord.truncate()
                self.stdout.write(self.style.ERROR('  KeyWord table deleted'))
                
            elif v_table == 'keyhierarchy':                  
                KeyHierarchy.truncate()
                self.stdout.write(self.style.ERROR('  Hierarchy table deleted'))

            elif v_table == 'keyge':  
                Keyge.truncate()
                self.stdout.write(self.style.ERROR('  Keyge table deleted'))

            elif v_table == 'category':  
                Category.truncate()
                self.stdout.write(self.style.ERROR('  Category table deleted'))

            elif v_table == 'group':  
                Group.truncate()
                self.stdout.write(self.style.ERROR('  Group table deleted'))

            elif v_table == 'logs':  
                LogsCollector.truncate()
                self.stdout.write(self.style.ERROR('  Logs table deleted'))
                
            elif v_table == 'workflow':  
                WFControl.truncate()
                self.stdout.write(self.style.ERROR('  WorkFlow table deleted'))

            elif v_table == 'dst':  
                DSTColumn.truncate()
                self.stdout.write(self.style.ERROR('  Ds Column table deleted'))

            elif v_table == 'prefix':  
                PrefixOpc.truncate()
                self.stdout.write(self.style.ERROR('  Prefix table deleted'))

            elif v_table == 'dataset':  
                Dataset.truncate()
                self.stdout.write(self.style.ERROR('  Dataset table deleted'))

            elif v_table == 'database':  
                Database.truncate()
                self.stdout.write(self.style.ERROR('  Database table deleted'))
     
            else:
                self.stdout.write(self.style.HTTP_NOT_FOUND('Table not recognized in the system. Choose one of the options: '))
                self.stdout.write(self.style.HTTP_NOT_FOUND('   database | dataset | ds_column | workflow | keyge | category | group | prefix | key_word | link_key | wordmap'))


        # DELETE BLOCK
        if options['delete']:
            v_table = str(options['delete']).lower()

            if v_table == 'database':
                v_database = str(options['database']).lower()
                if v_database != 'all': 
                    v_where_cs = {'database': v_database}
                else:
                    v_where_cs = {}
                try:
                    qs_database = Database.objects.filter(**v_where_cs).delete()
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Database not found'))
                    sys.exit(2)            
                self.stdout.write(self.style.SUCCESS("  Database successfully deleted"))

            elif v_table == 'dataset':
                v_database =  str(options['database']).lower()
                v_dataset = str(options['dataset']).lower()

                if v_database != 'all':
                    try:
                        QS_DB = Database.objects.filter(database = v_database)
                        for qs in QS_DB:
                            v_db_id = qs.id
                    except:
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('  Database not found'))
                        sys.exit(2)
                    if not QS_DB:                     
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Dataset table'))
                        sys.exit(2)
                
                if v_database != 'all' and v_dataset != 'all': 
                    v_where_cs = {'database': v_db_id, 'dataset': v_dataset}
                elif v_database == 'all' and v_dataset != 'all': 
                    v_where_cs = {'dataset': v_dataset}
                elif v_database != 'all' and v_dataset == 'all': 
                    v_where_cs = {'database': v_db_id}                
                else:
                    v_where_cs = {}
                try:
                    QS = Dataset.objects.filter(**v_where_cs).delete()
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                    sys.exit(2)
                self.stdout.write(self.style.SUCCESS("  Dataset successfully deleted"))
                  
            elif v_table == 'ds_column':
                v_dataset = str(options['dataset']).lower()
                if v_dataset != 'all':
                    try:
                        QS_DB = Dataset.objects.filter(dataset = v_dataset)
                        for qs in QS_DB:
                            v_db_id = qs.id
                    except:
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                        sys.exit(2)
                    if not QS_DB:                     
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Dataset table'))
                        sys.exit(2)
                if v_dataset != 'all': 
                    v_where_cs = {'dataset': v_db_id}
                else:
                    v_where_cs = {}
                try:
                    QS = DSTColumn.objects.filter(**v_where_cs).delete()
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                    sys.exit(2)
                self.stdout.write(self.style.SUCCESS("  Dataset Column Transformation successfully deleted"))
                # Improvement: add column idx

            elif v_table == 'workflow':
                v_dataset = str(options['dataset']).lower()
                if v_dataset != 'all':
                    try:
                        QS_DB = Dataset.objects.filter(dataset = v_dataset)
                        for qs in QS_DB:
                            v_db_id = qs.id
                    except:
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                        sys.exit(2)
                    if not QS_DB:                     
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Dataset table'))
                        sys.exit(2)
                if v_dataset != 'all': 
                    v_where_cs = {'dataset': v_db_id}
                else:
                    v_where_cs = {}
                try:
                    WFControl.objects.filter(**v_where_cs).delete()
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                    sys.exit(2)
                self.stdout.write(self.style.SUCCESS("  Workflow successfully deleted"))

            elif v_table == 'keyge':
                v_group = str(options['group']).lower()
                v_category = str(options['category']).lower()
                v_keyge  = str(options['keyge']).lower()
                if v_group != 'all':
                    try:
                        QS_DB = Group.objects.filter(group = v_group)
                        for qs in QS_DB:
                            v_id_group = qs.id     
                    except:
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('  Group not found'))
                        sys.exit(2)
                    if not QS_DB:                     
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Group table'))
                        sys.exit(2)
                if v_category != 'all':
                    try:
                        QS_DB = Category.objects.filter(category = v_category)
                        for qs in QS_DB:
                            v_id_cat = qs.id     
                    except:
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('  Category not found'))
                        sys.exit(2)
                    if not QS_DB:                     
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data in Category table'))
                        sys.exit(2)
                if v_group != 'all' and v_category != 'all': 
                    v_where_cs = {'group': v_id_group, 'category': v_id_cat}
                elif v_group == 'all' and v_category != 'all': 
                    v_where_cs = {'category': v_id_cat}
                elif v_group != 'all' and v_category == 'all': 
                    v_where_cs = {'group': v_id_group}   
                elif v_keyge != 'all' :
                    v_where_cs = {'keyge': v_keyge}           
                else:
                    self.stdout.write(self.style.ERROR("  operation not performed"))
                    sys.exit(2)
                try:
                    Keyge.objects.filter(**v_where_cs).delete()
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Keyge not found'))
                    sys.exit(2)
                self.stdout.write(self.style.SUCCESS("  KEYGE successfully deleted"))


            elif v_table == 'category':
                v_category = str(options['category']).lower()
                if v_category == 'all':
                    self.stdout.write(self.style.ERROR("  Inform the Category"))
                    sys.exit(2)
                
                try:
                    QS = Category.objects.filter(category = v_category).delete()
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Category not found'))
                    sys.exit(2)
                self.stdout.write(self.style.SUCCESS("  Category successfully deleted"))
                

            elif v_table == 'group':
                v_group = str(options['group']).lower()
                if v_group == 'all':
                    self.stdout.write(self.style.ERROR("  Inform the Group"))
                    sys.exit(2)
                
                try:
                    QS = Group.objects.filter(group = v_group).delete()
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Category not found'))
                    sys.exit(2)
                self.stdout.write(self.style.SUCCESS("  Group successfully deleted"))

            elif v_table == 'prefix':
                v_prefix = str(options['prefix']).lower()
                if v_prefix == 'all':
                    self.stdout.write(self.style.ERROR("  Inform the Prefix"))
                    sys.exit(2)
                try:
                    PrefixOpc.objects.filter(pre_value = v_prefix).delete()
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Prefix not found'))
                    sys.exit(2)
                self.stdout.write(self.style.SUCCESS("  Prefix successfully deleted"))

            elif v_table == 'keyword':
                v_word = str(options['word']).lower()
                v_keyge = str(options['keyge']).lower()
                if v_word != 'all' and v_keyge != 'all':
                    v_where_cs = {'word__contains': v_word , 'keyge': v_keyge} # %like%
                elif v_word != 'all' and v_keyge == 'all':
                    v_where_cs = {'word__contains': v_word } # %like%
                elif v_word == 'all' and v_keyge != 'all':
                    v_where_cs = {'keyge': v_keyge} # %like%
                else:
                    self.stdout.write(self.style.ERROR("  Inform the Keyge and/or Word"))
                    sys.exit(2)
                try:
                    KeyWord.objects.filter(**v_where_cs).delete()
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Word not found'))
                    sys.exit(2)
                self.stdout.write(self.style.SUCCESS("  Keyword successfully deleted"))

                              
            elif v_table == 'link_key':
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Option not implemented'))

            elif v_table == 'wordmap':
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Option not implemented'))

            else:
                self.stdout.write(self.style.HTTP_NOT_FOUND('Table not recognized in the system. Choose one of the options: '))
                self.stdout.write(self.style.HTTP_NOT_FOUND('   database | dataset | ds_column | workflow | keyge | category | group | prefix | key_word | link_key | wordmap'))

