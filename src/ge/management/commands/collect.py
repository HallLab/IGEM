# from clint.textui import progress
# from curses import update_lines_cols
# from turtle import update
import os
import sys
import requests
import patoolib
import time
from django.conf import settings
from django.core.management.base import BaseCommand
from ge.models import Dataset, WFControl
from django.utils import timezone
from django.core.exceptions import ObjectDoesNotExist
from os.path import splitext
import pandas as pd
""" 
First process in the data flow and aims to extract new versions of external databases for the PSA area

Version Control Rule:   Collect only considers datasets marked as active and different version control
                        To reprocess a database with same version, use reset and run the dataset

Options:
--run_all       ==> Consider all active datasets to collect the files. 
--run "ds"      ==> Consider just one dataset to collect the file.
--reset_all     ==> Reset version control to all datasets.
--reset "ds"    ==> Reset version control just one dataset.
--show          ==> Print all datasets
--active "ds"   ==> Active a dataset to collect the files
--deactive "ds" ==> Deactive a dataset to collect the files


Pendencies
 - Create setting to active logs
 - How to handle zip with multi files

 - Add download process
    Clint isn't works ()

- Criar um check para conferir se o link eh valido ou nao.
- no load da Dataset nao posso mudar tudo para lowcase

"""

class Command(BaseCommand):
    help = 'Collect external databases to PSA'

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
            '--show',
            action='store_true',
            help='Will show the Master Data Datasets',
        )

        parser.add_argument(
            '--activate',
            type=str,
            metavar='dataset',
            action='store',
            default=None,
            help='',
        )

        parser.add_argument(
            '--deactivate',
            type=str,
            metavar='dataset',
            action='store',
            default=None,
            help='',
        )

    def handle(self, *args, **options):
        v_path_file = str(settings.BASE_DIR) + "/psa/"



        def splitext_(path):
            if len(path.split('.')) > 2:
                return path.split('.')[0],'.'.join(path.split('.')[-2:])
            return splitext(path)


        if options['run']:
            v_time_process = time.time()                   
            v_opt_ds = str(options['run']).lower()
            
            self.stdout.write(self.style.HTTP_NOT_MODIFIED('Start: Process to collect external databases'))
     


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


            for qs in qs_queryset:
                self.stdout.write(self.style.HTTP_NOT_MODIFIED ('  Start: Run database {0} on dataset {1}'.format(qs.database, qs.dataset)))
                v_time_ds = time.time()
           
                # Variables                    
                v_dir = v_path_file + str(qs.database) + "/" + qs.dataset
                v_file_url = qs.source_path
                v_source_file = v_dir + "/" + qs.source_file_name
                v_target_file = v_dir + "/" + qs.target_file_name

                # Create folder to host file download
                if not os.path.isdir(v_dir):
                    os.makedirs(v_dir)
                    print("   Folder created to host the files in ", v_dir)

                # Get file source version from ETAG
                try:
                    #v_version = str(requests.get(v_file_url, stream=True).headers["etag"])
                    v_version = requests.head(v_file_url).headers['Content-Length']
                except:
                    self.stdout.write(self.style.HTTP_NOT_FOUND("    Could not find the version of the file. Check content-length attr"))
                    
                
                # Get WorkFlow Control
                try:
                    qs_wfc = WFControl.objects.get(dataset_id = qs.id)
                except ObjectDoesNotExist:
                    qs_control = WFControl(
                        dataset_id = qs.id,
                        last_update = timezone.now(),
                        source_file_version = 0,
                        source_file_size = 0,
                        target_file_size = 0,
                        chk_collect = False,
                        chk_prepare = False,
                        chk_map = False,
                        chk_reduce = False
                    )
                    qs_control.save()
                    qs_wfc = WFControl.objects.get(dataset_id = qs.id)

                # Check is new version before download
                if qs_wfc.source_file_version == v_version:
                    # Same vrsion, only write the log table
                    # Create a LOG setting control (optional to log control)
                    # log = LogsCollector(source_file_name = qs.source_file_name, 
                    #                     date = timezone.now(),
                    #                     dataset = qs.dataset,
                    #                     database = qs.database,
                    #                     version = v_version,
                    #                     status = False,
                    #                     size = 0) 
                    # log.save() 
                    self.stdout.write(self.style.HTTP_INFO('    Version already loaded in: {0}'.format(str(qs_wfc.last_update)[0:19])))          
                    continue

                # New file version, start download
                else:   
                    if os.path.exists(v_target_file):
                        os.remove(v_target_file)
                    if os.path.exists(v_source_file):
                        os.remove(v_source_file)

                    
                    self.stdout.write(self.style.HTTP_SUCCESS('    Download start'))   

                    r = requests.get(v_file_url, stream=True)
                    with open(v_source_file, "wb") as f:
                        # total_length = int(r.headers.get('content-length'))
                        # for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1):
                        for chunk in r.iter_content(chunk_size=1000000):
                                if chunk:
                                    f.write(chunk)
                                    f.flush()


                    # Update LOG table if new version
                    v_size = str(os.stat(v_source_file).st_size)
                    # Create a LOG setting control (optional to log control)
                    # log = LogsCollector(source_file_name = qs.source_file_name, 
                    #                     date = timezone.now(), #datetime.datetime.now(),
                    #                     dataset = qs.dataset,
                    #                     database = qs.database,
                    #                     version = v_version,
                    #                     status = True,
                    #                     size = v_size) 
                    # log.save()
                    self.stdout.write(self.style.HTTP_SUCCESS('    Download finish')) 

                    # Unzip source file
                    if qs.source_compact:
                        try:
                            self.stdout.write(self.style.HTTP_SUCCESS('    Unzip start')) 
                            patoolib.extract_archive(str(v_source_file), outdir=str(v_dir), verbosity=-1) 
                            os.remove(v_source_file)
                        except:
                            self.stdout.write(self.style.HTTP_BAD_REQUEST('    Failed to unzip file'))
                            continue

                    # XML files to CSV
                    # This point is critical for memore consume
                    file_name,ext = splitext(v_target_file)
                    if qs.source_file_format =='xml':
                        try:
                            v_src = str(file_name + '.xml')
                            DF = pd.read_xml(v_src)
                            v_csv = str(v_target_file)
                            DF.to_csv(v_csv, index=False)
                            os.remove(v_src) 
                        except:
                            self.stdout.write(self.style.HTTP_BAD_REQUEST('    Failed to convert XML to CSV'))


                    # Check if target file is ok
                    if not os.path.exists(v_target_file): 
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('    Failed to read file'))
                        self.stdout.write(self.style.HTTP_SUCCESS('       Possible cause: check if the names of the source and destination files are correct in the dataset table'))
                        qs_wfc.source_file_version = "ERROR"
                        qs_wfc.last_update = timezone.now()
                        qs_wfc.save()
                        for i in os.listdir(v_dir):
                            os.remove(v_dir + "/" + i)
                        continue


                    # # XML files to CSV
                    # # This point is critical for memore consume
                    # file_name,ext = splitext(v_target_file)
                    # if ext == '.xml':
                    #     try:
                    #         DF = pd.read_xml(v_target_file)
                    #         v_csv = str(file_name + '.csv')
                    #         DF.to_csv(v_csv, index=False)
                    #         os.remove(v_target_file)
                    #         v_target_file = v_csv
                    #     except:
                    #         self.stdout.write(self.style.HTTP_BAD_REQUEST('    Failed to convert XML to CSV'))


                    # Update WorkFlow Control table:
                    self.stdout.write(self.style.HTTP_SUCCESS('    Update workflow control'))
                    qs_wfc.source_file_version = v_version
                    qs_wfc.source_file_size = v_size
                    qs_wfc.target_file_size = str(os.stat(v_target_file).st_size)
                    qs_wfc.last_update = timezone.now()
                    qs_wfc.chk_collect = True
                    qs_wfc.chk_prepare = False
                    qs_wfc.chk_map = False
                    qs_wfc.chk_reduce = False
                    qs_wfc.save()

                    self.stdout.write(self.style.HTTP_REDIRECT('    Dataset loaded in {0} seconds'.format(int(time.time() - v_time_ds))))
                   
            self.stdout.write(self.style.SUCCESS('End of process in {0} seconds'.format(int(time.time() - v_time_process))))



        if options['reset']:
            v_opt_ds = str(options['reset']).lower()
              
            if  v_opt_ds == 'all':
                qs_wfc = WFControl.objects.all()
                qs_wfc.update(last_update = timezone.now(),
                                source_file_version = 0,
                                source_file_size = 0,
                                target_file_size = 0,
                                chk_collect = False,
                                chk_prepare = False,
                                chk_map = False,
                                chk_reduce = False)                  
                self.stdout.write(self.style.SUCCESS('All datasets are defined for the prepare step'))
            else:
                try:
                    qs_wfc = WFControl.objects.get(dataset_id__dataset = v_opt_ds)
                    qs_wfc.last_update = timezone.now()
                    qs_wfc.source_file_version = 0
                    qs_wfc.source_file_size = 0
                    qs_wfc.target_file_size = 0
                    qs_wfc.chk_collect = False
                    qs_wfc.chk_prepare = False
                    qs_wfc.chk_map = False
                    qs_wfc.chk_reduce = False
                    qs_wfc.save()                  
                    self.stdout.write(self.style.SUCCESS('Dataset {0} is defined for the prepare step'.format(v_opt_ds)))
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_NOT_FOUND('dataset {0} not fount'.format(v_opt_ds)))
  

        if options['show']:
            qs_queryset = Dataset.objects.all().order_by('database')
            v_db = 0
            for qs in qs_queryset:
                if v_db != qs.database:
                    self.stdout.write(self.style.HTTP_NOT_MODIFIED(qs.database))
                self.stdout.write(self.style.HTTP_SUCCESS('  Id: {0} - status: {2} - dataset: {1}'.format(qs.id, qs.dataset, qs.update_ds)))    
                v_db = qs.database


        if options['activate']:
            try:
                v_opt_ds = str(options['activate']).lower()
                qs_wfc = Dataset.objects.get(dataset = v_opt_ds)
                qs_wfc.update_ds=True
                qs_wfc.save() 
                self.stdout.write(self.style.SUCCESS('dataset activated'))
            except ObjectDoesNotExist:
                self.stdout.write(self.style.ERROR('Could not find dataset'))

        if options['deactivate']:
            try:
                v_opt_ds = str(options['deactivate']).lower()
                qs_wfc = Dataset.objects.get(dataset = v_opt_ds)
                qs_wfc.update_ds=False
                qs_wfc.save() 
                self.stdout.write(self.style.SUCCESS('dataset dactivated'))
            except ObjectDoesNotExist:
                self.stdout.write(self.style.ERROR('Could not find dataset'))
                          