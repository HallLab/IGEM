# from clint.textui import progress
# from curses import update_lines_cols
# from turtle import update
import os
import sys
import time
from os.path import splitext

import pandas as pd
import patoolib
import requests
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand
from django.utils import timezone
from ge.models import Connector, WFControl

"""
First process in the data flow and aims to extract new versions of external datasources for the PSA area  # noqa E501

Version Control Rule:   Collect only considers connectors marked as active and different version control  # noqa E501
                        To reprocess a datasource with same version, use reset and run the connector  # noqa E501

Options:
--run_all       ==> Consider all active connectors to collect the files. 
--run "ds"      ==> Consider just one connector to collect the file.
--reset_all     ==> Reset version control to all connectors.
--reset "ds"    ==> Reset version control just one connector.
--show          ==> Print all connectors
--active "ds"   ==> Active a connector to collect the files
--deactive "ds" ==> Deactive a connector to collect the files


Pendencies
 - Create setting to active logs
 - How to handle zip with multi files

 - Add download process
    Clint isn't works ()

- Criar um check para conferir se o link eh valido ou nao.
- no load da connector nao posso mudar tudo para lowcase

"""


class Command(BaseCommand):
    help = 'Collect external datasources to PSA'

    def add_arguments(self, parser):

        parser.add_argument(
            '--run',
            type=str,
            metavar='connector',
            action='store',
            default=None,
            help='Will process active connectors and with new version',
        )

        parser.add_argument(
            '--reset',
            type=str,
            metavar='connector',
            action='store',
            default=None,
            help='Will reset connector version control',
        )

        parser.add_argument(
            '--show',
            action='store_true',
            help='Will show the Master Data connectors',
        )

        parser.add_argument(
            '--activate',
            type=str,
            metavar='connector',
            action='store',
            default=None,
            help='',
        )

        parser.add_argument(
            '--deactivate',
            type=str,
            metavar='connector',
            action='store',
            default=None,
            help='',
        )

    def handle(self, *args, **options):
        v_path_file = str(settings.BASE_DIR) + "/psa/"

        def splitext_(path):
            if len(path.split('.')) > 2:
                return path.split('.')[0], '.'.join(path.split('.')[-2:])
            return splitext(path)

        if options['run']:
            v_time_process = time.time()
            v_opt_ds = str(options['run']).lower()
            self.stdout.write(self.style.HTTP_NOT_MODIFIED('Start: Process to collect external datasources'))  # noqa E501
            if v_opt_ds == 'all':
                v_where_cs = {'update_ds': True}
            else:
                v_where_cs = {'update_ds': True, 'connector': v_opt_ds}
            try:
                qs_queryset = Connector.objects.filter(**v_where_cs)
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  connectors not found or disabled'))  # noqa E501
                sys.exit(2)
            if not qs_queryset:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  connectors not found or disabled'))  # noqa E501
                sys.exit(2)

            for qs in qs_queryset:
                self.stdout.write(self.style.HTTP_NOT_MODIFIED ('  Start: Run datasource {0} on connector {1}'.format(qs.datasource, qs.connector)))  # noqa E501
                v_time_ds = time.time()
                # Variables
                v_dir = v_path_file + str(qs.datasource) + "/" + qs.connector
                v_file_url = qs.source_path
                v_source_file = v_dir + "/" + qs.source_file_name
                v_target_file = v_dir + "/" + qs.target_file_name

                # Create folder to host file download
                if not os.path.isdir(v_dir):
                    os.makedirs(v_dir)
                    print("   Folder created to host the files in ", v_dir)

                # Get file source version from ETAG
                try:
                    #v_version = str(requests.get(v_file_url, stream=True).headers["etag"])  # noqa E501
                    v_version = requests.head(v_file_url).headers['Content-Length']  # noqa E501
                except:   # noqa E5722
                    self.stdout.write(self.style.HTTP_NOT_FOUND("    Could not find the version of the file. Check content-length attr"))  # noqa E501
                # Get WorkFlow Control
                try:
                    qs_wfc = WFControl.objects.get(connector_id=qs.id)
                except ObjectDoesNotExist:
                    qs_control = WFControl(
                        connector_id=qs.id,
                        last_update=timezone.now(),
                        source_file_version=0,
                        source_file_size=0,
                        target_file_size=0,
                        chk_collect=False,
                        chk_prepare=False,
                        chk_map=False,
                        chk_reduce=False
                    )
                    qs_control.save()
                    qs_wfc = WFControl.objects.get(connector_id=qs.id)

                # Check is new version before download
                if qs_wfc.source_file_version == v_version:
                    # Same vrsion, only write the log table
                    # Create a LOG setting control (optional to log control)
                    # log = LogsCollector(source_file_name = qs.source_file_name,   # noqa E501
                    #                     date = timezone.now(),
                    #                     connector = qs.connector,
                    #                     datasource = qs.datasource,
                    #                     version = v_version,
                    #                     status = False,
                    #                     size = 0)
                    # log.save()
                    self.stdout.write(self.style.HTTP_INFO('    Version already loaded in: {0}'.format(str(qs_wfc.last_update)[0:19])))  # noqa E501   
                    continue

                # New file version, start download
                else:
                    if os.path.exists(v_target_file):
                        os.remove(v_target_file)
                    if os.path.exists(v_source_file):
                        os.remove(v_source_file)

                    self.stdout.write(self.style.HTTP_SUCCESS('    Download start'))    # noqa E501 

                    r = requests.get(v_file_url, stream=True)
                    with open(v_source_file, "wb") as f:
                        # total_length = int(r.headers.get('content-length'))
                        # for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1):  # noqa E501
                        for chunk in r.iter_content(chunk_size=1000000):
                            if chunk:
                                f.write(chunk)
                                f.flush()

                    # Update LOG table if new version
                    v_size = str(os.stat(v_source_file).st_size)
                    # Create a LOG setting control (optional to log control)
                    # log = LogsCollector(source_file_name = qs.source_file_name,   # noqa E501
                    #                     date = timezone.now(), #datetime.datetime.now(),  # noqa E501
                    #                     connector = qs.connector,
                    #                     datasource = qs.datasource,
                    #                     version = v_version,
                    #                     status = True,
                    #                     size = v_size)
                    # log.save()
                    self.stdout.write(self.style.HTTP_SUCCESS('    Download finish'))   # noqa E501

                    # Unzip source file
                    if qs.source_compact:
                        try:
                            self.stdout.write(self.style.HTTP_SUCCESS('    Unzip start'))   # noqa E501
                            patoolib.extract_archive(str(v_source_file), outdir=str(v_dir), verbosity=-1)   # noqa E501
                            os.remove(v_source_file)
                        except:   # noqa E722
                            self.stdout.write(self.style.HTTP_BAD_REQUEST('    Failed to unzip file'))  # noqa E501
                            continue

                    # XML files to CSV
                    # This point is critical for memore consume
                    file_name, ext = splitext(v_target_file)
                    if qs.source_file_format == 'xml':
                        try:
                            v_src = str(file_name + '.xml')
                            DF = pd.read_xml(v_src)
                            v_csv = str(v_target_file)
                            DF.to_csv(v_csv, index=False)
                            os.remove(v_src)
                        except: # noqa E722
                            self.stdout.write(self.style.HTTP_BAD_REQUEST('    Failed to convert XML to CSV'))  # noqa E501
                    # Check if target file is ok
                    if not os.path.exists(v_target_file):
                        self.stdout.write(self.style.HTTP_BAD_REQUEST('    Failed to read file'))  # noqa E501
                        self.stdout.write(self.style.HTTP_SUCCESS('       Possible cause: check if the names of the source and destination files are correct in the connector table'))  # noqa E501
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
                    #         self.stdout.write(self.style.HTTP_BAD_REQUEST('    Failed to convert XML to CSV'))  # noqa E501
                    # Update WorkFlow Control table:
                    self.stdout.write(self.style.HTTP_SUCCESS('    Update workflow control'))  # noqa E501
                    qs_wfc.source_file_version = v_version
                    qs_wfc.source_file_size = v_size
                    qs_wfc.target_file_size = str(os.stat(v_target_file).st_size)  # noqa E501
                    qs_wfc.last_update = timezone.now()
                    qs_wfc.chk_collect = True
                    qs_wfc.chk_prepare = False
                    qs_wfc.chk_map = False
                    qs_wfc.chk_reduce = False
                    qs_wfc.save()

                    self.stdout.write(self.style.HTTP_REDIRECT('    connector loaded in {0} seconds'.format(int(time.time() - v_time_ds))))  # noqa E501

            self.stdout.write(self.style.SUCCESS('End of process in {0} seconds'.format(int(time.time() - v_time_process))))  # noqa E501

        if options['reset']:
            v_opt_ds = str(options['reset']).lower()
            if v_opt_ds == 'all':
                qs_wfc = WFControl.objects.all()
                qs_wfc.update(
                    last_update=timezone.now(),
                    source_file_version=0,
                    source_file_size=0,
                    target_file_size=0,
                    chk_collect=False,
                    chk_prepare=False,
                    chk_map=False,
                    chk_reduce=False)
                self.stdout.write(self.style.SUCCESS('All connectors are defined for the prepare step'))  # noqa E501
            else:
                try:
                    qs_wfc = WFControl.objects.get(connector_id__connector=v_opt_ds)  # noqa E501
                    qs_wfc.last_update = timezone.now()
                    qs_wfc.source_file_version = 0
                    qs_wfc.source_file_size = 0
                    qs_wfc.target_file_size = 0
                    qs_wfc.chk_collect = False
                    qs_wfc.chk_prepare = False
                    qs_wfc.chk_map = False
                    qs_wfc.chk_reduce = False
                    qs_wfc.save()
                    self.stdout.write(self.style.SUCCESS('connector {0} is defined for the prepare step'.format(v_opt_ds)))  # noqa E501
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_NOT_FOUND('connector {0} not fount'.format(v_opt_ds)))  # noqa E501

        if options['show']:
            qs_queryset = Connector.objects.all().order_by('datasource')
            v_db = 0
            for qs in qs_queryset:
                if v_db != qs.datasource:
                    self.stdout.write(self.style.HTTP_NOT_MODIFIED(qs.datasource))  # noqa E501
                self.stdout.write(self.style.HTTP_SUCCESS('  Id: {0} - status: {2} - connector: {1}'.format(qs.id, qs.connector, qs.update_ds)))      # noqa E501
                v_db = qs.datasource

        if options['activate']:
            try:
                v_opt_ds = str(options['activate']).lower()
                qs_wfc = Connector.objects.get(connector=v_opt_ds)  # noqa E501
                qs_wfc.update_ds = True
                qs_wfc.save()
                self.stdout.write(self.style.SUCCESS('connector activated'))
            except ObjectDoesNotExist:
                self.stdout.write(self.style.ERROR('Could not find connector'))

        if options['deactivate']:
            try:
                v_opt_ds = str(options['deactivate']).lower()
                qs_wfc = Connector.objects.get(connector=v_opt_ds)
                qs_wfc.update_ds = False
                qs_wfc.save()
                self.stdout.write(self.style.SUCCESS('connector dactivated'))
            except ObjectDoesNotExist:
                self.stdout.write(self.style.ERROR('Could not find connector'))
