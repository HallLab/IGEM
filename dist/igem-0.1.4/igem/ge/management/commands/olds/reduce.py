from operator import index
import os, time, sys
import pandas as pd
import numpy as np
from django.conf import settings
from django.core.management.base import BaseCommand
from ge.models import Dataset, WFControl, KeyLink, WordMap
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Sum

""" 
Sera o ultimo processo. o objetivo sera extrair os dados agregados da WORDMAP e carregar na KEYLINK

Subprocess:


Pendencies:
- Better chunk process control

"""


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

      
        if options['run']:
            v_time_process = time.time() 
            v_chunk = options['chunk']
            v_opt_ds = str(options['run']).lower()

            self.stdout.write(self.style.HTTP_NOT_MODIFIED('Start: Process to reduce keyge combiantion on GE.db'))

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


            # Start process dataset 
            for qs in qs_queryset:
                self.stdout.write(self.style.HTTP_NOT_MODIFIED ('  Start: Run database {0} on dataset {1}'.format(qs.database, qs.dataset)))
                v_time_ds = time.time()

                # Check Workflow 
                try:
                    qs_wfc = WFControl.objects.get(dataset_id = qs.id, chk_collect=True, chk_prepare=True, chk_map=True, chk_reduce=False)
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_NOT_FOUND('    Dataset without workflow to process'))
                    continue
                
                # Here, the WordMap of the Records is read with both Keyge fields assigned and in an aggregated form.
                DFR = pd.DataFrame(WordMap.objects.values("dataset_id","keyge1_id","keyge2_id").filter(dataset_id = qs.id, keyge1_id__isnull=False, keyge2_id__isnull=False).annotate(count=Sum("count")), columns=["dataset_id","keyge1_id","keyge2_id","count"])
                # .exclude(keyge1_id__isnull=True, keyge2_id__isnull=True, count__isnull=True)


                DFR = DFR.fillna(0)
                DFR.keyge1_id = DFR.keyge1_id.astype(int)  
                DFR.keyge2_id = DFR.keyge2_id.astype(int)  

                v_size = len(DFR.index)

                self.stdout.write(self.style.HTTP_SUCCESS('    {0} records loaded from RoadMap will be aggregated'.format(v_size))) 
                     
                if not DFR.empty:
                    v_lower = 0
                    v_upper = v_chunk

                    KeyLink.objects.filter(dataset_id = qs.id).delete()

                    while v_upper <= (v_size+v_chunk):
                        DFRC = (DFR[v_lower:v_upper])

                        model_keylink = [KeyLink(
                            ckey = str(str(record.dataset_id) + '-' + str(record.Index)),
                            dataset_id = record.dataset_id,
                            keyge1_id =  record.keyge1_id,
                            keyge2_id = record.keyge2_id,
                            count = record.count,
                            ) for record in DFRC.itertuples()]

                        KeyLink.objects.bulk_create(model_keylink)
                    
                        #self.stdout.write(self.style.HTTP_SUCCESS('    Writing records from {0} to {1} on KeyLink'.format(v_lower, v_upper))) 
                        v_lower += v_chunk
                        v_upper += v_chunk

                else:
                    self.stdout.write(self.style.HTTP_NOT_FOUND('    No data from {0} to update KeyLink table'.format(qs.dataset)))
                   
                #Update WorkFlow Control Process
                qs_wfc.chk_reduce = True
                qs_wfc.save()

                self.stdout.write(self.style.HTTP_REDIRECT('    Dataset loaded in {0} seconds'.format(int(time.time() - v_time_ds))))    

            self.stdout.write(self.style.SUCCESS('End of process in {0} seconds'.format(int(time.time() - v_time_process)))) 

        if options['reset']:
            v_opt_ds = str(options['reset']).lower()
            if  v_opt_ds == 'all':
                qs_wfc = WFControl.objects.all()
                qs_wfc.update(chk_reduce = False)                  
                self.stdout.write(self.style.SUCCESS('All datasets are defined for the prepare step'))
            else:
                try:
                    qs_wfc = WFControl.objects.get(dataset_id__dataset = v_opt_ds)
                    qs_wfc.chk_reduce = False
                    qs_wfc.save()                  
                    self.stdout.write(self.style.SUCCESS('Dataset {0} is defined for the prepare step'.format(v_opt_ds)))
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_NOT_FOUND('dataset {0} not fount'.format(v_opt_ds)))