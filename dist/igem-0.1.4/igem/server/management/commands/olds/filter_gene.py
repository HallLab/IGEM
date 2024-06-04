""" 
gene_snp
=========================================================================================================
Example:
$ python manage.py filter --gene_snp ~/.../{}.csv
----------------------------------------------------------------------------------------------------------

OUTPUT CSV Structure
    gene / snp / exposome / count / qtd sources / (sources ID)
"""


import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
from django.db.models import Sum
from ge.models import KeyLink, WordMap
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q 
from django.core.management.base import BaseCommand
from ge.models import KeyWord
from django.core.exceptions import ObjectDoesNotExist
from concurrent.futures import as_completed
from django_thread import ThreadPoolExecutor

from ncbi.models import snpgene

import time


class Command(BaseCommand):
    help = 'Get data from Igem Database'

    def add_arguments(self, parser):

        parser.add_argument(
            '--gene_snp',
            type=str,
            metavar='file path',
            action='store',
            default=None,
            help='group value',
        )

        
    def handle(self, *args, **options):

# KEYLINK  
        if options['gene_snp']:
            v_path_in = str(options['gene_snp']).lower()
            v_path_out = os.path.dirname(v_path_in) + "/output_gene_snp.csv" # standard value before parameter file set
            
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
           
            
            # vou manter inicialmente a mesma estrutura, possibilitando ao usuario isorar um conjunto de dados
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

                # Teremos que sempre trazer a dataset ou database para realizar o agrupamento manualmente 
                if row['index'] == 'output':
                    if row['parameter'] == 'database':
                        if row['value'] == 'no':
                            v_ck_database   = True # Always True to manual aggregation
                    if row['parameter'] == 'dataset':
                        if row['value'] == 'no':
                            v_ck_dataset   = True # Always True to manual aggregation
                    if row['parameter'] == 'group':
                        if row['value'] == 'no':
                            v_ck_group   = True # Always True to manual aggregation
                    if row['parameter'] == 'category':
                        if row['value'] == 'no':
                            v_ck_category   = True # Always True to manual aggregation
                    if row['parameter'] == 'keyge':
                        if row['value'] == 'no':
                            v_ck_keyge   = True # Always need the keyge (for this process will be the Genes)

                # Overwrite the standard value            
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

            # Realizar um loop na DFR para ajustar quem sera Gene e quem sera Exposome
            # Bloco para agrupamento de Dataset
            DFR.sort_values(['keyge_1','keyge_2'], ascending=[True,True], inplace=True)

            DFR_NEW = pd.DataFrame(columns=['gene_id','gene_desc','exp_grp','exp_cat','exp_id','exp_desc','count','sources','dataset'])

            v_chk_key = ''
            v_chk_index = ''
            v_chk_source  = ''
            v_chk_qtd = ''
            v_chk_count = 0

            for index, row in DFR.iterrows():
                
                # Ajusta possicoes dos campos
                if row['category_1'] == 'gene' and row['group_2'] == 'environment':
                    v_gene_id = row['keyge_1']
                    v_gene_desc = row['description_1']
                    v_exp_grp = row['group_2']
                    v_exp_cat = row['category_2']
                    v_exp_id = row['keyge_2']
                    v_exp_desc = row['description_2']

                elif row['category_2'] == 'gene' and row['group_1'] == 'environment':
                    v_gene_id = row['keyge_2']
                    v_gene_desc = row['description_2']
                    v_exp_grp = row['group_1']
                    v_exp_cat = row['category_1']
                    v_exp_id = row['keyge_1']
                    v_exp_desc = row['description_1']

                else:
                    continue # add only Gene Category x Environment Group

                v_chk = v_gene_id + "-" + v_exp_id           

                # Check and process new and repeated records
                if v_chk != v_chk_key: # NEW
                    v_chk_source = str(row['dataset'])
                    v_chk_qtd = 1
                    v_chk_count = row['count']
                    v_chk_index = index
                elif v_chk == v_chk_key: # REPEATED
                    v_chk_source = v_chk_source + ',' + str(row['dataset'])
                    v_chk_qtd += 1
                    v_chk_count = v_chk_count + row['count']
                    v_chk_index = v_chk_index
                else:
                    print(" ERROR ON CHECK  ")

                DFR_NEW.loc[v_chk_index] = [v_gene_id, v_gene_desc, v_exp_grp, v_exp_cat, v_exp_id, v_exp_desc, v_chk_count, v_chk_qtd, v_chk_source]
                v_chk_key = v_chk 
                
            DFR_NEW.sort_values(['gene_id','exp_id'], ascending=[True,True], inplace=True)

            print(DFR_NEW)
            print("      ")

            DFR_NEW.to_csv(v_path_out, index=False)        
            self.stdout.write(self.style.SUCCESS('File with the results sucessfully created in %s' % str(v_path_out)))
            

            # Bloco para buscar e abrir por SNP

            # Separar os Genes e eliminar duplicidades
            # Ajustar os genes para buscar no NCBI (eliminar o prefixo GENE:)
            # selecionar esses gene e trazer os dados de snipers e Chrome
            # explodir esses snps pelo links


            
            ## tentar primeiro o processo de merge
            # for index, row in DFR_NEW.iterrows():
            #     v_gene = str(row['gene_id']).replace("gene:","")
            #     try:
            #         qs_queryset = snpgene.objects.filter(geneid = v_gene)           
            #     except ObjectDoesNotExist:
            #         self.stdout.write(self.style.HTTP_BAD_REQUEST('  Datasets not found or disabled'))
            #         # sys.exit(2)
            #     if not qs_queryset:
            #         self.stdout.write(self.style.HTTP_BAD_REQUEST('  Datasets not found or disabled'))
            #         # sys.exit(2)
            #     for qs in qs_queryset:
            #         #qs.campo
            #         v_time_ds = time.time()
            




            DFR_GENE = DFR_NEW['gene_id']
            DFR_GENE.drop_duplicates(inplace=True)
            DFR_GENE = DFR_GENE.apply(lambda x: str(x).strip('gene:')) # Remove gene: prefix from values to match on ncbi_snpgene

            
            print(DFR_NEW)
            
            list_gene = DFR_GENE.to_list()
  
            # print("======= LIST ++++++")
            # print(list_gene)

            
            try:
                # DFR = pd.DataFrame(KeyLink.objects.filter(**v_filter). \
                DFR_SNP = pd.DataFrame(snpgene.objects.filter(geneid__in = list_gene).values('rsid','chrom','start','end','contig','geneid','genesymbol'))
            
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  GE.db query error'))
                #sys.exit(2)
            if DFR_SNP.empty:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  No data found with the given parameters'))
                #sys.exit(2)

            print(" --- SNPs ------")

            DFR_SNP['gene_id'] = str('gene:') + DFR_SNP['geneid']
            print(DFR_SNP)

            df = pd.merge(DFR_NEW, DFR_SNP, how='left', on='gene_id')
         
            #df.drop(['exp_grp','geneid'], inplace=True)

            df = df.reindex(columns=['gene_id','gene_desc','genesymbol','rsid','chrom','contig','start','end','exp_cat','exp_id','exp_desc','count','sources','dataset'])

            print(df)

            df.to_csv('teste000000000.csv', index=False) 
    #         class snpgene(models.Model):
    # rsid = models.CharField(max_length=15, unique=True, verbose_name="SNP ID")
    # observed = models.CharField(max_length=30, verbose_name="observed")
    # genomicassembly = models.CharField(max_length=20, verbose_name="Assembly")
    # chrom = models.CharField(max_length=5, verbose_name="Chromosome")
    # start = models.CharField(max_length=15, verbose_name="Start")
    # end = models.CharField(max_length=15, verbose_name="End")
    # loctype = models.CharField(max_length=5, verbose_name="Local Type")
    # rsorienttochrom = models.CharField(max_length=5, verbose_name="Orient Chrom")
    # contigallele = models.CharField(max_length=20, verbose_name="Contig Allele")
    # contig = models.CharField(max_length=20, verbose_name="Contig")
    # geneid = models.CharField(max_length=15, verbose_name="Gene ID")
    # genesymbol = models.CharField(max_length=30, verbose_name="Gene Symbol")



            """
            Teoricamente nao precisamos desse novo processo 2 porque ja chassificamos as keyge no momento de gravar na tabela, sendo que uma combinacao entre chave nao teremos posicoes invertidas
            Analisar melhor

            DFR_NEW_2 = pd.DataFrame(columns=['gene_id','gene_desc','exp_grp','exp_cat','exp_id','exp_desc','count','sources','dataset'])
            
            for index, row in DFR_NEW.iterrows():
                
                v_chk = row['gene_id'] + "-" + row['exp_id']           

                # Check and process new and repeated records
                if v_chk != v_chk_key: # NEW
                    v_chk_source = str(row['dataset'])
                    v_chk_qtd = 1
                    v_chk_index = index
                elif v_chk == v_chk_key: # REPEATED
                    v_chk_source = v_chk_source + ',' + str(row['dataset'])
                    v_chk_qtd += 1
                    v_chk_index = v_chk_index
                else:
                    print(" ERROR ON CHECK  ")

            
                DFR_NEW_2.loc[v_chk_index] = [row['gene_id'], row['gene_desc'], row['exp_grp'], row['exp_cat'], row['exp_id'], row['exp_desc'], row['count'], v_chk_qtd, v_chk_source]


            """
            
            
            
            
            """            if v_chk != v_chk_key:
                    if row['category_1'] == 'gene':
                        if row['group_2'] == 'environment': # add only Environment group as exposome group
                            DFR_NEW.loc[index] = [row['keyge_1'], row['description_1'],row['group_2'],row['category_2'],row['keyge_2'],row['description_2'],row['count'],1,row['dataset']]
                            
                            v_chk_source = row['dataset']
                            v_chk_qtd = 1
                            v_chk_index = index

                    elif row['category_2'] == 'gene':
                        if row['group_1'] == 'environment': # add only Environment group as exposome group
                            DFR_NEW.loc[index] = [row['keyge_2'], row['description_2'],row['group_1'],row['category_1'],row['keyge_1'],row['description_1'],row['count'],1,row['dataset']]
                            v_chk_source = row['dataset']
                            v_chk_qtd = 1
                            v_chk_index = index
                            
                # Aggr row
                if v_chk == v_chk_key:       
                    if row['category_1'] == 'gene':
                        if row['group_2'] == 'environment': # add only Environment group as exposome group
                            v_chk_source = v_chk_source + ',' + str(row['dataset'])
                            v_chk_qtd += 1
                            DFR_NEW.loc[v_chk_index] = [row['keyge_1'], row['description_1'],row['group_2'],row['category_2'],row['keyge_2'],row['description_2'],row['count'],v_chk_qtd,v_chk_source]
                            #v_chk_index = v_chk_index # keep same index

                    elif row['category_2'] == 'gene':
                        if row['group_1'] == 'environment': # add only Environment group as exposome group
                            v_chk_source = v_chk_source + ',' + str(row['dataset'])
                            v_chk_qtd += 1
                            DFR_NEW.loc[v_chk_index] = [row['keyge_2'], row['description_2'],row['group_1'],row['category_1'],row['keyge_1'],row['description_1'],row['count'],v_chk_qtd,v_chk_source]
"""

            
