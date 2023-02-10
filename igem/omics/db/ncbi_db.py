# import os
# import sys

# import pandas as pd
# from django.conf import settings
# from django.db.models import Q

# # from django.db.models import Count, Q, Sum

# try:
#     x = str(settings.BASE_DIR)
#     sys.path.append(x)
#     from ncbi.models import snpgene
# except Exception as e:
#     print(e)
#     raise


# """
# python manage.py ncbi_db --load
#         snp-gene --path ~/DEV/GE/GE-DP/src/loader/snps/chrY.csv
# python manage.py ncbi_db --statistic snp-gene
# """


# def load(chunk=100000, table=None, path=None):
#     v_chunk = chunk

#     v_table = table.lower()
#     v_path = path.lower()

#     if v_path is None:
#         print('  Inform the path to load')
#         return 'failure'
#     if not os.path.isfile(v_path):
#         print('  File not found')
#         print('  Inform the path and the file in CSV format to load')
#         return 'failure'

#     if v_table == 'snp-gene':
#         v_index = 0
#         try:
#             for DFR in pd.read_csv(
#                 v_path,
#                 dtype=str,
#                 index_col=False,
#                 chunksize=v_chunk
#             ):
#                 DFR = DFR.apply(lambda x: x.astype(str).str.lower())
#                 model_instances = [snpgene(
#                     rsid=record.rsId,
#                     observed="",  # record.observed,
#                     genomicassembly=record.genomicAssembly,
#                     chrom=record.chrom,
#                     start=record.start,
#                     end=record.end,
#                     loctype="",  # record.locType,
#                     rsorienttochrom="",  # record.rsOrientToChrom,
#                     contigallele="",  # record.contigAllele,
#                     contig=record.contig,
#                     geneid=record.geneId,
#                     genesymbol=record.geneSymbol,
#                     ) for record in DFR.itertuples()]

#                 snpgene.objects.bulk_create(
#                     model_instances,
#                     ignore_conflicts=True
#                     )
#                 print('  Load with success to SNP-Gene table ')
#                 v_index += 1
#                 print("    Cicle:", v_index, "/ Rows:", len(DFR.index))

#         except IOError as e:
#             print('ERRO:')
#             print(e)
#             return 'failure'

#     else:
#         print('Table not recognized in the system. Choose one of the options: ')  # noqa E501
#         print('   snp-gene ')

#     return 'success'


# def statistic(table='snp-gene'):
#     v_table = table.lower()
#     if v_table == 'snp-gene':
#         # cursor = connection.cursor()
#         # cursor.execute(''' select count(*) from ncbi_snpgene where geneid = 'nan' ''') # noqa E501
#         # row = cursor.fetchone()
#         # print(row)
#         # Chromosome list:
#         # v_chrom = [''1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','x','y'']  # noqa E501
#         v_chrom = ['22']
#         for chrom in v_chrom:
#             print("Chromosome", chrom)
#             print(
#                 "   SNPs TOTAL:        ",
#                 snpgene.objects.filter(
#                     chrom=chrom
#                     ).count()
#                 )
#             print(
#                 "   SNPs without Genes:",
#                 snpgene.objects.filter(
#                     geneid='nan',
#                     chrom=chrom
#                     ).count()
#                 )
#             print(
#                 "   SNPs with Genes:   ",
#                 snpgene.objects.filter(
#                     ~Q(geneid='nan'),
#                     chrom=chrom
#                     ).count()
#                 )
#             print(
#                 "   Number of Genes:   ",
#                 snpgene.objects.filter(
#                     ~Q(geneid='nan'),
#                     chrom=chrom
#                     ).values("geneid").distinct().count()
#                 )
