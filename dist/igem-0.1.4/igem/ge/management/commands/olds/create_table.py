# import sys

# import pandas as pd
# from django.conf import settings
# from django.db.models import Q, Sum

# try:
#     x = str(settings.BASE_DIR)
#     sys.path.append(x)
#     from ge.models import TermMap
#     from ncbi.models import snpgene
# except Exception as e:
#     print(e)
#     raise


# def create_table(path=None):
#     # Debug Scenario
#     v_term = []
#     # v_term.append('2085537')
#     v_term.append("2490758")
#     query_term = Q(term_1__in=(v_term))
#     query_term.add(Q(term_2__in=(v_term)), Q.OR)

#     # Where only registr has Gene Category associated
#     v_term_category = []
#     v_term_category.append("5")
#     query_term_category = Q(term_1__term_category_id__in=(v_term_category))
#     query_term_category.add(Q(term_2__term_category_id__in=(v_term_category)), Q.OR)

#     # Select fields to extract from KeyLink Table
#     v_aggr = []
#     v_aggr.append("connector__connector")
#     # v_aggr.append('term_1__group_id')
#     # v_aggr.append('term_2__group_id')
#     v_aggr.append("term_1__term_category_id__term_category")
#     v_aggr.append("term_2__term_category_id__term_category")
#     v_aggr.append("term_1__term")
#     v_aggr.append("term_2__term")
#     v_aggr.append("term_1__description")
#     v_aggr.append("term_2__description")

#     # Run query
#     DFR = pd.DataFrame(
#         TermMap.objects.filter(query_term_category, query_term)
#         .values(*v_aggr)
#         .annotate(qtd_links=Sum("qtd_links"))
#     )

#     # Normalization Gene and Linked term in a single column
#     # --> consum a lot of resource and time (look for better process)
#     for index, row in DFR.iterrows():
#         # Normalization Gene and Linked term in a single column
#         if row["term_1__term_category_id__term_category"] == "gene":
#             DFR.loc[index, "geneid"] = row["term_1__term"]
#             DFR.loc[index, "linkid"] = row["term_2__term"]
#             DFR.loc[index, "linkname"] = row["term_2__description"]
#             DFR.loc[index, "linkcat"] = row["term_2__term_category_id__term_category"]

#         elif row["term_2__term_category_id__term_category"] == "gene":
#             DFR.loc[index, "geneid"] = row["term_2__term"]
#             DFR.loc[index, "linkid"] = row["term_1__term"]
#             DFR.loc[index, "linkname"] = row["term_1__description"]
#             DFR.loc[index, "linkcat"] = row["term_1__term_category_id__term_category"]

#         DFR.loc[index, "sources"] = 1
#         # adicionar os novos campos na DFR

#     if DFR.empty:
#         print("DF Empty")
#         return "success"

#     DFR.drop(
#         [
#             "term_1__term_category_id__term_category",
#             "term_2__term_category_id__term_category",
#             "term_1__term",
#             "term_2__term",
#             "connector__connector",
#         ],
#         axis=1,
#         inplace=True,
#     )

#     DFR.sort_values(["geneid", "linkid"], inplace=True)

#     DFR = DFR.groupby(["geneid", "linkid", "linkname", "linkcat"], as_index=False).agg(
#         "sum"
#     )

#     print(" ")
#     print("TermMap: ")
#     print(DFR)

#     # Eliminar todos os registros em que os Links sao do Grupo de Genomics
#     # Part 2: get SNP informafion and slip this

#     # Debug Scenario
#     v_snp = []
#     # v_snp.append('1491480232')
#     # query_snp = (Q(rsId__in=(v_snp)))
#     v_snp.append("246126")
#     query_snp = Q(geneid__in=(v_snp))

#     # Select fields to extract from SNP Table
#     v_aggr_snp = []
#     v_aggr_snp.append("rsid")
#     # v_aggr_snp.append('observed')
#     # v_aggr_snp.append('genomicAssembly')
#     v_aggr_snp.append("chrom")
#     v_aggr_snp.append("start")
#     v_aggr_snp.append("end")
#     # v_aggr_snp.append('locType')
#     # v_aggr_snp.append('rsOrientToChrom')
#     # v_aggr_snp.append('contigAllele')
#     # v_aggr_snp.append('contig')
#     v_aggr_snp.append("geneid")
#     v_aggr_snp.append("genesymbol")

#     # Run query
#     DF_SNP = pd.DataFrame(
#         snpgene.objects.filter(query_snp).values(*v_aggr_snp).annotate()
#     )

#     DF_SNP["geneid"] = "gene:" + DF_SNP["geneid"].astype(str)

#     print(" ")
#     print("SNPs: ")
#     print(DF_SNP)

#     # merge dos dois Dataframes

#     DF_M = pd.merge(DFR, DF_SNP, how="left", on="geneid")

#     print(" ")
#     print("Keylink and SNPs: ")
#     print(DF_M)

#     return "success"
