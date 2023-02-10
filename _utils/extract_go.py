import pandas as pd

DF = pd.read_csv("~/dev/GE/GE-DP/src/psa/ctd/ctdcgoasso/CTD_chem_go_enriched.csv", skiprows=27, index_col=False)

DF = DF.drop(DF.columns[[0,1,2,3,6,7,8,9,10,11,12]], axis=1)
print(len(DF.index))
DF = DF.sort_values('GOTermID')
DF = DF.drop_duplicates(subset=['GOTermName', 'GOTermID'],keep=False)
print(len(DF.index))

DF.to_csv('~/dev/go.csv')





"""
# ChemicalName	ChemicalID	CasRN	Ontology	GOTermName	GOTermID	HighestGOLevel	PValue	CorrectedPValue	TargetMatchQty	TargetTotalQty	BackgroundMatchQty	BackgroundTotalQty
#												
10074-G5	C534883		Biological Process	negative regulation of cellular metabolic process	GO:0031324	3	4.89E-06	5.26E-03	4	4	2107	44767
"""