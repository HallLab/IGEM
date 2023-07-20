"""

AUTHOR: Andre Rico
DATE: 2022/10/14 

Script to transform NCBI/ENTREZ XML Files into CSV. In this case, it was adapted for 
dbSNP and selected only SNP and Genes per Chromosome information.

Prcess:
1. Download the NCBI zipped file (https://ftp.ncbi.nih.gov/snp/organisms/human_9606/XML/)
2. Unzip the file
3. Map the file in the script by the variable v_path
4. Run the Script
5. The output will be a CSVfile in the same directory as the v_path
6. If exist more files to process, just run the script for each file (chromossome)

Possible Improvements:
- Download files automatic and decompression
- Option of witch chromosome to process or all
- Process to consolidate all result files into a single or even a database
- Routine to choose output fields fexible

SNP XML Structure
-- this part only includes the rsID block of the file
1. TAG: SnpInfo / ATTR: rsid, observed
    1.1. TAG: SnpLoc / ATTR: genomicAssembly, geneid, geneSymbol, chrom, start, locType, rsOrier, contigAllele, contig
    1.2. TAG: Ssinfo / ATTR: ssId, locSnpid, ssOrientToRs
        1.2.1. TAG: ByPop / ATTR: popid, sampleSize
            1.2.1.1. TAG: AlleleFreg / ATTR: allele, freq
            1.2.1.2. TAG: GTypeFreq / ATTR: gtype , freq
    1.3. TAG: GTypeFreq / ATTR: gtype , freq


OUTPUT CSV Structure
rsID, genomicAssembly, geneid, geneSymbol, chrom, start, locType, rsOrier, contigAllele, contig

"""


from datetime import datetime
from xml.etree.ElementTree import iterparse

import pandas as pd

# PARAMETERS:
v_path = r"/Users/andrerico/DEV/LAB/TESTE_XML/gt_chr22.xml"
v_path_output = r"/Users/andrerico/DEV/LAB/TESTE_XML/chr22.csv"


tm_start = datetime.now()
data = []

# Method to not overload the memory
# Impotant: The XML file does not have the suffix, but it will be necessary to know witch lines to read.
for _, SnpInf in iterparse(v_path, events=("end",)):
    if SnpInf.tag == "{http://www.ncbi.nlm.nih.gov/SNP/geno}SnpInfo":
        Snp_id = {}
        Snp_id = SnpInf.attrib
        for child in SnpInf:
            if child.tag == "{http://www.ncbi.nlm.nih.gov/SNP/geno}SnpLoc":
                Snp_Loc = {}
                Snp_Loc = Snp_id.copy()
                Snp_Loc.update(child.attrib)
                data.append(Snp_Loc)

        SnpInf.clear()

# Transform rows in dataframe
df = pd.DataFrame.from_dict(data)
print("The chromosome has: ", len(df.index), " SNPs mapped")


df.to_csv(v_path_output)
print("CSV file created at: ", v_path_output)

tm_end = datetime.now() - tm_start
print("Process done in: ", tm_end)


"""
# NOTE: Script generic to run
from xml.etree.ElementTree import iterparse
import pandas as pd
file_path = r"/path/to/Input.xml"
dict_list = []
for _, elem in iterparse(file_path, events=("end",)):
    if elem.tag == "row":
        dict_list.append({'rowId': elem.attrib['Id'],
                          'UserId': elem.attrib['UserId'],
                          'Name': elem.attrib['Name'],
                          'Date': elem.attrib['Date'],
                          'Class': elem.attrib['Class'],
                          'TagBased': elem.attrib['TagBased']})
        # dict_list.append(elem.attrib)      # ALTERNATIVELY, PARSE ALL ATTRIBUTES
        elem.clear()
df = pd.DataFrame(dict_list)
"""
"""
# NOTE: Script Consulm more process and memory
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd
tm_start = datetime.now()
tree  = ET.parse('/Users/andrerico/DEV/LAB/TESTE_XML/SNP/gt_chrY.xml')
root = tree.getroot()
data = []
for SnpInf in root.iter('{http://www.ncbi.nlm.nih.gov/SNP/geno}SnpInfo'):
    #print("SNP Code: ", SnpInf.attrib["rsId"])ß
    Snp_id = {}
    Snp_id = SnpInf.attrib
    for child in SnpInf:
        if child.tag == "{http://www.ncbi.nlm.nih.gov/SNP/geno}SnpLoc":
            Snp_Loc = {}
            Snp_Loc = Snp_id.copy()
            Snp_Loc.update(child.attrib)
            data.append(Snp_Loc)
df = pd.DataFrame.from_dict(data)
df.to_csv("chrY.csv")
tm_end = datetime.now() - tm_start
print("Process done in: ", tm_end)
"""
