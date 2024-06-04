# TODO: Integrar a rotina para baixar o arquivo do NCBI
# TODO: Adicionar processo de descompactacao do XML

import os
import sys

import pandas as pd
from django.conf import settings
from django.db.models import Q

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from omics.models import snpgene
except Exception as e:
    print(e)
    raise


def load_data(chunk=100000, table=None, path=None) -> bool:
    """
    Loads data from a CSV file into the OMICS database. This process does
    not update existing data, it only inserts new records.

    Parameters
    ----------
    - table: str
        snp
    - path: str
        full path and file name to load

    Layout of data file
    -------------------
    - SNP:
        (rsId, observed, genomicAssembly, chrom, start, end, locType,
        rsOrientToChrom, contigAllele, contig, geneId, geneSymbol)

    We can generate an example file with the get_data() function and
    manipulate and load it with the new data.

    Return
    ------
    Boolean: (TRUE if the process occurred without errors and FALSE if had
    some errors).

    Examples
    --------
    >>> from igem.omics import db
    >>> db.load_data(
            table="snp
            path=”{your_path}/chrom_22.csv”
            )
    """

    v_chunk = chunk

    v_table = table.lower()
    v_path = path.lower()

    if v_path is None:
        print("  Inform the path to load")
        return False
    if not os.path.isfile(v_path):
        print("  File not found")
        print("  Inform the path and the file in CSV format to load")
        return False

    if v_table == "snp":
        v_index = 0
        try:
            for DFR in pd.read_csv(
                v_path, dtype=str, index_col=False, chunksize=v_chunk
            ):
                DFR = DFR.apply(lambda x: x.astype(str).str.lower())
                model_instances = [
                    snpgene(
                        rsid=record.rsId,
                        observed="",  # record.observed,
                        genomicassembly=record.genomicAssembly,
                        chrom=record.chrom,
                        start=record.start,
                        end=record.end,
                        loctype="",  # record.locType,
                        rsorienttochrom="",  # record.rsOrientToChrom,
                        contigallele="",  # record.contigAllele,
                        contig=record.contig,
                        geneid=record.geneId,
                        genesymbol=record.geneSymbol,
                    )
                    for record in DFR.itertuples()
                ]

                snpgene.objects.bulk_create(
                    model_instances, ignore_conflicts=True
                )  # noqa E501
                print("  Load with success to SNP-Gene table ")
                v_index += 1
                print("    Cicle:", v_index, "/ Rows:", len(DFR.index))

        except IOError as e:
            print("ERRO:")
            print(e)
            return False

    else:
        print(
            "Table not recognized in the system. Choose one of the options: "
        )  # noqa E501
        print("   snp-gene ")

    return True


def statistic(table="snp", chrom=[]):
    """
    Return summary on the table.

    Parameters
    ----------
    - table: str
        snp
    - chrom: str
        chromossom id

    Layout of data file
    -------------------
    - Datasource:
        (datasource, description, category, website)
    - Connector:
        (connector, datasource, description, update_ds, source_path,
        source_web, source_compact, source_file_name, source_file_format,
        source_file_sep, source_file_skiprow, target_file_name,
        target_file_format)
    - Ds_column:
        (connector, status, column_number, column_name, pre_value, single_word)
    - Term_group:
        (term_group, description)
    - Term_category:
        (term_category, description)
    - Term:
        (term, category, group, description)
    - Prefix:
        (pre_value)
    - Wordterm:
        (term, word, status, commute)
    - Termmap:
        (ckey, connector, term_1, term_2, qtd_links)
    - Wordmap:
        (cword, datasource, connector, term_1, term_2, word_1, word_2,
        qtd_links)

    We can generate an example file with the get_data() function and
    manipulate and load it with the new data.

    Return
    ------
    Boolean: (TRUE if the process occurred without errors and FALSE if had
    some errors).

    Examples
    --------
    >>> from igem.omics import db
    >>> db.statistic(
            table="snp"
            chrom=['22','2','x']
            )

    """

    v_table = table.lower()
    if v_table == "snp":
        v_chrom = chrom
        if not v_chrom:
            v_chrom = [
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "10",
                "11",
                "12",
                "13",
                "14",
                "15",
                "16",
                "17",
                "18",
                "19",
                "20",
                "21",
                "22",
                "x",
                "y",
            ]

        for chrm in v_chrom:
            print("Chromosome", chrm)
            print(
                "   SNPs TOTAL:        ",
                snpgene.objects.filter(chrom=chrm).count(),  # noqa E501
            )  # noqa E501
            print(
                "   SNPs without Genes:",
                snpgene.objects.filter(geneid="nan", chrom=chrm).count(),
            )
            print(
                "   SNPs with Genes:   ",
                snpgene.objects.filter(~Q(geneid="nan"), chrom=chrm).count(),
            )
            print(
                "   Number of Genes:   ",
                snpgene.objects.filter(~Q(geneid="nan"), chrom=chrm)
                .values("geneid")
                .distinct()
                .count(),
            )
