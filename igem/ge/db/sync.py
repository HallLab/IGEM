import os
import sys

import pandas as pd
import requests
from django.conf import settings
from django.db import transaction

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import (  # WordTerm,
        Connector,
        Datasource,
        DSTColumn,
        PrefixOpc,
        Term,
        TermCategory,
        TermGroup,
        TermMap,
        WFControl,
        WordTerm,
    )
    from omics.models import snpgene
except Exception as e:
    print(e)
    raise


# Function to read source data
def read_data(source, file):
    if source.startswith("http"):
        file_url = source.rstrip("/") + "/" + file
        try:
            print(f"Lodding {file_url}")
            df_src = pd.read_csv(file_url, compression='gzip')
        except (requests.RequestException, pd.errors.ParserError) as e:
            raise ValueError(
                f"Failed to read data from the GitHub folder. {e}"
                )
    elif os.path.isdir(source):
        file_path = os.path.join(source, file)
        try:
            print(f"lodding {file_path}")
            df_src = pd.read_csv(file_path, compression='gzip')
        except IOError as e:
            raise ValueError(
                f"Failed to read data from the local directory. {e}"
                )
    else:
        raise ValueError("Invalid data source provided")
    return df_src


# Sync Function from IGEM Server to Client
def sync_db(
    table: str = "all",
    source: str = "https://raw.githubusercontent.com/HallLab/IGEM_Support/main/db/current", # noqa E501
) -> bool:
    """
    Perform a data synchronization between the IGEM server and the IGEM

    Parameters
    ----------
    - table: str
        (datasource, connector, dstcolumn, termgroup, termcategory, term,
        prefixopc, termmap, wfcontrol, snpgene)
    - source: str
        Folder path to store the generated backup files
        default: https://github.com/HallLab/IGEM_Support/tree/main/db/current

    If inform table="all", the function will synch all table on GE database.

    Return
    ------
    Boolean: (TRUE if the process occurred without errors and FALSE if had
    some errors).

    Examples
    --------
    >>> import igem
    >>> igem.ge.db.sync_db(
            table="",
            source="/root/back/")
    """

    v_table = table.lower()

    # Run only arg is in table_list
    table_list = [
        "all",
        "datasource",
        "connector",
        "dstcolumn",
        "termgroup",
        "termcategory",
        "term",
        "prefixopc",
        "termmap",
        "wfcontrol",
        "snpgene",
        "wordterm",
        ]
    if v_table not in table_list:
        return False

    # GE APP TABLES
    # DATASOURCE
    if v_table == "datasource" or v_table == "all":
        df_src = read_data(source, "datasource.csv.gz")
        model_instances = [
            Datasource(
                id=record.id,
                datasource=record.datasource,
                description=record.description,
                category=record.category,
                website=record.website,
            )
            for record in df_src.itertuples(index=False)
        ]
        # Truncate table and load new data in a transaction
        with transaction.atomic():
            Datasource.objects.all().delete()
            Datasource.objects.bulk_create(model_instances, batch_size=1000)
        print(f"DataSource data updated with {len(df_src)} records")

    # CONNECTOR
    if v_table == "connector" or v_table == "all":
        df_src = read_data(source, "connector.csv.gz")
        model_instances = [
            Connector(
                id=record.id,
                connector=record.connector,
                description=record.description,
                update_ds=record.update_ds,
                source_path=record.source_path,
                source_web=record.source_web,
                source_compact=record.source_compact,
                source_file_name=record.source_file_name,
                source_file_format=record.source_file_format,
                source_file_sep=record.source_file_sep,
                source_file_skiprow=record.source_file_skiprow,
                target_file_name=record.target_file_name,
                target_file_format=record.target_file_format,
                target_file_keep=record.target_file_keep,
                datasource_id=record.datasource_id,
            )
            for record in df_src.itertuples()
        ]
        with transaction.atomic():
            Connector.objects.all().delete()
            Connector.objects.bulk_create(model_instances, batch_size=1000)
        print(f"Connector data updated with {len(df_src)} records")

    # TERMS PREFIX
    if v_table == "prefixopc" or v_table == "all":
        df_src = read_data(source, "prefixopc.csv.gz")
        model_instances = [
            PrefixOpc(
                pre_value=record.pre_value,
            )
            for record in df_src.itertuples()
        ]
        with transaction.atomic():
            PrefixOpc.objects.all().delete()
            PrefixOpc.objects.bulk_create(
                model_instances,
                batch_size=1000,
                ignore_conflicts=True)
        print(f"Prefix data updated with {len(df_src)} records")

    # COLUMNS SETUP TRANSFORMATION BY CONNECTOR
    if v_table == "dstcolumn" or v_table == "all":
        df_src = read_data(source, "dstcolumn.csv.gz")
        model_instances = [
            DSTColumn(
                id=record.id,
                status=record.status,
                column_name=record.column_name,
                column_number=record.column_number,
                single_word=record.single_word,
                connector_id=record.connector_id,
                pre_value_id=record.pre_value_id,
            )
            for record in df_src.itertuples()
        ]
        with transaction.atomic():
            DSTColumn.objects.all().delete()
            DSTColumn.objects.bulk_create(
                model_instances,
                batch_size=1000,
                ignore_conflicts=True)
        print(f"Connector Rules data updated with {len(df_src)} records")

    # GROUP OF TERMS
    if v_table == "termgroup" or v_table == "all":
        df_src = read_data(source, "termgroup.csv.gz")
        model_instances = [
            TermGroup(
                id=record.id,
                term_group=record.term_group,
                description=record.description,
            )
            for record in df_src.itertuples()
        ]
        with transaction.atomic():
            TermGroup.objects.all().delete()
            TermGroup.objects.bulk_create(
                model_instances,
                batch_size=1000,
                ignore_conflicts=True)
        print(f"Group Term data updated with {len(df_src)} records")

    # CATEGORY OF TERMS
    if v_table == "termcategory" or v_table == "all":
        df_src = read_data(source, "termcategory.csv.gz")
        model_instances = [
            TermCategory(
                id=record.id,
                term_category=record.term_category,
                description=record.description,
            )
            for record in df_src.itertuples()
        ]
        with transaction.atomic():
            TermCategory.objects.all().delete()
            TermCategory.objects.bulk_create(
                model_instances,
                batch_size=1000,
                ignore_conflicts=True)
        print(f"Category Term data updated with {len(df_src)} records")

    # TERMS
    if v_table == "term" or v_table == "all":
        df_src = read_data(source, "term.csv.gz")
        model_instances = [
            Term(
                id=record.id,
                term=record.term,
                description=record.description,
                term_category_id=record.term_category_id,
                term_group_id=record.term_group_id,
            )
            for record in df_src.itertuples()
        ]
        with transaction.atomic():
            Term.objects.all().delete()
            Term.objects.bulk_create(
                model_instances,
                batch_size=1000,
                ignore_conflicts=True)
        print(f"Term data updated with {len(df_src)} records")

    # WORD - TERMS (TURN OFF TO IGEM CLIENT)
    if v_table == "wordterm" or v_table == "all":
        df_src = read_data(source, "wordterm.csv.gz")
        model_instances = [
            WordTerm(
                word=record.word,
                status=record.status,
                commute=record.commute,
                term_id=record.term_id,
            )
            for record in df_src.itertuples()
        ]
        with transaction.atomic():
            WordTerm.objects.all().delete()
            WordTerm.objects.bulk_create(
                model_instances,
                batch_size=1000,
                ignore_conflicts=True)
        print(f"WordTerm data updated with {len(df_src)} records")

    # TERMS - MAP
    if v_table == "termmap" or v_table == "all":
        df_src = read_data(source, "termmap.csv.gz")
        model_instances = [
            TermMap(
                ckey=record.ckey,
                qtd_links=record.qtd_links,
                connector_id=record.connector_id,
                term_1_id=record.term_1_id,
                term_2_id=record.term_2_id,
            )
            for record in df_src.itertuples()
        ]
        with transaction.atomic():
            TermMap.objects.all().delete()
            TermMap.objects.bulk_create(
                model_instances,
                batch_size=1000,
                ignore_conflicts=True)
        print(f"Term Mapping data updated with {len(df_src)} records")

    # WORKFLOW CONTROL
    if v_table == "wfcontrol" or v_table == "all":
        df_src = read_data(source, "wfcontrol.csv.gz")
        model_instances = [
            WFControl(
                id=record.id,
                connector_id=record.connector_id,
                last_update=record.last_update,
                source_file_version=record.source_file_version,
                source_file_size=record.source_file_size,
                target_file_size=record.target_file_size,
                chk_collect=record.chk_collect,
                chk_prepare=record.chk_prepare,
                chk_map=record.chk_map,
                chk_reduce=record.chk_reduce,
                igem_version=record.igem_version,
                status=record.status,
                time_collect=record.time_collect,
                time_prepare=record.time_prepare,
                time_map=record.time_map,
                time_reduce=record.time_reduce,
                row_collect=record.row_collect,
                row_prepare=record.row_prepare,
                row_map=record.row_map,
                row_reduce=record.row_reduce,
            )
            for record in df_src.itertuples()
        ]
        with transaction.atomic():
            WFControl.objects.all().delete()
            WFControl.objects.bulk_create(
                model_instances,
                batch_size=1000,
                ignore_conflicts=True)
        print(f"Workflow Control data updated with {len(df_src)} records")

    # OMIC APP TABLES
    # SNP - GENE
    if v_table == "snpgene" or v_table == "all":
        df_src = read_data(source, "snpgene.csv.gz")
        model_instances = [
            snpgene(
                id=record.id,
                rsid=record.rsid,
                observed=record.observed,
                genomicassembly=record.genomicassembly,
                chrom=record.chrom,
                start=record.start,
                end=record.end,
                loctype=record.loctype,
                rsorienttochrom=record.rsorienttochrom,
                contigallele=record.contigallele,
                contig=record.contig,
                geneid=record.geneid,
                genesymbol=record.genesymbol,
            )
            for record in df_src.itertuples()
        ]
        with transaction.atomic():
            snpgene.objects.all().delete()
            snpgene.objects.bulk_create(
                model_instances,
                batch_size=1000,
                ignore_conflicts=True)
        print(f"Snp-Gene data updated with {len(df_src)} records")

    return True
