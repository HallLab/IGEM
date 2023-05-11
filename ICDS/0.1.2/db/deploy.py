import os
import sys

import pandas as pd
from django.conf import settings

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import (
        Connector,
        Datasource,
        DSTColumn,
        PrefixOpc,
        Term,
        TermCategory,
        TermGroup,
        TermMap,
        WordTerm,
    )
    from omics.models import snpgene
except Exception as e:
    print(e)
    raise


def backup(
    table: str = "",
    path_out: str = "",
) -> bool:
    """
    Backup the database with the internal keys. It can be performed at once
    for all GE.db tables

    Parameters
    ----------
    - table: str
        (datasource, connector, dst, term_group, term_category, term,
        prefix,  wordterm, termmap, wordmap, workflow, logs)
    - path_out: str
        Folder path to store the generated backup files

    If inform table="all", the function will backup all table on GE database.

    Return
    ------
    Boolean: (TRUE if the process occurred without errors and FALSE if had
    some errors).

    Examples
    --------
    >>> import igem
    >>> igem.ge.db.backup(
        table="",
        path_out="/root/back")

    """

    if table == "":
        table = "all"
    v_table = table.lower()

    if path_out == "":
        print("inform path")
    elif not os.path.isdir(path_out):
        raise KeyError
    else:
        v_path_out = path_out
    print(v_path_out)

    if v_table == "datasource" or v_table == "all":
        v_fields = ["id", "datasource", "description", "category", "website"]
        qs = Datasource.objects.all().values_list(*v_fields).order_by("id")
        df = pd.DataFrame(list(qs), columns=v_fields)
        v_file = v_path_out + "/" + "datasource.csv"
        df.to_csv(v_file, index=False)

    if v_table == "connector" or v_table == "all":
        v_fields = [
            "id",
            "connector",
            "description",
            "update_ds",
            "source_path",
            "source_web",
            "source_compact",
            "source_file_name",
            "source_file_format",
            "source_file_sep",
            "source_file_skiprow",
            "target_file_name",
            "target_file_format",
            "target_file_keep",
            "datasource_id",
        ]
        qs = Connector.objects.all().values_list(*v_fields).order_by("id")
        df = pd.DataFrame(list(qs), columns=v_fields)
        df = df.replace(["FALSE"], "False")
        df = df.replace(["True"], "True")
        v_file = v_path_out + "/" + "connector.csv"
        df.to_csv(v_file, index=False)

    if v_table == "prefixopc" or v_table == "all":
        v_fields = ["pre_value"]
        qs = PrefixOpc.objects.all().values_list(*v_fields)
        df = pd.DataFrame(list(qs), columns=v_fields)
        v_file = v_path_out + "/" + "prefixopc.csv"
        df.to_csv(v_file, index=False)

    if v_table == "dstcolumn" or v_table == "all":
        v_fields = [
            "id",
            "status",
            "column_name",
            "single_word",
            "connector_id",
            "pre_value_id",
        ]
        qs = DSTColumn.objects.all().values_list(*v_fields).order_by("id")
        df = pd.DataFrame(list(qs), columns=v_fields)
        df = df.replace(["FALSE"], "False")
        df = df.replace(["True"], "True")
        v_file = v_path_out + "/" + "dstcolumn.csv"
        df.to_csv(v_file, index=False)

    if v_table == "termgroup" or v_table == "all":
        v_fields = [
            "id",
            "term_group",
            "description",
        ]
        qs = TermGroup.objects.all().values_list(*v_fields).order_by("id")
        df = pd.DataFrame(list(qs), columns=v_fields)
        v_file = v_path_out + "/" + "termgroup.csv"
        df.to_csv(v_file, index=False)

    if v_table == "termcategory" or v_table == "all":
        v_fields = [
            "id",
            "term_category",
            "description",
        ]
        qs = TermCategory.objects.all().values_list(*v_fields).order_by("id")
        df = pd.DataFrame(list(qs), columns=v_fields)
        v_file = v_path_out + "/" + "termcategory.csv"
        df.to_csv(v_file, index=False)

    if v_table == "term" or v_table == "all":
        v_fields = [
            "id",
            "term",
            "description",
            "term_category_id",
            "term_group_id",
        ]
        qs = Term.objects.all().values_list(*v_fields).order_by("id")
        df = pd.DataFrame(list(qs), columns=v_fields)
        v_file = v_path_out + "/" + "term.csv"
        df.to_csv(v_file, index=False)

    if v_table == "wordterm" or v_table == "all":
        v_fields = [
            "word",
            "status",
            "commute",
            "term_id",
        ]
        qs = WordTerm.objects.all().values_list(*v_fields)
        df = pd.DataFrame(list(qs), columns=v_fields)
        df = df.replace(["FALSE"], "False")
        df = df.replace(["True"], "True")
        v_file = v_path_out + "/" + "wordterm.csv"
        df.to_csv(v_file, index=False)

    if v_table == "termmap" or v_table == "all":
        v_fields = [
            "ckey",
            "qtd_links",
            "connector_id",
            "term_1_id",
            "term_2_id",
        ]
        qs = TermMap.objects.all().values_list(*v_fields)
        df = pd.DataFrame(list(qs), columns=v_fields)
        v_file = v_path_out + "/" + "termmap.csv"
        df.to_csv(v_file, index=False)

    if v_table == "snpgene" or v_table == "all":
        v_fields = [
            "id",
            "rsid",
            "observed",
            "genomicassembly",
            "chrom",
            "start",
            "end",
            "loctype",
            "rsorienttochrom",
            "contigallele",
            "contig",
            "geneid",
            "genesymbol",
        ]
        qs = snpgene.objects.all().values_list(*v_fields)
        df = pd.DataFrame(list(qs), columns=v_fields)
        df = df.replace(["FALSE"], "False")
        df = df.replace(["True"], "True")
        v_file = v_path_out + "/" + "snpgene.csv"
        df.to_csv(v_file, index=False)

    return True


def restore(
    table: str = "",
    path_out: str = "",
) -> bool:
    """
    Restore the database with the internal keys. It can be performed at once
    for all GE.db tables

    Parameters
    ----------
    - table: str
        (datasource, connector, dst, term_group, term_category, term,
        prefix,  wordterm, termmap, wordmap, workflow, logs)
    - path_out: str
        Folder path to store the generated backup files

    If inform table="all", the function will restore all table on GE database.

    Return
    ------
    Boolean: (TRUE if the process occurred without errors and FALSE if had
    some errors).

    Examples
    --------
    >>> import igem
    >>> igem.ge.db.restore(
        table="",
        path_out="/root/back")

    """

    if table == "":
        table = "all"
    v_table = table.lower()

    if path_out == "":
        print("inform path")
    elif not os.path.isdir(path_out):
        raise KeyError
    else:
        v_path_out = path_out
    print(v_path_out)

    # if not os.path.isfile(v_path):
    #         print("file not found")
    #         print("inform the path and the file in CSV format to load")

    if v_table == "datasource" or v_table == "all":
        try:
            df_src = pd.read_csv(v_path_out + "/datasource.csv")
            # df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        model_instances = [
            Datasource(
                id=record.id,
                datasource=record.datasource,
                description=record.description,
                category=record.category,
                website=record.website,
            )
            for record in df_src.itertuples()
        ]
        Datasource.objects.bulk_create(model_instances, ignore_conflicts=True)

    elif v_table == "connector" or v_table == "all":
        try:
            df_src = pd.read_csv(v_path_out + "/connector.csv")
            # df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
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
        Connector.objects.bulk_create(model_instances, ignore_conflicts=True)

    if v_table == "prefixopc" or v_table == "all":
        try:
            df_src = pd.read_csv(v_path_out + "/prefixopc.csv")
            # df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        model_instances = [
            PrefixOpc(
                pre_value=record.pre_value,
            )
            for record in df_src.itertuples()
        ]
        PrefixOpc.objects.bulk_create(model_instances, ignore_conflicts=True)

    if v_table == "dstcolumn" or v_table == "all":
        try:
            df_src = pd.read_csv(v_path_out + "/dstcolumn.csv")
            # df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        model_instances = [
            DSTColumn(
                id=record.id,
                status=record.status,
                column_name=record.column_name,
                single_word=record.single_word,
                connector_id=record.connector_id,
                pre_value_id=record.pre_value_id,
            )
            for record in df_src.itertuples()
        ]
        DSTColumn.objects.bulk_create(model_instances, ignore_conflicts=True)

    if v_table == "termgroup" or v_table == "all":
        try:
            df_src = pd.read_csv(v_path_out + "/termgroup.csv")
            # df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        model_instances = [
            TermGroup(
                id=record.id,
                term_group=record.term_group,
                description=record.description,
            )
            for record in df_src.itertuples()
        ]
        TermGroup.objects.bulk_create(model_instances, ignore_conflicts=True)

    if v_table == "termcategory" or v_table == "all":
        try:
            df_src = pd.read_csv(v_path_out + "/termcategory.csv")
            # df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        model_instances = [
            TermCategory(
                id=record.id,
                term_category=record.term_category,
                description=record.description,
            )
            for record in df_src.itertuples()
        ]
        TermCategory.objects.bulk_create(
            model_instances, ignore_conflicts=True
        )  # noqa E501

    if v_table == "term" or v_table == "all":
        try:
            df_src = pd.read_csv(v_path_out + "/term.csv")
            # df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
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
        Term.objects.bulk_create(model_instances, ignore_conflicts=True)

    if v_table == "wordterm" or v_table == "all":
        try:
            df_src = pd.read_csv(v_path_out + "/wordterm.csv")
            # df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        model_instances = [
            WordTerm(
                word=record.word,
                status=record.status,
                commute=record.commute,
                term_id=record.term_id,
            )
            for record in df_src.itertuples()
        ]
        WordTerm.objects.bulk_create(model_instances, ignore_conflicts=True)

    if v_table == "termmap" or v_table == "all":
        try:
            df_src = pd.read_csv(v_path_out + "/termmap.csv")
            # df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
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
        TermMap.objects.bulk_create(model_instances, ignore_conflicts=True)

    if v_table == "snpgene" or v_table == "all":
        try:
            df_src = pd.read_csv(v_path_out + "/snpgene.csv")
            # df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
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
        snpgene.objects.bulk_create(model_instances, ignore_conflicts=True)

    return True
