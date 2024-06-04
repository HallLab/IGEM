import os
import sys

import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Sum

from .tag import create_tag
from .utils import read_parameters

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import TermMap, WordMap
    from omics.models import snpgene
except Exception as e:
    print(e)
    raise


def _query_map(query, kwargs):
    (
        v_status,
        v_path_out,
        query_datasource,
        query_connector,
        query_term_group,
        query_term_category,
        query_term,
        query_word,
        v_aggr,
    ) = read_parameters(kwargs)
    if not v_status:
        return v_status, "", "", ""
    try:
        # df_query = pd.DataFrame(TermMap.objects.filter(**v_filter). \
        if query == "term":
            v_aggr.remove("word_1")
            v_aggr.remove("word_2")
            df_query = pd.DataFrame(
                TermMap.objects.filter(
                    query_datasource,
                    query_connector,
                    query_term_group,
                    query_term_category,
                    query_term,
                )
                .values(*v_aggr)
                .annotate(qtd_links=Sum("qtd_links"))
            )
        elif query == "word":
            df_query = pd.DataFrame(
                WordMap.objects.filter(
                    query_datasource,
                    query_connector,
                    query_term_group,
                    query_term_category,
                    query_term,
                    query_word,
                )
                .values(*v_aggr)
                .annotate(qtd_links=Sum("qtd_links"))
            )

        # Create a TAG
        # TAG = Current WFControl ID for each Connector in this Report, sep by "-"  # noqa E501
        if not df_query.empty:
            df_conn = df_query[["connector_id__connector"]].copy()
            df_conn.drop_duplicates(inplace=True)
            ls_conn = df_conn.values.tolist()
            ls_conn = [item for sublist in ls_conn for item in sublist]
            v_tag = create_tag(connectors=ls_conn)

    except ObjectDoesNotExist:
        print("GE.db query error")
        return False, "", "", ""
    if df_query.empty:
        print("  No data found with the given parameters")
        return False, "", "", ""
    df_query.rename(
        columns={
            "connector_id__datasource__datasource": "datasource",
            "connector_id__connector": "connector",
            "term_1__term_group_id__term_group": "term_group_1",
            "term_2__term_group_id__term_group": "term_group_2",
            "term_1__term_category_id__term_category": "term_category_1",
            "term_2__term_category_id__term_category": "term_category_2",
            "term_1__term": "term_1",
            "term_2__term": "term_2",
            "term_1__description": "description_1",
            "term_2__description": "description_2",
        },
        inplace=True,
    )
    df_query = df_query.reindex(
        columns=[
            "datasource",
            "connector",
            "term_group_1",
            "term_category_1",
            "term_1",
            "word_1",
            "description_1",
            "term_group_2",
            "term_category_2",
            "term_2",
            "word_2",
            "description_2",
            "qtd_links",
        ]
    )

    return True, v_path_out, df_query, v_tag


def _gene_exposome_layout(df_query):
    # Identify the Terms Genes and Terms Exposomes
    df_query.sort_values(
        ["term_1", "term_2"],
        ascending=[True, True],
        inplace=True,
    )
    df_gen_exp = pd.DataFrame(
        columns=[
            "gene_id",
            "gene_desc",
            "exp_grp",
            "exp_cat",
            "exp_id",
            "exp_desc",
            "qtd_links",
            "sources",
            "connector",
        ]
    )
    v_chk_key = ""
    v_chk_index = ""
    v_chk_source = ""
    v_chk_qtd = ""
    v_chk_qtd = 0

    for index, row in df_query.iterrows():
        # adjust field position
        if (
            row["term_category_1"] == "gene"
            and row["term_group_2"] == "environment"  # noqa E501
        ):  # noqa E501
            v_gene_id = row["term_1"]
            v_gene_desc = row["description_1"]
            v_exp_grp = row["term_group_2"]
            v_exp_cat = row["term_category_2"]
            v_exp_id = row["term_2"]
            v_exp_desc = row["description_2"]
        elif (
            row["term_category_2"] == "gene"
            and row["term_group_1"] == "environment"  # noqa E501
        ):  # noqa E501
            v_gene_id = row["term_2"]
            v_gene_desc = row["description_2"]
            v_exp_grp = row["term_group_1"]
            v_exp_cat = row["term_category_1"]
            v_exp_id = row["term_1"]
            v_exp_desc = row["description_1"]
        else:
            continue  # add only Gene Category x Environment Group
        v_chk = v_gene_id + "-" + v_exp_id
        # Check and process new and repeated records
        if v_chk != v_chk_key:  # NEW
            v_chk_source = str(row["connector"])
            v_chk_qtd = 1
            v_chk_qtd_links = row["qtd_links"]
            v_chk_index = index
        elif v_chk == v_chk_key:  # REPEATED
            v_chk_source = v_chk_source + "," + str(row["connector"])
            v_chk_qtd += 1
            v_chk_qtd_links = v_chk_qtd_links + row["qtd_links"]
            v_chk_index = v_chk_index
        else:
            print(" ERROR ON CHECK  ")
        df_gen_exp.loc[v_chk_index] = [
            v_gene_id,
            v_gene_desc,
            v_exp_grp,
            v_exp_cat,
            v_exp_id,
            v_exp_desc,
            v_chk_qtd_links,
            v_chk_qtd,
            v_chk_source,
        ]
        v_chk_key = v_chk
    df_gen_exp.sort_values(
        ["gene_id", "exp_id"], ascending=[True, True], inplace=True
    )  # noqa E501

    return df_gen_exp


# Return dataframe or boolean on TermMap query
def term_map(*args, **kwargs):
    """
    TermMap table query function.

    Parameters
    ----------
    - path_in: str
        parameter file path with filter information, aggregation and result
        file path.
    - path_out: str
        result file path
    - term: list[str]
        List of terms to filter passed through the function. If you inform the
        file with the parameters, the values passed by this parameter will be
        disregarded.


    Return
    ------
    it may return a boolean value if you have informed an output per file
    (path_out) or a DataFrame if you have not informed an output file.

    Examples:
    --------
    >>> from igem.ge import filter
    >>> filter.term_map(
            path_in="../../file.csv",
            path_out="../../outcome.csv"
            )
    >>> df_result = filter.term_map(
            term=["gene:246126"]
    """

    v_status, v_path_out, df_query, v_tag = _query_map(
        query="term",
        kwargs=kwargs,
    )
    if not v_status:
        return False, ""
    if v_path_out:
        df_query.to_csv(v_path_out, index=False)
        print(
            "GE.filter run successfully and returned", len(df_query), "relationships"  # noqa E501
            )
        print("Records have been saved to file: %s" % str(v_path_out))
        print("Your GE.db Version Tag is:", v_tag)
        return True, v_tag
    else:
        print(
            "GE.filter run successfully and returned", len(df_query), "relationships"  # noqa E501
            )
        print("Your GE.db Version Tag is:", v_tag)
        return df_query


# Return dataframe or boolean on TermMap query in Gene x Exposome layout
def gene_exposome(*args, **kwargs):
    """
    Queries GE.db and returns links between genes and exposomes based on input
    parameters or the parameter file.

    Parameters
    ----------
    - path_in: str
        parameter file path with filter information, aggregation and result
        file path.
    - path_out: str
        result file path
    - term: list[str]
        List of terms to filter passed through the function. If you inform the
        file with the parameters, the values passed by this parameter will be
        disregarded.


    Return
    ------
    it may return a boolean value if you have informed an output per file
    (path_out) or a DataFrame if you have not informed an output file.

    Examples:
    ---------
    >>> from igem.ge import filter
    >>> filter.gene_exposome(
            path_in="../../file.csv",
            path_out="../../outcome.csv"
            )
    >>> df_result = filter.gene_exposome(
            term=["gene:246126"]
    """

    v_status, v_path_out, df_query, v_tag = _query_map(
        query="term",
        kwargs=kwargs,
    )
    if not v_status:
        return False, ""
    df_gen_exp = _gene_exposome_layout(df_query)

    if v_path_out:
        df_gen_exp.to_csv(v_path_out, index=False)
        print(
            "GE.filter run successfully and returned", len(df_gen_exp), "relationships"  # noqa E501
            )
        print("Records have been saved to file: %s" % str(v_path_out))
        print("Your GE.db Version Tag is:", v_tag)
        return True, v_tag
    else:
        print(
            "GE.filter run successfully and returned", len(df_gen_exp), "relationships"  # noqa E501
            )
        print("Your GE.db Version Tag is:", v_tag)
        return df_gen_exp


def snp_exposome(*args, **kwargs):
    """
    Queries GE.db and returns links between snp and exposomes based on input
    parameters or the parameter file.

    Parameters
    ----------
    - path_in: str
        parameter file path with filter information, aggregation and result
        file path.
    - path_out: str
        result file path
    - term: list[str]
        List of terms to filter passed through the function. If you inform the
        file with the parameters, the values passed by this parameter will be
        disregarded.


    Return
    ------
    it may return a boolean value if you have informed an output per file
    (path_out) or a DataFrame if you have not informed an output file.

    Examples:
    --------
    >>> from igem.ge import filter
    >>> filter.snp_exposome(
            path_in="../../file.csv",
            path_out="../../outcome.csv"
            )
    >>> df_result = filter.snp_exposome(
            term=["gene:246126"]
    """

    v_status, v_path_out, df_query, v_tag = _query_map(
        query="term",
        kwargs=kwargs,
    )
    if not v_status:
        return False, ""
    df_gen_exp = _gene_exposome_layout(df_query)
    df_query_gene = df_gen_exp["gene_id"]
    df_query_gene.drop_duplicates(inplace=True)
    df_query_gene = df_query_gene.apply(
        lambda x: str(x).strip("gene:")
    )  # Remove gene: prefix from values to match on ncbi_snpgene
    list_gene = df_query_gene.to_list()
    try:
        df_query_snp = pd.DataFrame(
            snpgene.objects.filter(geneid__in=list_gene).values(
                "rsid",
                "chrom",
                "start",
                "end",
                "contig",
                "geneid",
                "genesymbol",
            )
        )
    except ObjectDoesNotExist:
        print("GE.db query error")
        return False, ""
    if df_query_snp.empty:
        print("No data found with the given parameters")
        return False, ""
    df_query_snp["gene_id"] = str("gene:") + df_query_snp["geneid"]
    df_query = pd.merge(df_gen_exp, df_query_snp, how="left", on="gene_id")
    df_query = df_query.reindex(
        columns=[
            "gene_id",
            "gene_desc",
            "genesymbol",
            "rsid",
            "chrom",
            "contig",
            "start",
            "end",
            "exp_cat",
            "exp_id",
            "exp_desc",
            "qtd_links",
            "sources",
            "connector",
        ]
    )

    if v_path_out:
        df_query.to_csv(v_path_out, index=False)
        print(
            "GE.filter run successfully and returned", len(df_query), "relationships"  # noqa E501
            )
        print("Records have been saved to file: %s" % str(v_path_out))
        print("Your GE.db Version Tag is:", v_tag)
        return True, v_tag
    else:
        print(
            "GE.filter run successfully and returned", len(df_query), "relationships"  # noqa E501
            )
        print("Your GE.db Version Tag is:", v_tag)
        return df_query


def word_map(*args, **kwargs):
    """
    Queries GE.db and returns links between words without terms.

    Parameters
    ----------
    - path_in: str
        parameter file path with filter information, aggregation and result
        file path.
    - path_out: str
        result file path
    - term: list[str]
        List of terms to filter passed through the function. If you inform the
        file with the parameters, the values passed by this parameter will be
        disregarded.

    Return
    ------
    it may return a boolean value if you have informed an output per file
    (path_out) or a DataFrame if you have not informed an output file.

    Examples:
    --------
    >>> from igem.ge import filter
    >>> filter.word_map(
            path_in="../../file.csv",
            path_out="../../outcome.csv"
            )
    """

    v_status, v_path_out, df_query = _query_map(
        query="word",
        kwargs=kwargs,
    )
    if not v_status:
        return False
    if v_path_out:
        df_query.to_csv(v_path_out, index=False)
        print("Results sucessfully created in %s" % str(v_path_out))
        return True
    else:
        return df_query


# PARAMETERS
def parameters_file(path=None):
    """
    generates a model file to be used as a parameter file in query functions

    Parameters
    ----------
    - path: str
        path where the file will be generated.

    File layout
    -----------
    In the file structure, new lines for the index filter can be included with
    additional values, and each filter line must contain only a single value.
    The output index and path must be unique, as they will be applied to the
    entire corresponding field (parameter).

    In the example below, let's select all terms from two data sources from a
    single group. Also, the Datasource and Connector fields will be aggregated
    and will not appear on the results

    {parameter file layout}
    - index,parameter,value
    - filter,datasource,ds_01
    - filter,datasource,ds_02
    - filter,connector,
    - filter,term_group,Chemical
    - filter,term_category,
    - filter,word,
    - output,datasource,no
    - output,connector,no
    - output,term_group,
    - output,term_category,
    - output,term,
    - output,word,no
    - path,path,/../output_file.csv

    Return
    ------
    it return a boolean value if the file was created

    Examples:
    --------
    >>> from igem.ge import filter
    >>> filter.parameters_file(
            path="../../folder"
            )
    """

    v_path_in = path.lower()
    if not os.path.isdir(v_path_in):
        print("  Output path not found")
        sys.exit(2)
    else:
        v_path_out = v_path_in + "/filter_parameters.csv"
    v_index = [
        "filter",
        "filter",
        "filter",
        "filter",
        "filter",
        "filter",
        "output",
        "output",
        "output",
        "output",
        "output",
        "output",
        "path",
    ]
    v_parameter = [
        "datasource",
        "connector",
        "group",
        "category",
        "term",
        "word",
        "datasource",
        "connector",
        "group",
        "category",
        "term",
        "word",
        "path",
    ]
    v_value = [
        "*",
        "*",
        "*",
        "*",
        "*",
        "*",
        "*",
        "*",
        "*",
        "*",
        "*",
        "*",
        "/../output_result_file.csv",
    ]
    v_list = list(zip(v_index, v_parameter, v_value))
    df_parameters = pd.DataFrame(
        v_list,
        columns=["index", "parameter", "value"],
    )
    df_parameters.to_csv(v_path_out, index=False)
    print("File template with parameters created in {0}".format(v_path_out))
    return True


# def gedb_tag(tag=None, bkp=False):
#     # vou receber a tg e gerar um relatorio do WFControl
#     # GE.db-TAG:10-8

#     # check first
#     if tag is None:
#         print("inform one tag")
#         return ''
#     # Split the string into two parts using ":" as the separator

#     ls_parts = tag.split(":")

#     v_head = ls_parts[0]
#     if v_head != 'GE.db-TAG':
#         print('head is different')

#     # Split the second part into two values using "-" as the separator
#     values = ls_parts[1]
#     value_list = values.split("-")

#     qs_wfc = pd.DataFrame(
#         WFControl.objects.filter(
#             pk__in=value_list
#             ).values(
#                 "id",
#                 "connector_id",
#                 "connector_id__connector",
#                 "last_update",
#                 "igem_version",
#                 "row_collect",
#                 "row_reduce",
#                 "source_file_version",
#                 )
#         )

#     if bkp:
#         # Get Connectors ID from WFControl
#         df_conn = qs_wfc[['connector_id']].copy()
#         df_conn.drop_duplicates(inplace=True)
#         ls_conn = df_conn.values.tolist()
#         ls_conn = [item for sublist in ls_conn for item in sublist]

#         base = pd.DataFrame(TermMap.objects.filter(
#             connector_id__in=ls_conn).values(
#                 'ckey',
#                 'connector_id',
#                 'term_1_id',
#                 'term_2_id',
#                 'qtd_links',
#             ))
#         base.to_csv('mydata.csv', index=False)
#         # base.to_csv('mydata.csv.gz', index=False, compression='gzip')

#     return qs_wfc
