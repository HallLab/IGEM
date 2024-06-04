import os

import pandas as pd
from django.db.models import Q


def read_parameters(kwargs):
    v_path = kwargs.get("path_in", "")
    v_path_out = kwargs.get("path_out", "")
    v_datasource = kwargs.get("datasource", [])
    v_connector = kwargs.get("connector", [])
    v_term_group = kwargs.get("term_group", [])
    v_term_category = kwargs.get("term_category", [])
    v_term = kwargs.get("term", [])
    v_word = kwargs.get("word", [])
    v_ck_datasource = kwargs.get("group_by_datasource", True)
    v_ck_connector = kwargs.get("group_by_connector", True)
    v_ck_term_group = kwargs.get("group_by_term_group", True)
    v_ck_term_category = kwargs.get("group_by_term_category", True)
    v_ck_term = kwargs.get("group_by_term", True)
    v_ck_word = kwargs.get("group_by_word", True)

    # read input file
    if v_path:
        v_path_in = v_path.lower()
        # check if is valid file
        if not os.path.isfile(v_path_in):
            print("inform the path and the file in CSV format to load")
            v_status = False
            return v_status
        try:
            df_param_in = pd.read_csv(v_path_in)
            df_param_in = df_param_in.apply(
                lambda x: x.astype(str).str.lower()
            )  # noqa E501
        except IOError as e:
            print("ERRO:", e)
            v_status = False
            return v_status
        for index, row in df_param_in.iterrows():
            if row["index"] == "filter":
                if row["parameter"] == "datasource":
                    if row["value"] == "nan" or row["value"] == "*":
                        pass
                    else:
                        v_datasource.append(row["value"])
                if row["parameter"] == "connector":
                    if row["value"] == "nan" or row["value"] == "*":
                        pass
                    else:
                        v_connector.append(row["value"])
                if row["parameter"] == "term_group":
                    if row["value"] == "nan" or row["value"] == "*":
                        pass
                    else:
                        v_term_group.append(row["value"])
                if row["parameter"] == "term_category":
                    if row["value"] == "nan" or row["value"] == "*":
                        pass
                    else:
                        v_term_category.append(row["value"])
                if row["parameter"] == "term":
                    if row["value"] == "nan" or row["value"] == "*":
                        pass
                    else:
                        v_term.append(row["value"])
                if row["parameter"] == "word":
                    if row["value"] == "nan" or row["value"] == "*":
                        pass
                    else:
                        v_word.append(row["value"])

            # We will always have to bring the connector or datasource
            # to carry out the term_grouping manually
            if row["index"] == "output":
                if row["parameter"] == "datasource":
                    if row["value"] == "no":
                        v_ck_datasource = True
                if row["parameter"] == "connector":
                    if row["value"] == "no":
                        v_ck_connector = True
                if row["parameter"] == "term_group":
                    if row["value"] == "no":
                        v_ck_term_group = True
                if row["parameter"] == "term_category":
                    if row["value"] == "no":
                        v_ck_term_category = True
                if row["parameter"] == "term":
                    if row["value"] == "no":
                        v_ck_term = True
                if row["parameter"] == "word":
                    if row["value"] == "no":
                        v_ck_word = False

            # Overwrite the standard value
            if row["index"] == "path" and v_path_out == "":
                if row["value"]:
                    v_path_out_tmp = row["value"]
                    if not os.path.isdir(os.path.dirname(v_path_out_tmp)):
                        print("Output path not found")
                        print("Inform the path to results download")
                        v_path_out = ""
                    v_path_out = v_path_out_tmp
        if not v_path_out:
            v_path_out = os.path.dirname(v_path_in) + "/filter_results.csv"  # noqa E501

    v_filter = {}
    if v_datasource:
        v_filter["connector_id__datasource__datasource__in"] = v_datasource
    if v_connector:
        v_filter["connector_id__connector__in"] = v_connector
    if v_term_group:
        v_filter["term_1__term_group_id__term_group__in"] = v_term_group
        v_filter["term_2__term_group_id__term_group__in"] = v_term_group
    if v_term_category:
        v_filter[
            "term_1__term_category_id__term_category__in"
        ] = v_term_category  # noqa E501
        v_filter[
            "term_2__term_category_id__term_category__in"
        ] = v_term_category  # noqa E501
    if v_term:
        v_filter["term_1__term__in"] = v_term
        # performance issues, switch to term_id and convert the input
        v_filter["term_2__term__in"] = v_term
        # performance issues, switch to term_id and convert the input
    if v_word:
        v_filter["word_1__in"] = v_word
        v_filter["word_2__in"] = v_word

    v_aggr = []
    if v_ck_datasource:
        v_aggr.append("connector_id__datasource__datasource")
    if v_ck_connector:
        v_aggr.append("connector_id__connector")
    if v_ck_term_group:
        v_aggr.append("term_1__term_group_id__term_group")
        v_aggr.append("term_2__term_group_id__term_group")
    if v_ck_term_category:
        v_aggr.append("term_1__term_category_id__term_category")
        v_aggr.append("term_2__term_category_id__term_category")
    if v_ck_term:
        v_aggr.append("term_1__term")
        v_aggr.append("term_2__term")
        v_aggr.append("term_1__description")
        v_aggr.append("term_2__description")
    if v_ck_word:
        v_aggr.append("word_1")
        v_aggr.append("word_2")

    if v_datasource:
        v_datasource.append("dummy")
        query_datasource = Q(
            connector__datasource__datasource__in=(v_datasource)
        )  # noqa E501
    else:
        query_datasource = Q(connector_id__gt=(0))
    if v_connector:
        v_connector.append("dummy")
        query_connector = Q(connector_id__connector__in=(v_connector))
    else:
        query_connector = Q(connector_id__gt=(0))
    if v_term_group:
        v_term_group.append("dummy")
        query_term_group = Q(
            term_1__term_group_id__term_group__in=(v_term_group)
        )  # noqa E501
        query_term_group.add(
            Q(term_2__term_group_id__term_group__in=(v_term_group)), Q.OR
        )
    else:
        query_term_group = Q(connector_id__gt=(0))
    if v_term_category:
        v_term_category.append("dummy")
        query_term_category = Q(
            term_1__term_category_id__term_category__in=(v_term_category)
        )
        query_term_category.add(
            Q(term_2__term_category_id__term_category__in=(v_term_category)),
            Q.OR,  # noqa E501
        )  # noqa E501
    else:
        query_term_category = Q(connector_id__gt=(0))
    if v_term:
        v_term.append("dummy")
        query_term = Q(term_1__term__in=(v_term))
        query_term.add(Q(term_2__term__in=(v_term)), Q.OR)
    else:
        query_term = Q(connector_id__gt=(0))
    if v_word:
        v_word.append("dummy")
        query_word = Q(word1__in=(v_word))
        query_word.add(Q(word2__in=(v_word)), Q.OR)
    else:
        query_word = Q(connector_id__gt=(0))

    return (
        True,
        v_path_out,
        query_datasource,
        query_connector,
        query_term_group,
        query_term_category,
        query_term,
        query_word,
        v_aggr,
    )
