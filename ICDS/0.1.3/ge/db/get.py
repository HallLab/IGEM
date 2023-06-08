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
        WFControl,
        WordMap,
        WordTerm,
    )
except Exception as e:
    print(e)
    raise


def get_data(table, **kwargs):
    """
    The get_data() function allows extracting data from the GE database
    and loading this data into a Pandas DataFrame structure or CSV File.

    It has an intelligent filter mechanism that allow you to perform data
    selections simply through a conversion layer of function arguments and SQL
    syntax. This allows the same input arguments regardless of implemented
    database management system.

    Parameters
    ----------
    Only the table parameter will be mandatory, the others being optional, and
    will model the data output. In the case of only informing the table, the
    function will return a DataFrame with all the columns and values of the
    table.

    - table: str
        datasource, connector, ds_column, term_group, term_category, term,
        prefix,  wordterm, termmap, wordmap
    - path: str
        With this parameter, the function will save the selected data
        in a file in the directory informed as the parameter argument. In this
        scenario, data will not be returned in the form of a Dataframe; only a
        Boolean value will be returned, informing whether the file was
        generated or not
    - columns: list[“str”]
        Columns that will be selected for output. They must be informed with
        the same name as the database. It is possible to load other data from
        other tables as long as it correlate. For example, suppose the table
        only has the term field and not the category field. In that case, you
        can inform as an argument: "term_id__term_category_id__category", the
        system selected the ID of the term, consulted the ID of the category
        in the Term table, and went to the Category table to choose the
        category
    - columns_out: list[“str”]
        If you want to rename the header of the output fields to more familiar
        names, you can use this parameter, passing the desired names in the
        same sequential sequence in the parameter columns
    - datasource: Dict{“str”:list[”str”]}
        Filter argument. It is used to filter datasource, with the dictionary
        key being the selection argument and the dictionary value being the
        datasources selected as the filter. Without this parameter, the
        function will return all datasources
    - connector: Dict{“str”:list[”str”]}
        Filter argument. It uses the same logic as the datasource, but applied
        to the connector field
    - word: Dict{“str”:list[”str”]}
        Filter argument. It uses the same logic as the datasource, but applied
        to the word field
    - term: Dict{“str”:list[”str”]}
        Filter argument. It uses the same logic as the datasource, but applied
        to the term field
    - term_category: Dict{“str”:list[”str”]}
        Filter argument. It uses the same logic as the datasource, but applied
        to the term_categorty field
    - term_group: Dict{“str”:list[”str”]}
        Filter argument. It uses the same logic as the datasource, but applied
        to the term_group field


    Return
    ------
    Pandas Dataframe or Boolean (If the parameter path is informed, the
    function will generate the file; if successful, it will return the
    TRUE. Otherwise, it will return FALSE)

    Examples
    --------
    >>> from igem.ge import db
    >>> db.get_data(
            table=”datasource”,
            datasource={“datasource__in”: [“ds_01”,”ds_02”]},
            columns=[“id”,”datasource”],
            columns_out=[“Datasource ID”, “Datasource Name”],
            path=”{your_path}/datasource.csv”
            )

    >>> df = db.get_data(
            table="connector",
            connector={"connector__start": ["conn_ds"]},
            datasource={"datasource_id__datasource__in": ["ds_01"]},
            columns=["connector", "status"]
            )

    >>> x = db.get_data(
            table="termmap",
            term={"term_id__term": "chem:c112297"},
            path="{your_path},
            )
        If x:
            print("file created")
    """

    try:
        v_table = table.lower()
        v_path = kwargs.get("path", "")
        v_datasource = kwargs.get("datasource", {})
        v_connector = kwargs.get("connector", {})
        v_word = kwargs.get("word", {})
        v_term = kwargs.get("term", {})
        v_term_category = kwargs.get("term_category", {})
        v_term_group = kwargs.get("term_group", {})
        v_columns = kwargs.get("columns", [])
        v_columns_out = kwargs.get("columns_out", [])

        if v_table == "datasource":
            if not v_columns:
                v_columns = [
                    "datasource",
                    "description",
                    "website",
                    "category",
                ]  # noqa E501

            v_where_cs = {**v_datasource}

            qs = (
                Datasource.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("datasource")
            )
            df = pd.DataFrame(list(qs), columns=v_columns)

        elif v_table == "connector":
            if not v_columns:
                v_columns = [
                    "connector",
                    "datasource",
                    "datasource_id__datasource",
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
                    "description",
                ]
                v_columns_out = v_columns
            v_where_cs = {**v_datasource, **v_connector}
            qs = (
                Connector.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("datasource_id__datasource", "connector")
            )
            df = pd.DataFrame(list(qs), columns=v_columns_out)

        elif v_table == "ds_column":
            if not v_columns:
                v_columns = [
                    "connector",
                    "connector_id__connector",
                    "status",
                    "column_number",
                    "column_name",
                    "pre_value",
                    "single_word",
                ]
                v_columns_out = v_columns
            v_where_cs = {**v_connector}
            qs = (
                DSTColumn.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("connector")
            )
            df = pd.DataFrame(list(qs), columns=v_columns_out)

        elif v_table == "workflow":
            if not v_columns:
                v_columns = [
                    "connector",
                    "last_update",
                    "source_file_version",
                    "source_file_size",
                    "target_file_size",
                    "chk_collect",
                    "chk_prepare",
                    "chk_map",
                    "chk_reduce",
                ]
                v_columns_out = v_columns
            v_where_cs = {**v_connector}
            qs = (
                WFControl.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("connector")
            )
            df = pd.DataFrame(list(qs), columns=v_columns_out)

        elif v_table == "term":
            if not v_columns:
                v_columns = [
                    "term",
                    "term_group",
                    "term_category",
                    "description",
                ]  # noqa E501
                v_columns_out = v_columns
            v_where_cs = {**v_term_group, **v_term_category, **v_term}
            qs = (
                Term.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("term")
            )
            df = pd.DataFrame(list(qs), columns=v_columns_out)

        elif v_table == "term_category":
            if not v_columns:
                v_columns = ["term_category", "description"]
                v_columns_out = v_columns
            v_where_cs = {**v_term_category}
            qs = (
                TermCategory.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("term_category")
            )
            df = pd.DataFrame(list(qs), columns=v_columns_out)

        elif v_table == "term_group":
            if not v_columns:
                v_columns = ["term_group", "description"]
                v_columns_out = v_columns
            v_where_cs = {**v_term_group}
            qs = (
                TermGroup.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("term_group")
            )
            df = pd.DataFrame(list(qs), columns=v_columns_out)

        elif v_table == "prefix":
            v_where_cs = {}
            qs = PrefixOpc.objects.filter(**v_where_cs)
            df = pd.DataFrame(list(qs), columns=["pre_value"])

        elif v_table == "wordterm":
            if not v_columns:
                v_columns = [
                    "status",
                    "commute",
                    "word",
                    "term",
                    "term_id__term",
                    "term_id__term_category_id__term_category",
                    "term_id__term_group_id__term_group",
                ]
                v_columns_out = v_columns
            v_where_cs = {**v_word, **v_term}
            qs = (
                WordTerm.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("word")
            )
            df = pd.DataFrame(list(qs), columns=v_columns_out)

        elif v_table == "termmap":
            if not v_columns:
                v_columns = [
                    "ckey",
                    "connector",
                    "term_1",
                    "term_1__term",
                    "term_2",
                    "term_2__term",
                    "qtd_links",
                ]
                v_columns_out = v_columns
            v_where_cs = {**v_term, **v_term_category, **v_term_group}
            qs = (
                TermMap.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("ckey")
            )
            df = pd.DataFrame(list(qs), columns=v_columns_out)

        elif v_table == "wordmap":
            if not v_columns:
                v_columns = [
                    "cword",
                    "datasource",
                    "connector",
                    "term_1",
                    "term_2",
                    "word_1",
                    "word_2",
                    "qtd_links",
                ]
                v_columns_out = v_columns
            v_where_cs = {**v_word}
            qs = (
                WordMap.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("cword")
            )
            df = pd.DataFrame(list(qs), columns=v_columns_out)

        else:
            return False

        if v_path:
            if df.empty:
                return False
            if not os.path.isdir(v_path):
                return False
            v_file = v_path + "/" + v_table + ".csv"
            df.to_csv(v_file, index=False)
            return True
        else:
            return df
    except Exception as e:
        return e
