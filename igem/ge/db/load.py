# TODO: Create options to load from a DataFrame
# TODO: Create option to update data


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
        WordMap,
        WordTerm,
    )
except Exception as e:
    print(e)
    raise


def load_data(table: str, path: str, **kwargs) -> bool:
    """
    Loads data from a CSV file into the IGEM database. This process does
    not update existing data, it only inserts new records.

    Parameters
    ----------
    - table: str
        datasource, connector, ds_column, term_group, term_category, term,
        prefix, wordterm, termmap, wordmap
    - path: str
        full path and file name to load

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
    >>> from igem.ge import db
    >>> db.load_data(
            table="datasource”
            path=”{your_path}/datasource.csv”
            )
    """

    v_table = table.lower()
    v_path = path.lower()

    if v_path is None:
        print("inform the path to load")
        return False
    if not os.path.isfile(v_path):
        print("file not found")
        print("inform the path and the file in CSV format to load")
        return False

    if v_table == "datasource":
        try:
            df_src = pd.read_csv(v_path)
            df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        model_instances = [
            Datasource(
                datasource=record.datasource,
                description=record.description,
                category=record.category,
                website=record.website,
            )
            for record in df_src.itertuples()
        ]
        Datasource.objects.bulk_create(model_instances, ignore_conflicts=True)
        print("load with success to Datasource")
        return True

    elif v_table == "connector":
        try:
            df_src = pd.read_csv(v_path, encoding="utf-8")
            df_src["datasource"] = df_src["datasource"].str.lower()
            df_src["connector"] = df_src["connector"].str.lower()
        except IOError as e:
            print("ERRO:", e)
            return False
        df_datasource = pd.DataFrame(list(Datasource.objects.values()))
        df_src["db_id"] = df_src.set_index("datasource").index.map(
            df_datasource.set_index("datasource")["id"]
        )
        # TODO: What happen if datasource not exist
        model_instances = [
            Connector(
                connector=record.connector,
                datasource_id=record.db_id,
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
            )
            for record in df_src.itertuples()
        ]
        Connector.objects.bulk_create(model_instances, ignore_conflicts=True)
        print("load with success to Connector")
        # TODO: Updata data need different approach, add connector id
        # Connector.objects.bulk_update(
        #   model_instances,
        #   ['update_ds'],
        #   batch_size=None)
        # https://pytutorial.com/django-bulk-create-bulk-update/

        return True

    elif v_table == "ds_column":
        try:
            df_src = pd.read_csv(v_path)
            df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        df_connector = pd.DataFrame(list(Connector.objects.values()))
        df_src["connector"] = df_src.set_index("connector").index.map(
            df_connector.set_index("connector")["id"]
        )
        df_src["status"] = df_src["status"].replace("false", "False")
        df_src["status"] = df_src["status"].replace("true", "True")
        df_src["single_word"] = df_src["single_word"].replace("false", "False")
        df_src["single_word"] = df_src["single_word"].replace("true", "True")
        if df_src.isnull().values.any():
            print("Connector was not match. Check log file")
            df_src.to_csv(str(v_path + ".log"))
            return False
        model_instances = [
            DSTColumn(
                connector_id=record.connector,
                status=record.status,
                column_number=record.column_number,
                column_name=record.column_name,
                pre_value_id=record.pre_value,
                single_word=record.single_word,
            )
            for record in df_src.itertuples()
        ]
        DSTColumn.objects.bulk_create(model_instances, ignore_conflicts=True)
        return True

    elif v_table == "term_group":
        try:
            df_src = pd.read_csv(v_path)
            df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        model_instances = [
            TermGroup(
                term_group=record.term_group,
                description=record.description,
            )
            for record in df_src.itertuples()
        ]
        TermGroup.objects.bulk_create(model_instances, ignore_conflicts=True)
        print("Load with success to TermGroup")
        return True

    elif v_table == "term_category":
        try:
            df_src = pd.read_csv(v_path)
            df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            print(e)
            return False
        model_instances = [
            TermCategory(
                term_category=record.term_category,
                description=record.description,
            )
            for record in df_src.itertuples()
        ]
        TermCategory.objects.bulk_create(
            model_instances, ignore_conflicts=True
        )  # noqa E501
        print("Load with success to TermCategory")
        return True

    elif v_table == "term":
        try:
            df_src = pd.read_csv(v_path)
            df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        df_group = pd.DataFrame(list(TermGroup.objects.values()))
        df_category = pd.DataFrame(list(TermCategory.objects.values()))
        df_src["term_group_id"] = df_src.set_index("term_group").index.map(
            df_group.set_index("term_group")["id"]
        )
        df_src["term_category_id"] = df_src.set_index(
            "term_category"
        ).index.map(  # noqa E501
            df_category.set_index("term_category")["id"]
        )  # noqa E501
        if df_src.isnull().values.any():
            print("Group and/or Category was not match. Check log file")
            df_src.to_csv(str(v_path + ".log"))
            return False
        model_instances = [
            Term(
                term=record.term,
                term_category_id=record.term_category_id,
                term_group_id=record.term_group_id,
                description=record.description,
            )
            for record in df_src.itertuples()
        ]
        Term.objects.bulk_create(model_instances, ignore_conflicts=True)
        print("Load with success to Term")
        return True

    elif v_table == "prefix":
        try:
            df_src = pd.read_csv(v_path)
            df_src = df_src.apply(lambda x: x.astype(str).str.lower())
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
        print("Load with success to Prefix")
        return True

    elif v_table == "wordterm":
        try:
            df_src = pd.read_csv(v_path)
            df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        df_term = pd.DataFrame(list(Term.objects.values()))
        df_src["term_id"] = df_src.set_index("term").index.map(
            df_term.set_index("term")["id"]
        )  # noqa E501
        df_src["status"] = df_src["status"].replace("false", "False")
        df_src["status"] = df_src["status"].replace("true", "True")
        df_src["commute"] = df_src["commute"].replace("false", "False")
        df_src["commute"] = df_src["commute"].replace("true", "True")
        if df_src.isnull().values.any():
            print("Term was not match. Check log file")
            df_src.to_csv(str(v_path + ".log"))
            return False

        model_instances = [
            WordTerm(
                term_id=record.term_id,
                word=record.word,
                status=record.status,
                commute=record.commute,
            )
            for record in df_src.itertuples()
        ]
        WordTerm.objects.bulk_create(model_instances, ignore_conflicts=True)
        print("Load with success to WordTerms")
        return True

    elif v_table == "termmap":
        try:
            df_src = pd.read_csv(v_path)
            df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        df_term = pd.DataFrame(list(Term.objects.values()))
        df_connector = pd.DataFrame(list(Connector.objects.values()))
        df_src["connector_id"] = df_src.set_index("connector").index.map(
            df_connector.set_index("connector")["id"]
        )
        df_src["term_1_id"] = df_src.set_index("term_1").index.map(
            df_term.set_index("term")["id"]
        )
        df_src["term_2_id"] = df_src.set_index("term_2").index.map(
            df_term.set_index("term")["id"]
        )
        if df_src.isnull().values.any():
            print("Term was not match. Check log file")
            df_src.to_csv(str(v_path + ".log"))
            return False
        model_instances = [
            TermMap(
                ckey=record.ckey,
                connector_id=record.connector_id,
                term_1_id=record.term_1_id,
                term_2_id=record.term_2_id,
                qtd_links=record.qtd_links,
            )
            for record in df_src.itertuples()
        ]
        TermMap.objects.bulk_create(model_instances, ignore_conflicts=True)
        print("Load with success to TermMap")
        return True

    elif v_table == "wordmap":
        try:
            df_src = pd.read_csv(v_path)
            df_src = df_src.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print("ERRO:", e)
            return False
        df_term = pd.DataFrame(list(Term.objects.values()))
        df_connector = pd.DataFrame(list(Connector.objects.values()))
        df_datasource = pd.DataFrame(list(Datasource.objects.values()))
        df_src["datasource_id"] = df_src.set_index("datasource").index.map(
            df_datasource.set_index("datasource")["id"]
        )
        df_src["connector_id"] = df_src.set_index("connector").index.map(
            df_connector.set_index("connector")["id"]
        )
        df_src["term_1_id"] = df_src.set_index("term_1").index.map(
            df_term.set_index("term")["id"]
        )
        df_src["term_2_id"] = df_src.set_index("term_2").index.map(
            df_term.set_index("term")["id"]
        )
        if df_src.isnull().values.any():
            print("Term was not match. Check log file")
            df_src.to_csv(str(v_path + ".log"))
            return False
        model_instances = [
            WordMap(
                cword=record.cword,
                datasource_id=record.datasource_id,
                connector_id=record.connector_id,
                term_1_id=record.term_1_id,
                term_2_id=record.term_2_id,
                word_1=record.word_1,
                word_2=record.word_2,
                qtd_links=record.qtd_links,
            )
            for record in df_src.itertuples()
        ]
        WordMap.objects.bulk_create(model_instances, ignore_conflicts=True)
        print("Load with success to TermMap")
        return True

    else:
        print("Table not recognized in the system")
        return False
