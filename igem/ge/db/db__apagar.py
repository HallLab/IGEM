import os
import sys

import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist

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


def ge_db():
    return 'success'


def get_model_field_names(model, ignore_fields=['content_object']):
    model_fields = model._meta.get_fields()
    model_field_names = list(set([f.name for f in model_fields if f.name not in ignore_fields]))  # noqa E501
    return model_field_names


def get_lookup_fields(model, fields=None):
    model_field_names = get_model_field_names(model)
    if fields is not None:
        lookup_fields = []
        for x in fields:
            if "__" in x:
                # the __ is for ForeignKey lookups
                lookup_fields.append(x)
            elif x in model_field_names:
                lookup_fields.append(x)
    else:
        lookup_fields = model_field_names
    return lookup_fields


def qs_to_connector(qs, fields=None):
    lookup_fields = get_lookup_fields(qs.model, fields=fields)
    return list(qs.values(*lookup_fields))


def convert_to_dataframe(qs, fields=None, index=None):
    lookup_fields = get_lookup_fields(qs.model, fields=fields)
    index_col = None
    if index in lookup_fields:
        index_col = index
    elif "id" in lookup_fields:
        index_col = 'id'
    values = qs_to_connector(qs, fields=fields)
    df = pd.DataFrame.from_records(values, columns=lookup_fields, index=index_col)  # noqa E501
    return df


def get_datasource(v_datasource):
    if v_datasource != 'all':
        v_where_cs = {'datasource': v_datasource}
    else:
        v_where_cs = {}
    try:
        qs_datasource = Datasource.objects.filter(**v_where_cs).order_by('datasource')  # noqa E501
    except ObjectDoesNotExist:
        print('  Datasource not found')
        sys.exit(2)
    if not qs_datasource:
        print('  No data in Datasource table')
        sys.exit(2)
    return qs_datasource


def get_connector(v_datasource, v_connector):
    if v_datasource != 'all':
        try:
            QS_DB = Datasource.objects.filter(datasource=v_datasource)
            for qs in QS_DB:
                v_db_id = qs.id
        except Exception as e:
            print('  Datasource not found', e)
            sys.exit(2)
        if not QS_DB:
            print('  No data in Connector table')
            sys.exit(2)
    if v_datasource != 'all' and v_connector != 'all':
        v_where_cs = {'datasource': v_db_id, 'connector': v_connector}
    elif v_datasource == 'all' and v_connector != 'all':
        v_where_cs = {'connector': v_connector}
    elif v_datasource != 'all' and v_connector == 'all':
        v_where_cs = {'datasource': v_db_id}
    else:
        v_where_cs = {}
    try:
        QS = Connector.objects.filter(**v_where_cs).order_by('datasource', 'connector')  # noqa E501
    except ObjectDoesNotExist:
        print('  Connector not found')
        sys.exit(2)
    if not QS:
        print('  No data in Connector table')
        sys.exit(2)
    return QS


def get_ds_column(v_connector):
    if v_connector != 'all':
        try:
            QS_DB = Connector.objects.filter(connector=v_connector)
            for qs in QS_DB:
                v_db_id = qs.id
        except Exception as e:
            print('  Connector not found', e)
            sys.exit(2)
        if not QS_DB:
            print('  No data in Connector table')
            sys.exit(2)
    if v_connector != 'all':
        v_where_cs = {'connector': v_db_id}
    else:
        v_where_cs = {}
    try:
        QS = DSTColumn.objects.filter(**v_where_cs).order_by('connector')
    except ObjectDoesNotExist:
        print('  Connector not found')
        sys.exit(2)
    if not QS:
        print('  No data in DSTColumn table')
        sys.exit(2)
    return QS


def get_workflow(v_connector):
    if v_connector != 'all':
        try:
            QS_DB = Connector.objects.filter(connector=v_connector)
            for qs in QS_DB:
                v_db_id = qs.id
        except Exception as e:
            print('  Connector not found', e)
            sys.exit(2)
        if not QS_DB:
            print('  No data in Connector table')
            sys.exit(2)
    if v_connector != 'all':
        v_where_cs = {'connector': v_db_id}
    else:
        v_where_cs = {}
    try:
        QS = WFControl.objects.filter(**v_where_cs).order_by('connector')
    except ObjectDoesNotExist:
        print('  Connector not found')
        sys.exit(2)
    if not QS:
        print('  No data in WorkFlow table')
        sys.exit(2)
    return QS


def get_term(v_group, v_category):
    if v_group != 'all':
        try:
            QS_DB = TermGroup.objects.filter(group=v_group)
            for qs in QS_DB:
                v_id_group = qs.id
        except Exception as e:
            print('  TermGroup not found', e)
            sys.exit(2)
        if not QS_DB:
            print('  No data in TermGroup table')
            sys.exit(2)
    if v_category != 'all':
        try:
            QS_DB = TermCategory.objects.filter(category=v_category)
            for qs in QS_DB:
                v_id_cat = qs.id
        except Exception as e:
            print('  TermCategory not found', e)
            sys.exit(2)
        if not QS_DB:
            print('  No data in TermCategory table')
            sys.exit(2)
    if v_group != 'all' and v_category != 'all':
        v_where_cs = {'group': v_id_group, 'category': v_id_cat}
    elif v_group == 'all' and v_category != 'all':
        v_where_cs = {'category': v_id_cat}
    elif v_group != 'all' and v_category == 'all':
        v_where_cs = {'group': v_id_group}
    else:
        v_where_cs = {}
    try:
        QS = Term.objects.filter(**v_where_cs).order_by('group', 'category', 'term')  # noqa E501
    except ObjectDoesNotExist:
        print('  Term not found')
        sys.exit(2)
    if not QS:
        print('  No data in Term table')
        sys.exit(2)
    return QS


def get_category(*args):
    try:
        QS = TermCategory.objects.all().order_by('category')
    except ObjectDoesNotExist:
        print('  TermCategory not found')
        sys.exit(2)
    if not QS:
        print('  No data in TermCategory table')
        sys.exit(2)
    return QS


def get_group(*args):
    try:
        QS = TermGroup.objects.all().order_by('group')
    except ObjectDoesNotExist:
        print('  TermGroup not found')
        sys.exit(2)
    if not QS:
        print('  No data in TermGroup table')
        sys.exit(2)
    return QS


def get_prefix(*args):
    try:
        QS = PrefixOpc.objects.all().order_by('pre_value')
    except ObjectDoesNotExist:
        print('  Prefix not found')
        sys.exit(2)
    if not QS:
        print('  No data in Prefix table')
        sys.exit(2)
    return QS


def get_wordterm(v_word, v_term):
    if v_word != 'all' and v_term != 'all':
        v_where_cs = {'word__contains': v_word, 'term_id__term': v_term}
    elif v_word == 'all' and v_term != 'all':
        v_where_cs = {'term_id__term': v_term}
    elif v_word != 'all' and v_term == 'all':
        v_where_cs = {'word__contains': v_word}
    else:
        v_where_cs = {}

    try:
        QS = WordTerm.objects.filter(**v_where_cs).order_by('term', 'word')
    except ObjectDoesNotExist:
        print('  Word not found')
        sys.exit(2)
    if not QS:
        print('  No data in WordTerm table')
        sys.exit(2)
    return QS


def get_termmap(v_word):
    if v_word != 'all':
        v_where_cs = {'word__contains': v_word}  # %like%
    else:
        v_where_cs = {}
    try:
        QS = TermMap.objects.values(
            'ckey',
            'connector',
            'term_1',
            'term_2',
            'qtd_link',
            'term_1__term',
            'term_2__term'
            ).filter(**v_where_cs).order_by(
                'term_1__term',
                'term_2__term')
    except ObjectDoesNotExist:
        print('  Word not found')
        sys.exit(2)
    if not QS:
        print('  No data in TermMaps table')
        sys.exit(2)
    return QS


def get_wordmap(v_word):
    if v_word != 'all':
        v_where_cs = {'word1__contains': v_word}  # %like% TODO Improve
    else:
        v_where_cs = {}
    try:
        QS = WordMap.objects.filter(**v_where_cs).order_by('word1', 'word2')
    except ObjectDoesNotExist:
        print('  Word not found')
        sys.exit(2)
    if not QS:
        print('  No data in WordMap table')
        sys.exit(2)
    return QS

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


def show(
    table=None,
    datasource=None,
    connector=None,
    term_group=None,
    term_category=None,
    word=None,
    term=None
):

    v_table = table

    if v_table == 'datasources':
        v_datasource = datasource.lower()
        QS = get_datasource(v_datasource)
        print(f'{f"ID":<5}{f"DATABASE":<15}{f"CATEGORY":<15}{f"DESCRIPTION":<50}{f"WEBSITE":<50}')  # noqa E501
        for qs in QS:
            print(
                f'{f"{qs.id}":<5}{f"{qs.datasource}":<15}{f"{qs.category}":<15}{f"{qs.description}":<50}{f"{qs.website}":<50}')  # noqa E501

    elif v_table == 'connector':
        v_datasource = datasource.lower()
        v_connector = connector.lower()
        QS = get_connector(v_datasource, v_connector)
        print(f'{f"ID":<5}{f"DATABASE":<15}{f"DATASET":<15}{f"STATUS":<10}{f"DESCRIPTION":<50}')  # noqa E501
        for qs in QS:
            print(f'{f"{qs.id}":<5}{f"{qs.datasource}":<15}{f"{qs.connector}":<15}{f"{qs.update_ds}":<10}{f"{qs.description}":<50}')  # noqa E501

    elif v_table == 'ds_column':
        v_connector = connector.lower()
        QS = get_ds_column(v_connector)
        v_ds = ''
        print(f'{f"ID":<5}{f"DATASET":<15}{f"COL SEQ":<10}{f"COL NAME":<25}{f"STATUS":<10}{f"PREFIX":<10}')  # noqa E501
        for qs in QS:
            if v_ds != str(qs.connector):
                print('')
            if str(qs.pre_value) == 'none':
                v_pre = ''
            else:
                v_pre = qs.pre_value
            print(f'{f"{qs.id}":<5}{f"{qs.connector}":<15}{f"{qs.column_number}":<10}{f"{qs.column_name}":<25}{f"{qs.status}":<10}{f"{v_pre}":<10}')  # noqa E501
            v_ds = str(qs.connector)

    elif v_table == 'workflow':
        v_connector = connector.lower()
        QS = get_workflow(v_connector)
        print(f'{f"DATASET":<15}{f"DT UPDATE":<25}{f"VERSION":<40}{f"SIZE":<15}{f"COLLECT":<10}{f"PREPARE":<10}{f"MAP":<10}{f"REDUCE":<10}')  # noqa E501
        for qs in QS:
            if str(qs.last_update) != '':
                v_upd = str(qs.last_update)[:19]
            v_col = ''
            v_pre = ''
            v_red = ''
            v_map = ''
            if qs.chk_collect:
                v_col = 'pass'
            if qs.chk_prepare:
                v_pre = 'pass'
            if qs.chk_map:
                v_map = 'pass'
            if qs.chk_reduce:
                v_red = 'pass'
            print(f'{f"{qs.connector}":<15}{f"{v_upd}":<25}{f"{qs.source_file_version}":<40}{f"{qs.source_file_size}":<15}{f"{v_col}":<10}{f"{v_pre}":<10}{f"{v_map}":<10}{f"{v_red}":<10}       ')  # noqa E501

    elif v_table == 'term':
        v_group = term_group.lower()
        v_category = term_category.lower()
        QS = get_term(v_group, v_category)
        print(f'{f"ID":<15}{f"GROUP":<15}{f"CATEGORY":<15}{f"KEYGE":<20}{f"DESCRIPTION":<50}')  # noqa E501
        for qs in QS:
            print(f'{f"{qs.id}":<15}{f"{qs.group}":<15}{f"{qs.category}":<15}{f"{qs.term}":<20}{f"{qs.description}":<50}')  # noqa E501

    elif v_table == 'term_category':
        QS = get_category()
        print(f'{f"ID":<5}{f"CATEGORY":<15}{f"DESCRIPTION":<50}')
        for qs in QS:
            print(f'{f"{qs.id}":<5}{f"{qs.category}":<15}{f"{qs.description}":<50}')  # noqa E501  

    elif v_table == 'term_group':
        QS = get_group()
        print(f'{f"ID":<5}{f"GROUP":<15}{f"DESCRIPTION":<50}')
        for qs in QS:
            print(f'{f"{qs.id}":<5}{f"{qs.group}":<15}{f"{qs.description}":<50}')  # noqa E501

    elif v_table == 'prefix':
        QS = get_prefix()
        print(f'{f"pre_value":<15}')
        for qs in QS:
            print(f'{f"{qs.pre_value}":<15}')

    elif v_table == 'wordterm':
        v_word = word.lower()
        v_term = term.lower()
        QS = get_wordterm(v_word, v_term)
        print(f'{f"STATUS":<10}{f"COMMUTE":<10}{f"KEYGE":<40}{f"WORD":<50}')
        for qs in QS:
            print(f'{f"{qs.status}":<10}{f"{qs.commute}":<10}{f"{qs.term}":<40}{f"{qs.word}":<50}')  # noqa E501

    elif v_table == 'termmap':
        print('function not implemented')
    elif v_table == 'wordmap':
        print('function not implemented')
    else:
        print('Table not recognized in the system. Choose one of the options: ')  # noqa E501
        print('   datasource | connector | ds_column | workflow | term | term_category | term_group | prefix | wordterm | termmap | wordmap')  # noqa E501


def download(
    path=None,
    table=None,
    datasource=None,
    connector=None,
    term_group=None,
    term_category=None,
    word=None,
    term=None
):

    v_path = path.lower()
    v_table = table.lower()

    if v_path is None:
        print('  Inform the path to download')
        sys.exit(2)
    if not os.path.isdir(v_path):
        print('  Path not found')
        sys.exit(2)
    v_file = v_path + "/" + v_table + ".csv"

    if v_table == 'datasource':
        v_datasource = datasource.lower()
        QS = get_datasource(v_datasource)
        DF = convert_to_dataframe(QS, fields=['datasource', 'description', 'website', 'category'], index=False)  # noqa E501
        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    elif v_table == 'connector':
        v_datasource = datasource.lower()
        v_connector = connector.lower()
        QS = get_connector(v_datasource, v_connector)
        DF = convert_to_dataframe(QS, fields=[
            'datasource',
            'connector',
            'update_ds',
            'source_path',
            'source_web',
            'source_compact',
            'source_file_name',
            'source_file_format',
            'source_file_sep',
            'source_file_skiprow',
            'target_file_name',
            'target_file_format',
            'description'
            ], index=False)
        # Data transformations rules
        # Rule 1: Transform Datasource ID to Datasource Name
        try:
            DF_DB = pd.DataFrame(list(Datasource.objects.values('id', 'datasource').order_by('id')))  # noqa E501
        except Exception as e:
            print('  Datasource not found', e)
            sys.exit(2)
        if DF_DB.empty:
            print('  No data in Connector table')
            sys.exit(2)
        DF["datasource"] = DF.set_index("datasource").index.map(DF_DB.set_index("id")["datasource"])  # noqa E501
        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    elif v_table == 'ds_column':
        v_connector = connector.lower()
        QS = get_ds_column(v_connector)
        DF = convert_to_dataframe(QS, fields=[
            'connector',
            'status',
            'column_number',
            'column_name',
            'pre_value',
            'single_word'
            ], index=False)
        # Data transformations rules
        # Rule 1: Transform Connector ID to Connector Name
        try:
            DF_DB = pd.DataFrame(list(Connector.objects.values('id', 'connector').order_by('id')))  # noqa E501
        except Exception as e:
            print('  Connector not found', e)
            sys.exit(2)
        if DF_DB.empty:
            print('  No data in Connector table')
            sys.exit(2)
        DF["connector"] = DF.set_index("connector").index.map(DF_DB.set_index("id")["connector"])  # noqa E501

        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    elif v_table == 'workflow':
        v_connector = connector.lower()
        QS = get_workflow(v_connector)
        DF = convert_to_dataframe(QS, fields=[
            'connector',
            'last_update',
            'source_file_version',
            'source_file_size',
            'target_file_size',
            'chk_collect',
            'chk_prepare',
            'chk_map',
            'chk_reduce'
            ], index=False)
        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    elif v_table == 'term':
        v_group = term_group.lower()
        v_category = term_category.lower()
        QS = get_term(v_group, v_category)
        DF = convert_to_dataframe(QS, fields=[
            'term',
            'group',
            'category',
            'description'
            ], index=False)
        # Data transformations rules
        # Rule 1: Transform TermGroup ID to TermGroup Name
        try:
            DF_DB = pd.DataFrame(list(TermGroup.objects.values('id', 'group').order_by('id')))  # noqa E501
        except Exception as e:
            print('  TermGroup not found', e)
            sys.exit(2)
        if DF_DB.empty:
            print('  No data in TermGroup table')
            sys.exit(2)
        DF["group"] = DF.set_index("group").index.map(DF_DB.set_index("id")["group"])  # noqa E501
        # Rule 2: Transform TermCategory ID to TermCategory Name
        try:
            DF_DB = pd.DataFrame(list(TermCategory.objects.values('id', 'category').order_by('id')))  # noqa E501
        except Exception as e:
            print('  TermCategory not found', e)
            sys.exit(2)
        if DF_DB.empty:
            print('  No data in TermCategory table')
            sys.exit(2)
        DF["category"] = DF.set_index("category").index.map(DF_DB.set_index("id")["category"])  # noqa E501
        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    elif v_table == 'term_category':
        QS = get_category()
        DF = convert_to_dataframe(QS, fields=[
            'category',
            'description'
            ], index=False)
        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    elif v_table == 'term_group':
        QS = get_group()
        DF = convert_to_dataframe(QS, fields=[
            'group',
            'description'
            ], index=False)
        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    elif v_table == 'prefix':
        QS = get_prefix()
        DF = convert_to_dataframe(QS, fields=['pre_value'], index=False)
        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    elif v_table == 'wordterm':
        v_word = word.lower()
        QS = get_wordterm(v_word)
        DF = convert_to_dataframe(QS, fields=[
            'status',
            'commute',
            'word',
            'term'
            ], index=False)
        # Data transformations rules
        # Rule 1: Transform term ID to term Name
        try:
            DF_DB = pd.DataFrame(list(Term.objects.values('id', 'term').order_by('id')))  # noqa E501
        except Exception as e:
            print('  Term not found', e)
            sys.exit(2)
        if DF_DB.empty:
            print('  No data in Term table')
            sys.exit(2)
        DF["term"] = DF.set_index("term").index.map(DF_DB.set_index("id")["term"])  # noqa E501
        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    elif v_table == 'termmap':
        v_word = word.lower()
        QS = get_termmap(v_word)
        DF = convert_to_dataframe(QS, fields=[
            'ckey',
            'connector',
            'term_1',
            'term_1__term',
            'term_2',
            'term_2__term',
            'qtd_link'
            ], index=False)
        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    elif v_table == 'wordmap':
        v_word = word.lower()
        QS = get_wordmap(v_word)
        DF = convert_to_dataframe(QS, fields=[
            'cword',
            'datasource',
            'connector',
            'term_1',
            'term_2',
            'word_1',
            'word_2',
            'qtd_link'
            ], index=False)
        DF.to_csv(v_file, index=False)
        print('  File generated successfully')

    else:
        print('Table not recognized in the system. Choose one of the options: ')  # noqa E501
        print('   datasource | connector | ds_column | workflow | term | category | group | prefix | key_word | termmap | wordmap')  # noqa E501


def load(
    path=None,
    table=None,
):

    v_table = table.lower()
    v_path = path.lower()

    if v_path is None:
        print('  Inform the path to load')
        sys.exit(2)
    if not os.path.isfile(v_path):
        print('  File not found')
        print('  Inform the path and the file in CSV format to load')
        sys.exit(2)

    if v_table == 'datasource':
        try:
            DFR = pd.read_csv(v_path)
            DFR = DFR.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print('ERRO:')
            print(e)
            sys.exit(2)
        model_instances = [Datasource(
            datasource=record.datasource,
            description=record.description,
            category=record.category,
            website=record.website,
            ) for record in DFR.itertuples()]
        Datasource.objects.bulk_create(model_instances, ignore_conflicts=True)
        print('  Load with success to Datasource')

    elif v_table == 'connector':
        try:
            DFR = pd.read_csv(v_path)
            DFR['datasource'] = DFR['datasource'].str.lower()
            DFR['connector'] = DFR['connector'].str.lower()
        except IOError as e:
            print('ERRO:')
            print(e)
            sys.exit(2)
        DFDB = pd.DataFrame(list(Datasource.objects.values()))
        DFR["db_id"] = DFR.set_index("datasource").index.map(DFDB.set_index("datasource")["id"])  # noqa E501
        # tratar se nao localizar
        model_instances = [Connector(
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
        ) for record in DFR.itertuples()]
        Connector.objects.bulk_create(model_instances, ignore_conflicts=True)
        print('  Load with success to Connector')

    elif v_table == 'ds_column':
        try:
            DFR = pd.read_csv(v_path)
            DFR = DFR.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print('ERRO:')
            print(e)
            sys.exit(2)
        DFG = pd.DataFrame(list(Connector.objects.values()))
        DFR["connector"] = DFR.set_index("connector").index.map(DFG.set_index("connector")["id"])  # noqa E501
        DFR['status'] = DFR['status'].replace('false', 'False')
        DFR['status'] = DFR['status'].replace('true', 'True')
        DFR['single_word'] = DFR['single_word'].replace('false', 'False')
        DFR['single_word'] = DFR['single_word'].replace('true', 'True')
        if DFR.isnull().values.any():
            print('  Connector was not match. Check log file')
            DFR.to_csv(str(v_path + ".log"))
            sys.exit(2)
        model_instances = [DSTColumn(
            connector_id=record.connector,
            status=record.status,
            column_number=record.column_number,
            column_name=record.column_name,
            pre_value_id=record.pre_value,
            single_word=record.single_word,
            ) for record in DFR.itertuples()]
        DSTColumn.objects.bulk_create(model_instances, ignore_conflicts=True)

    elif v_table == 'term':
        try:
            DFR = pd.read_csv(v_path)
            DFR = DFR.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print('ERRO:')
            print(e)
            sys.exit(2)
        DFG = pd.DataFrame(list(TermGroup.objects.values()))
        DFC = pd.DataFrame(list(TermCategory.objects.values()))
        DFR["group_id"] = DFR.set_index("term_group").index.map(DFG.set_index("term_group")["id"])  # noqa E501
        DFR["category_id"] = DFR.set_index("term_category").index.map(DFC.set_index("term_category")["id"])  # noqa E501
        if DFR.isnull().values.any():
            print('  TermGroup and/or TermCategory was not match. Check log file')  # noqa E501
            DFR.to_csv(str(v_path + ".log"))
            sys.exit(2)
        model_instances = [Term(
            term=record.term,
            category_id=record.term_category_id,
            group_id=record.term_group_id,
            description=record.description,
            ) for record in DFR.itertuples()]
        Term.objects.bulk_create(model_instances, ignore_conflicts=True)
        print('  Load with success to Term')

    elif v_table == 'term_category':
        try:
            DFR = pd.read_csv(v_path)
            DFR = DFR.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print('ERRO:')
            print(e)
            sys.exit(2)
        model_instances = [TermCategory(
            category=record.term_category,
            description=record.description,
            ) for record in DFR.itertuples()]
        TermCategory.objects.bulk_create(model_instances, ignore_conflicts=True)  # noqa E501
        print('  Load with success to TermCategory')

    elif v_table == 'term_group':
        try:
            DFR = pd.read_csv(v_path)
            DFR = DFR.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print('ERRO:')
            print(e)
            sys.exit(2)
        model_instances = [TermGroup(
            group=record.group,
            description=record.description,
            ) for record in DFR.itertuples()]
        TermGroup.objects.bulk_create(model_instances, ignore_conflicts=True)
        print('  Load with success to TermGroup')

    elif v_table == 'prefix':
        try:
            DFR = pd.read_csv(v_path)
            DFR = DFR.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print('ERRO:')
            print(e)
            sys.exit(2)
        model_instances = [PrefixOpc(
            pre_value=record.pre_value,
            ) for record in DFR.itertuples()]
        PrefixOpc.objects.bulk_create(model_instances, ignore_conflicts=True)
        print('  Load with success to Prefix')

    elif v_table == 'wordterm':
        try:
            DFR = pd.read_csv(v_path)
            DFR = DFR.apply(lambda x: x.astype(str).str.lower())
        except IOError as e:
            print('ERRO:')
            print(e)
            sys.exit(2)
        DFK = pd.DataFrame(list(Term.objects.values()))
        DFR["term_id"] = DFR.set_index("term").index.map(DFK.set_index("term")["id"]) # noqa E501
        DFR['status'] = DFR['status'].replace('false', 'False')
        DFR['status'] = DFR['status'].replace('true', 'True')
        DFR['commute'] = DFR['commute'].replace('false', 'False')
        DFR['commute'] = DFR['commute'].replace('true', 'True')
        if DFR.isnull().values.any():
            print('  Term was not match. Check log file')
            DFR.to_csv(str(v_path + ".log"))
            sys.exit(2)

        model_instances = [WordTerm(
            term_id=record.term_id,
            word=record.word,
            status=record.status,
            commute=record.commute,
            ) for record in DFR.itertuples()]
        WordTerm.objects.bulk_create(model_instances, ignore_conflicts=True)
        print('  Load with success to WordTerms')

    else:
        print('Table not recognized in the system. Choose one of the options: ') # noqa E501
        print('   datasource | connector | ds_column | term | category | group | prefix | wordterms') # noqa E501


# def truncate(
#     table=None
# ):
#     v_table = truncate.lower()

#     if v_table == 'all':
#         TermMap.truncate()
#         WordMap.truncate()
#         WordTerm.truncate()
#         Term.truncate()
#         TermCategory.truncate()
#         TermGroup.truncate()
#         LogsCollector.truncate()
#         WFControl.truncate()
#         DSTColumn.truncate()
#         PrefixOpc.truncate()
#         Connector.truncate()
#         Datasource.truncate()
#         print('  All tables deleted')

#     elif v_table == 'termmap':
#         TermMap.truncate()
#         print('  Keylinks table deleted')

#     elif v_table == 'wordmap':
#         WordMap.truncate()
#         print('  WordMap table deleted')

#     elif v_table == 'wordterm':
#         WordTerm.truncate()
#         print('  WordTerm table deleted')

#     elif v_table == 'term':
#         Term.truncate()
#         print('  Term table deleted')

#     elif v_table == 'term_category':
#         TermCategory.truncate()
#         print('  TermCategory table deleted')

#     elif v_table == 'term_group':
#         TermGroup.truncate()
#         print('  TermGroup table deleted')

#     elif v_table == 'logs':
#         LogsCollector.truncate()
#         print('  Logs table deleted')

#     elif v_table == 'workflow':
#         WFControl.truncate()
#         print('  WorkFlow table deleted')

#     elif v_table == 'dst':
#         DSTColumn.truncate()
#         print('  Ds Column table deleted')

#     elif v_table == 'prefix':
#         PrefixOpc.truncate()
#         print('  Prefix table deleted')

#     elif v_table == 'connector':
#         Connector.truncate()
#         print('  Connector table deleted')

#     elif v_table == 'datasource':
#         Datasource.truncate()
#         print('  Datasource table deleted')

#     else:
#           print('')

# def delete(
#     table=None,
#     datasource=None,
#     connector=None,
#     term_group=None,
#     term_category=None,
#     word=None,
#     term=None,
#     prefix=None
# ):

#     v_table = table.lower()

#     if v_table == 'datasource':
#         v_datasource = datasource.lower()
#         if v_datasource != 'all':
#             v_where_cs = {'datasource': v_datasource}
#         else:
#             v_where_cs = {}
#         try:
#             Datasource.objects.filter(**v_where_cs).delete()
#             # qs_datasource = Datasource.objects.filter(**v_where_cs).delete()  # noqa E501
#         except ObjectDoesNotExist:
#             print('  Datasource not found')
#             sys.exit(2)
#         print("  Datasource successfully deleted")

#     elif v_table == 'connector':
#         v_datasource = datasource.lower()
#         v_connector = connector.lower()

#         if v_datasource != 'all':
#             try:
#                 QS_DB = Datasource.objects.filter(datasource=v_datasource)
#                 for qs in QS_DB:
#                     v_db_id = qs.id
#             except Exception as e:
#                 print('  Datasource not found', e)
#                 sys.exit(2)
#             if not QS_DB:
#                 print('  No data in Connector table')
#                 sys.exit(2)

#         if v_datasource != 'all' and v_connector != 'all':
#             v_where_cs = {'datasource': v_db_id, 'connector': v_connector}
#         elif v_datasource == 'all' and v_connector != 'all':
#             v_where_cs = {'connector': v_connector}
#         elif v_datasource != 'all' and v_connector == 'all':
#             v_where_cs = {'datasource': v_db_id}
#         else:
#             v_where_cs = {}
#         try:
#             # QS = Connector.objects.filter(**v_where_cs).delete()
#             Connector.objects.filter(**v_where_cs).delete()
#         except ObjectDoesNotExist:
#             print('  Connector not found')
#             sys.exit(2)
#         print("  Connector successfully deleted")
#     elif v_table == 'ds_column':
#         v_connector = connector.lower()
#         if v_connector != 'all':
#             try:
#                 QS_DB = Connector.objects.filter(connector=v_connector)
#                 for qs in QS_DB:
#                     v_db_id = qs.id
#             except Exception as e:
#                 print('  Connector not found', e)
#                 sys.exit(2)
#             if not QS_DB:
#                 print('  No data in Connector table')
#                 sys.exit(2)
#         if v_connector != 'all':
#             v_where_cs = {'connector': v_db_id}
#         else:
#             v_where_cs = {}
#         try:
#             DSTColumn.objects.filter(**v_where_cs).delete()
#             # QS = DSTColumn.objects.filter(**v_where_cs).delete()
#         except ObjectDoesNotExist:
#             print('  Connector not found')
#             sys.exit(2)
#         print("  Connector Column Transformation successfully deleted")
#         # Improvement: add column idx

#     elif v_table == 'workflow':
#         v_connector = connector.lower()
#         if v_connector != 'all':
#             try:
#                 QS_DB = Connector.objects.filter(connector=v_connector)
#                 for qs in QS_DB:
#                     v_db_id = qs.id
#             except Exception as e:
#                 print('  Connector not found', e)
#                 sys.exit(2)
#             if not QS_DB:
#                 print('  No data in Connector table')
#                 sys.exit(2)
#         if v_connector != 'all':
#             v_where_cs = {'connector': v_db_id}
#         else:
#             v_where_cs = {}
#         try:
#             WFControl.objects.filter(**v_where_cs).delete()
#         except ObjectDoesNotExist:
#             print('  Connector not found')
#             sys.exit(2)
#         print("  Workflow successfully deleted")

#     elif v_table == 'term':
#         v_group = term_group.lower()
#         v_category = term_category.lower()
#         v_term = term.lower()
#         if v_group != 'all':
#             try:
#                 QS_DB = TermGroup.objects.filter(group=v_group)
#                 for qs in QS_DB:
#                     v_id_group = qs.id
#             except Exception as e:
#                 print('  Connector not found', e)
#                 sys.exit(2)
#             if not QS_DB:
#                 print('  No data in TermGroup table')
#                 sys.exit(2)
#         if v_category != 'all':
#             try:
#                 QS_DB = TermCategory.objects.filter(category=v_category)
#                 for qs in QS_DB:
#                     v_id_cat = qs.id
#             except Exception as e:
#                 print('  TermCategory not found', e)
#                 sys.exit(2)
#             if not QS_DB:
#                 print('  No data in TermCategory table')
#                 sys.exit(2)
#         if v_group != 'all' and v_category != 'all':
#             v_where_cs = {'term_group': v_id_group, 'term_category': v_id_cat}  # noqa E501
#         elif v_group == 'all' and v_category != 'all':
#             v_where_cs = {'term_category': v_id_cat}
#         elif v_group != 'all' and v_category == 'all':
#             v_where_cs = {'term_group': v_id_group}
#         elif v_term != 'all':
#             v_where_cs = {'term': v_term}
#         else:
#             print("  operation not performed")
#             sys.exit(2)
#         try:
#             Term.objects.filter(**v_where_cs).delete()
#         except ObjectDoesNotExist:
#             print('  Term not found')
#             sys.exit(2)
#         print("  KEYGE successfully deleted")

#     elif v_table == 'term_category':
#         v_category = term_category.lower()
#         if v_category == 'all':
#             print("  Inform the TermCategory")
#             sys.exit(2)
#         try:
#             TermCategory.objects.filter(category=v_category).delete()
#             # QS = TermCategory.objects.filter(category=v_category).delete()
#         except ObjectDoesNotExist:
#             print('  TermCategory not found')
#             sys.exit(2)
#         print("  TermCategory successfully deleted")

#     elif v_table == 'term_group':
#         v_group = term_group.lower()
#         if v_group == 'all':
#             print("  Inform the TermGroup")
#             sys.exit(2)
#         try:
#             # QS = TermGroup.objects.filter(group=v_group).delete()
#             TermGroup.objects.filter(group=v_group).delete()
#         except ObjectDoesNotExist:
#             print('  TermCategory not found')
#             sys.exit(2)
#         print("  TermGroup successfully deleted")

#     elif v_table == 'prefix':
#         v_prefix = prefix.lower()
#         if v_prefix == 'all':
#             print("  Inform the Prefix")
#             sys.exit(2)
#         try:
#             PrefixOpc.objects.filter(pre_value=v_prefix).delete()
#         except ObjectDoesNotExist:
#             print('  Prefix not found')
#             sys.exit(2)
#         print("  Prefix successfully deleted")

#     elif v_table == 'wordterm':
#         v_word = word.lower()
#         v_term = term.lower()
#         if v_word != 'all' and v_term != 'all':
#             v_where_cs = {'word__contains': v_word, 'term': v_term}  # %like%
#         elif v_word != 'all' and v_term == 'all':
#             v_where_cs = {'word__contains': v_word}  # %like%
#         elif v_word == 'all' and v_term != 'all':
#             v_where_cs = {'term': v_term}  # %like%
#         else:
#             print("  Inform the Term and/or Word")
#             sys.exit(2)
#         try:
#             WordTerm.objects.filter(**v_where_cs).delete()
#         except ObjectDoesNotExist:
#             print('  Word not found')
#             sys.exit(2)
#         print("  Keyword successfully deleted")

#     elif v_table == 'termmap':
#         print('  Option not implemented')

#     elif v_table == 'wordmap':
#         print('  Option not implemented')

#     else:
#         print('Table not recognized in the system. Choose one of the options: ')  # noqa E501
#         print('   datasource | connector | ds_column | workflow | term | category | group | prefix | key_word | termmap | wordmap')  # noqa E501
