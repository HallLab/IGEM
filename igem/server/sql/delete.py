import sys

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


def _delete_data(v_table, v_where_cs):
    try:
        if v_table == "datasource":
            qs = Datasource.objects.filter(**v_where_cs).delete()
        elif v_table == "connector":
            qs = Connector.objects.filter(**v_where_cs).delete()
        elif v_table == "ds_column":
            qs = DSTColumn.objects.filter(**v_where_cs).delete()
        elif v_table == "workflow":
            qs = WFControl.objects.filter(**v_where_cs).delete()
        elif v_table == "term":
            qs = Term.objects.filter(**v_where_cs).delete()
        elif v_table == "term_category":
            qs = TermCategory.objects.filter(**v_where_cs).delete()
        elif v_table == "term_group":
            qs = TermGroup.objects.filter(**v_where_cs).delete()
        elif v_table == "prefix":
            qs = PrefixOpc.objects.filter(**v_where_cs).delete()
        elif v_table == "wordterm":
            qs = WordTerm.objects.filter(**v_where_cs).delete()
        elif v_table == "termmap":
            qs = TermMap.objects.filter(**v_where_cs).delete()
        elif v_table == "wordmap":
            qs = WordMap.objects.filter(**v_where_cs).delete()
    except Exception as e:
        print("error to records delete: ", e)
        return False
    # qs = 0 did not find the record
    if qs[0] == 0:
        print("no records deleted")
        return False
    print("records deleted")
    return True


def delete_data(table, **kwargs):
    """
    Allows deleting a record from the given table. The deletion will be
    carried out in all records related to the informed parameter. For example,
    if we delete a datasource, the connectors, ds_columns, and termmap
    associated with the datasource will be deleted.

    Parameters
    ----------
    Only the table parameter will always be requested, the others will depend
    on the selected table, functioning as a record that will be eliminated.

     - table: str
        (datasource, connector, ds_column, term_group, term_category, term,
        prefix, wordterm, termmap, wordmap, workflow)
    - datasource: Dict{“str”:list[”str”]}
    - connector: Dict{“str”:list[”str”]}
    - word: Dict{“str”:list[”str”]}
    - term: Dict{“str”:list[”str”]}
    - term_category: Dict{“str”:list[”str”]}
    - term_group: Dict{“str”:list[”str”]}
    - prefix: Dict{“str”:list[”str”]}

    (Filter argument. It is used to filter the field, with the dictionary
    key being the selection argument and the dictionary value being the
    field selected as the filter. Without this parameter, the
    function will return all values of the field.)

    Return
    ------
    Boolean: (TRUE if the process occurred without errors and FALSE if had
    some errors).

    Examples
    --------
    >>> from igem.ge import db
    >>> db.delete_data(
            table='datasource',
            datasource={'datasource__in': [ds_01]}
            )
    """
    v_table = table.lower()
    v_datasource = kwargs.get("datasource", "error")
    v_connector = kwargs.get("connector", "error")
    v_word = kwargs.get("word", "error")
    v_term = kwargs.get("term", "error")
    v_term_category = kwargs.get("term_category", "error")
    v_term_group = kwargs.get("term_group", "error")
    v_prefix = kwargs.get("prefix", "error")

    if v_table == "datasource":
        v_where_cs = v_datasource

    elif v_table == "connector":
        v_where_cs = v_connector

    elif v_table == "ds_column":
        v_where_cs = v_connector

    elif v_table == "workflow":
        v_where_cs = v_connector

    elif v_table == "term":
        v_where_cs = v_term

    elif v_table == "term_category":
        v_where_cs = v_term_category

    elif v_table == "term_group":
        v_where_cs = v_term_group

    elif v_table == "prefix":
        # replace prefix to pre_value (model)
        v_prefix['pre_value__in'] = v_prefix.pop('prefix__in')
        v_where_cs = v_prefix

    elif v_table == "wordterm":
        if v_word:
            v_where_cs = v_word
        elif v_term:
            v_where_cs = v_term
        else:
            return False

    elif v_table == "termmap":
        if v_term:
            v_where_cs = v_term
        elif v_connector:
            v_where_cs = v_connector
        else:
            return False

    elif v_table == "wordmap":
        if v_word:
            v_where_cs = v_word
        elif v_connector:
            v_where_cs = v_connector
        else:
            return False

    else:
        return False

    return _delete_data(v_table, v_where_cs)
