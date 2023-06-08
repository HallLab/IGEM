# TODO:  Create second function to mgm trucante and message
# TODO:  Paramenter to hide prints/message

import sys

from django.conf import settings

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import (
        Connector,
        Datasource,
        DSTColumn,
        Logs,
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


def truncate_table(table: str = "") -> bool:
    """
    will delete all records from a table, never use this function, with excess
    if the need is to restart a new instance of the database, free up log
    table space or in test environments.

    Parameters
    ----------
    - table: str
        (datasource, connector, dst, term_group, term_category, term,
        prefix,  wordterm, termmap, wordmap, workflow, logs)

    If inform table="all", the function will truncate all table on GE database.
    The other tables of the IGEM system will be maintained.

    Return
    ------
    Boolean: (TRUE if the process occurred without errors and FALSE if had
    some errors).

    Examples
    --------
    >>> from igem.ge import db
    >>> db.truncate_table(
            table='datasource'
            )
    """

    if table == "":
        print("Inform the table")
        return False

    v_table = table.lower()

    if v_table == "all":
        TermMap.truncate()
        WordMap.truncate()
        WordTerm.truncate()
        Term.truncate()
        TermCategory.truncate()
        TermGroup.truncate()
        Logs.truncate()
        WFControl.truncate()
        DSTColumn.truncate()
        PrefixOpc.truncate()
        Connector.truncate()
        Datasource.truncate()
        print("All tables deleted")
        return True

    elif v_table == "termmap":
        TermMap.truncate()
        print("TermMap table deleted")
        return True

    elif v_table == "wordmap":
        WordMap.truncate()
        print("WordMap table deleted")
        return True

    elif v_table == "wordterm":
        WordTerm.truncate()
        print("WordTerm table deleted")
        return True

    elif v_table == "term":
        Term.truncate()
        print("Term table deleted")
        return True

    elif v_table == "term_category":
        TermCategory.truncate()
        print("TermCategory table deleted")
        return True

    elif v_table == "term_group":
        TermGroup.truncate()
        print("TermGroup table deleted")
        return True

    elif v_table == "logs":
        Logs.truncate()
        print("Logs table deleted")
        return True

    elif v_table == "workflow":
        WFControl.truncate()
        print("WorkFlow table deleted")
        return True

    elif v_table == "dst":
        DSTColumn.truncate()
        print("Ds Column table deleted")
        return True

    elif v_table == "prefix":
        PrefixOpc.truncate()
        print("Prefix table deleted")
        return True

    elif v_table == "connector":
        Connector.truncate()
        print("Connector table deleted")
        return True

    elif v_table == "datasource":
        Datasource.truncate()
        print("Datasource table deleted")
        return True

    else:
        print("non-existent table")
        # print('datasource | connector | dst | workflow | term | term_category | term_group | prefix | wordterm | termmap | wordmap') # noqa E501
        return False
