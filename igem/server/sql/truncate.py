# TODO:  Create second function to mgm trucante and message
# TODO:  Paramenter to hide prints/message
# Work only on SQLite


import sys

from django.conf import settings
from django.db import connection

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    # from ge.models import (
    #     Connector,
    #     Datasource,
    #     DSTColumn,
    #     Logs,
    #     PrefixOpc,
    #     Term,
    #     TermCategory,
    #     TermGroup,
    #     TermMap,
    #     WFControl,
    #     WordMap,
    #     WordTerm,
    # )
except Exception as e:
    print(e)
    raise


# # Truncate Function
# def truncate_table(table_name):
#     try:
#         with connection.cursor() as cursor:
#             # Disable foreign key checks temporarily
#             cursor.execute('PRAGMA foreign_keys = OFF;')
#             # Truncate the table by deleting all records
#             cursor.execute(f"DELETE FROM {table_name};")
#             # Reset the primary key sequence for the table
#             cursor.execute(
#                 f"DELETE FROM sqlite_sequence WHERE name='{table_name}';"
#                 )
#             # Enable foreign key checks
#             cursor.execute('PRAGMA foreign_keys = ON;')
#             # Vacuum the database to reclaim unused space
#             cursor.execute("VACUUM;")
#         return True
#     except Exception as e:
#         print("ERROR:", e)
#         return False

def truncate_ge(table_name, vacuum):
    with connection.cursor() as cursor:
        cursor.execute(f"DELETE FROM {table_name};")
        if vacuum:
            cursor.execute("VACUUM;")
    return True


def truncate_table(table: str = "", vacuum=True) -> bool:
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
    v_vacuum = vacuum

    if v_table == "all":
        v_return = truncate_ge('ge_termmap', v_vacuum)
        v_return = truncate_ge('ge_wordmap', v_vacuum)
        v_return = truncate_ge('ge_wordterm', v_vacuum)
        v_return = truncate_ge('ge_term', v_vacuum)
        v_return = truncate_ge('ge_termcategory', v_vacuum)
        v_return = truncate_ge('ge_termgroup', v_vacuum)
        v_return = truncate_ge('ge_logs', v_vacuum)
        v_return = truncate_ge('ge_wfcontrol', v_vacuum)
        v_return = truncate_ge('ge_dstcolumn', v_vacuum)
        v_return = truncate_ge('ge_prefixopc', v_vacuum)
        v_return = truncate_ge('ge_connector', v_vacuum)
        v_return = truncate_ge('ge_datasource', v_vacuum)
        print("All tables deleted")
        if v_return:
            return True
        else:
            return False

    # -- This code works on Postgres DB
    # elif v_table == "termmap":
    #     TermMap.truncate()
    #     print("TermMap table deleted")
    #     return True

    # elif v_table == "wordmap":
    #     WordMap.truncate()
    #     print("WordMap table deleted")
    #     return True

    # elif v_table == "wordterm":
    #     WordTerm.truncate()
    #     print("WordTerm table deleted")
    #     return True

    # elif v_table == "term":
    #     Term.truncate()
    #     print("Term table deleted")
    #     return True

    # elif v_table == "term_category":
    #     TermCategory.truncate()
    #     print("TermCategory table deleted")
    #     return True

    # elif v_table == "term_group":
    #     TermGroup.truncate()
    #     print("TermGroup table deleted")
    #     return True

    # elif v_table == "logs":
    #     Logs.truncate()
    #     print("Logs table deleted")
    #     return True

    # elif v_table == "workflow":
    #     WFControl.truncate()
    #     print("WorkFlow table deleted")
    #     return True

    # elif v_table == "dst":
    #     DSTColumn.truncate()
    #     print("Ds Column table deleted")
    #     return True

    # elif v_table == "prefix":
    #     # PrefixOpc.truncate()
    #     # print("Prefix table deleted")
    #     with connection.cursor() as cursor:
    #         cursor.execute(f"DELETE FROM {'ge_logs'};")
    #         cursor.execute("VACUUM;")

    #     return True

    # elif v_table == "connector":
    #     Connector.truncate()
    #     print("Connector table deleted")
    #     return True

    # elif v_table == "datasource":
    #     Datasource.truncate()
    #     print("Datasource table deleted")
    #     return True

    else:
        print("non-existent table")
        # print('datasource | connector | dst | workflow | term | term_category | term_group | prefix | wordterm | termmap | wordmap') # noqa E501
        return False
