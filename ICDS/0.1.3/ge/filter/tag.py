import os
import sys

import pandas as pd
from django.conf import settings

# Import required Django models
try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import TermMap, WFControl
except Exception as e:
    print(e)
    raise


def create_tag(connectors):
    """
    Function to create a TAG with Current connector in TermMap.
    The IDs generated in the TAG are the WFControl table IDs with Current status. # noqa E501
    Args:
        connectors (list): List of connector names.
    Returns:
        str: Generated TAG string.
    """
    if not connectors:
        return "GE.db-TAG-E:ERROR_no_conn_parameters"

    # Get last IDs with status Current from WFControl
    qs_ids = WFControl.objects.filter(
        connector_id__connector__in=connectors,
        status="c"
    ).values_list("id", flat=True)

    if not qs_ids:
        return "GE.db-TAG-E:ERROR_wfcontrol_empty"

    tag = "-".join(str(qs_id) for qs_id in qs_ids)
    return f"GE.db-TAG:{tag}"


def get_tag(tag):
    """
    Function to retrieve WFControl data based on a TAG.
    Args:
        tag (str): TAG string.
    Returns:
        pd.DataFrame: DataFrame with WFControl data.
    """
    if not tag.startswith("GE.db-TAG"):
        print("Invalid TAG.")
        return None

    tag_ids = tag.split(":")[1].split("-")
    if not all(tag_id.isdigit() for tag_id in tag_ids):
        print("Invalid TAG.")
        return None

    wfc_data = WFControl.objects.filter(pk__in=tag_ids).values(
        "id",
        "connector_id",
        "connector_id__connector",
        "last_update",
        "igem_version",
        "row_collect",
        "row_reduce",
        "source_file_version",
        "status",
    )
    df_wfc = pd.DataFrame.from_records(wfc_data)
    df_wfc.rename(
        columns={
            "id": "wfc id",
            "connector_id": "Connector ID",
            "connector_id__connector": "Connector",
            "last_update": "Last Update",
            "igem_version": "IGEM Version",
            "row_collect": "Source Rows",
            "row_reduce": "Relationships",
            "source_file_version": "Control Version",
            "status": "Status",
        },
        inplace=True,
    )
    return df_wfc


def get_tag_data(tag, path):
    """
    Function to save TermMap and WFControl data to CSV files based on a TAG.
    Args:
        tag (str): TAG string.
        path (str): Path to save the files.
    Returns:
        bool: True if the files were successfully created, False otherwise.
    """
    if not os.path.isdir(path):
        print("Invalid path.")
        return False

    db_path = os.path.join(path, f"data_{tag}.csv.gz")
    tag_path = os.path.join(path, f"tag_{tag}.csv.gz")

    tag_data = get_tag(tag)
    if tag_data is None:
        return False

    connector_ids = tag_data["Connector ID"].unique()
    term_map_data = TermMap.objects.filter(connector_id__in=connector_ids).values(  # noqa E501
        "ckey",
        "connector_id",
        "term_1_id",
        "term_1_id__term",
        "term_2_id",
        "term_2_id__term",
        "qtd_links",
    )
    df_base = pd.DataFrame.from_records(term_map_data)

    try:
        df_base.to_csv(db_path, index=False, compression="gzip")
        tag_data.to_csv(tag_path, index=False, compression="gzip")
        print("Files created in", path, "with the TermMap data referring to the informed TAG.")  # noqa E501
        return True
    except Exception as e:
        print("Error saving files:", str(e))
        return False

# OLD version
# import os
# import sys

# import pandas as pd
# from django.conf import settings

# try:
#     x = str(settings.BASE_DIR)
#     sys.path.append(x)
#     from ge.models import TermMap, WFControl
# except Exception as e:
#     print(e)
#     raise


# # Function to create a TAG with Current connector in TermMap.
# # The IDs generated in the TAG are the WFControl table IDs with Current status.  # noqa E501
# def create_tag(**kwargs):
#     # Connectors Parameter will receive by Connector Name
#     ls_connectors = kwargs.get("connectors", [])

#     if not ls_connectors:
#         v_tag = "GE.db-TAG-E:ERROR_no_conn_parameters"
#         return v_tag

#     # Get from WFControl, last ID update with status Current
#     qs_ids = WFControl.objects.filter(
#         connector_id__connector__in=ls_connectors,
#         status="c"
#         )
#     if not qs_ids:
#         v_tag = "GE.db-TAG-E:ERROR_wfcontrol_empty"
#         return v_tag

#     # Create a string Tag
#     v_tag = "GE.db-TAG:"
#     v_first = True
#     for qs in qs_ids:
#         if not v_first:
#             v_tag += str("-" + str(qs.id))
#             continue
#         v_tag += str(str(qs.id))
#         v_first = False

#     return v_tag


# def get_tag(*args):
#     """
#     Function to return the WFControl from a TAG
#     Need to pass a valid TAG string
#     """

#     if len(args) == 0:
#         # raise ValueError("At least one argument is required.")
#         print("One TAG is required.")
#         return None

#     tag = args[0]

#     # Split the string into two parts using ":" as the separator
#     ls_parts = tag.split(":")

#     # Error TAG
#     v_head = ls_parts[0]
#     if v_head == 'GE.db-TAG-E':
#         print("This is an error tag; it will not be possible to process the request.") # noqa E501
#         return None

#     # Invalid TAG
#     if v_head != 'GE.db-TAG':
#         print("Invalid TAG.")
#         return None

#     # Split the second part into two values using "-" as the separator
#     ls_wfc_id = ls_parts[1].split("-")

#     # Checks list integrity
#     if len(ls_wfc_id) == 0:
#         print("Invalid TAG.")
#         return None
#     check = all(element.isdigit() or isinstance(element, (int, float)) for element in ls_wfc_id) # noqa E501
#     if not check:
#         print("Invalid TAG.")
#         return None

#     # Queryset to Connector and Version information
#     df_wfc = pd.DataFrame(
#         WFControl.objects.filter(
#             pk__in=ls_wfc_id
#             ).values(
#                 "id",
#                 "connector_id",
#                 "connector_id__connector",
#                 "last_update",
#                 "igem_version",
#                 "row_collect",
#                 "row_reduce",
#                 "source_file_version",
#                 "status",
#                 )
#         )

#     df_wfc.rename(
#         columns={
#             "id": "wfc id",
#             "connector_id": "Connector ID",
#             "connector_id__connector": "Connector",
#             "last_update": "Last Update",
#             "igem_version": "IGEM Version",
#             "row_collect": "Source Rows",
#             "row_reduce": "Relationships",
#             "source_file_version":  "Control Version",
#             "status": "Status",
#         },
#         inplace=True,
#     )

#     return df_wfc


# def get_tag_data(*args):
#     """
#     Function to return the WFControl from a TAG
#     Need to pass a valid TAG string
#     """

#     if len(args) < 2:
#         # raise ValueError("At least one argument is required.")
#         print("One TAG and Path is required.")
#         return None

#     tag = args[0]
#     path = args[1]

#     # Create folder to host file download
#     if not os.path.isdir(path):
#         print("Path invalid.")
#         return False
#     v_path_db = path + "/data_" + tag + ".csv.gz"
#     v_path_tag = path + "/tag_" + tag + ".csv.gz"

#     # Get WFControl data
#     ls_tag = get_tag(tag)

#     # Get Connectors ID from WFControl
#     df_conn = ls_tag[['Connector ID']].copy()
#     df_conn.drop_duplicates(inplace=True)
#     ls_conn = df_conn.values.tolist()
#     ls_conn = [item for sublist in ls_conn for item in sublist]

#     # Get TermMap data
#     df_base = pd.DataFrame(TermMap.objects.filter(
#         connector_id__in=ls_conn).values(
#             'ckey',
#             'connector_id',
#             'term_1_id',
#             'term_1_id__term',
#             'term_2_id',
#             'term_2_id__term',
#             'qtd_links',
#         ))

#     # Save dataframes
#     df_base.to_csv(v_path_db, index=False, compression='gzip')
#     ls_tag.to_csv(v_path_tag, index=False, compression='gzip')
#     # base.to_csv('mydata.csv.gz', index=False, compression='gzip')
#     print("Files created in", path, "with the TermMap data referring to the informed TAG.") # noqa E501

#     return True
