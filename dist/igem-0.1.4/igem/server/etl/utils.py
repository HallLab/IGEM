import os

from django.core.exceptions import ObjectDoesNotExist
from django.utils import timezone
from ge.models import Connector, WFControl  # noqa E402
from ge.utils import logger


def get_connectors(v_conn, log):
    # Get Connector parameters
    if v_conn == "all":
        v_where_cs = {"update_ds": True}
    else:
        v_where_cs = {"update_ds": True, "connector": v_conn}
    try:
        qs_queryset = Connector.objects.filter(**v_where_cs)
    # Check Queryset twice
    except ObjectDoesNotExist:
        logger(log, "e", "Connectors not found or disabled") # noqa E501
        # Raise an error with a custom message
        raise ValueError("Connector not found or disabled")
    if not qs_queryset:
        logger(log, "w", "Connectors not found or disabled, queryset null")
        # TODO: Add raise error?
    return qs_queryset


# Get data from WFControl Table
def get_workflow(qs, process, log):

    if process == "collect":
        v_fields = {
            "connector_id": qs.id,
            "last_update": timezone.now(),
            "source_file_version": 0,
            "source_file_size": 0,
            "target_file_size": 0,
            "chk_collect": False,
            "chk_prepare": False,
            "chk_map": False,
            "chk_reduce": False,
            "status": "c",
            }

    # Get WorkFlow Control
    try:
        qs_wfc_all = WFControl.objects.filter(
            connector_id=qs.id, status__in=["c", "w"]
        )
        v_first_load = False
    # Create new record if is New or only has Overwrite
    except ObjectDoesNotExist:
        qs_control = WFControl(**v_fields)
        qs_control.save()
        qs_wfc_all = WFControl.objects.get(connector_id=qs.id)
        v_first_load = True
        logger(log, "s", f"{qs.connector}: Create workflow record")
    # Force create new workflow control record
    if not qs_wfc_all:
        qs_control = WFControl(**v_fields)
        qs_control.save()
        qs_wfc_all = WFControl.objects.filter(
            connector_id=qs.id, status__in=["c", "w"]
        )
        v_first_load = True
        logger(log, "s", f"{qs.connector}: Create workflow record")

    return qs_wfc_all, v_first_load


def audit_workprocess(qs, qs_wfc, log):
    qs_wfc_audit = WFControl.objects.filter(
            connector_id=qs.id, status__in=["c", "w"]
        ).exclude(pk=qs_wfc.pk)
    # if has row, change to Overwrite
    if qs_wfc_audit:
        for qsa in qs_wfc_audit:
            qsa.status = "o"
            qsa.save()
            logger(log, "w", f"{qs.connector}: {qsa.pk} WFControl ID alter to Overwrite manually")  # noqa E501
    return True


def clean_folder(folder_path, log):
    try:
        # Get a list of all files in the folder
        file_list = os.listdir(folder_path)

        # Loop through the file list and remove each file
        for file_name in file_list:
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
            else:
                print(f"Skipping: {file_path} (not a file)")
    except Exception as e:
        logger(log, "e", f"An error occurred while cleaning the folder: {e}")  # noqa E501
