import os
import sys
import time
from datetime import datetime
from os.path import splitext

import pandas as pd
import patoolib
import requests
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.utils import timezone

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import Connector, WFControl  # noqa E402
    from ge.utils import logger, start_logger
except:  # noqa E722
    raise


def collect(connector="all") -> bool:
    """
    Consult the IGEM system manual for information on the ETL mechanism and
    the COLLECT process.
    """

    # Global Variables
    log_file = __name__
    v_path_file = str(settings.BASE_DIR) + "/psa/"
    v_time_process = time.time()
    v_conn = connector.lower()

    log = start_logger(log_file)

    logger(log, "s", "Start of COLLECT process")

    # def splitext_(path):
    #     if len(path.split(".")) > 2:
    #         return path.split(".")[0], ".".join(path.split(".")[-2:])
    #     return splitext(path)

    # Get Connector parameters
    if v_conn == "all":
        v_where_cs = {"update_ds": True}
    else:
        v_where_cs = {"update_ds": True, "connector": v_conn}
    try:
        qs_queryset = Connector.objects.filter(**v_where_cs)
    # Check Queryset twice
    except ObjectDoesNotExist:
        logger(log, "e", "Connectors not found or disabled, ObjectDoesNotExist") # noqa E501
        # Stops this process and return False
        return False
    if not qs_queryset:
        logger(log, "w", "Connectors not found or disabled, queryset null")

        # Stops this process and return False
        return False

    # Process each connector in turn
    for qs in qs_queryset:
        logger(log, "s", f"{qs.connector}: Start Collector Process")

        # QS Variables
        v_time_ds = time.time()
        v_dir = v_path_file + str(qs.datasource) + "/" + qs.connector
        v_file_url = qs.source_path
        v_source_file = v_dir + "/" + qs.source_file_name
        v_target_file = v_dir + "/" + qs.target_file_name

        # Create folder to host file download
        if not os.path.isdir(v_dir):
            os.makedirs(v_dir)
            logger(log, "s", f"Folder created to host the files in {v_dir}")

        # Get file source header to check new versions
        response = requests.head(v_file_url, allow_redirects=True)

        # Check response status
        if response.status_code != 200:
            logger(log, "e", f"{qs.connector}: Expected a 200 code when querying the file header, but returned: {response.status_code}")  # noqa E501
            # Stops this process and starts the next connector
            continue

        # Get header data to version control
        v_date = ""
        v_etag = ""
        v_length = ""
        if "Last-Modified" in response.headers:
            date_str = response.headers["Last-Modified"]
            date_obj = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")
            v_date = date_obj.strftime("%Y%m%d")
        if "ETag" in response.headers:
            v_etag = response.headers["ETag"]
        if "Content-Length" in response.headers:
            v_length = response.headers["Content-Length"]
        # Create version control syntax
        v_version = (
            "ds:"
            + str(qs.datasource)
            + "|conn:"
            + str(qs.connector)
            + "|etag:"
            + str(v_etag)
            + "|lenght:"
            + str(v_length)
            + "|modified:"
            + str(v_date)
        )
        logger(log, "s", f"{qs.connector}: The current version of the data source: {v_version}")  # noqa E501

        # Get WorkFlow Control
        try:
            qs_wfc_all = WFControl.objects.filter(
                connector_id=qs.id, status__in=["c", "w"]
            )
            v_first_load = False
        # Create new record if is New or only has Overwrite
        except ObjectDoesNotExist:
            qs_control = WFControl(
                connector_id=qs.id,
                last_update=timezone.now(),
                source_file_version=0,
                source_file_size=0,
                target_file_size=0,
                chk_collect=False,
                chk_prepare=False,
                chk_map=False,
                chk_reduce=False,
                status="c",
            )
            qs_control.save()
            qs_wfc_all = WFControl.objects.get(connector_id=qs.id)
            v_first_load = True
            logger(log, "s", f"{qs.connector}: Create workflow record")
        #
        if not qs_wfc_all:
            qs_control = WFControl(
                connector_id=qs.id,
                last_update=timezone.now(),
                source_file_version=0,
                source_file_size=0,
                target_file_size=0,
                chk_collect=False,
                chk_prepare=False,
                chk_map=False,
                chk_reduce=False,
                status="c",
            )
            qs_control.save()
            qs_wfc_all = WFControl.objects.filter(
                connector_id=qs.id, status__in=["c", "w"]
            )
            v_first_load = True
            logger(log, "s", f"{qs.connector}: Create workflow record")

        # filter te Current Version
        # Used get to load (only one record = c)
        qs_wfc = qs_wfc_all.filter(status__in=["c", "w"]).first()

        # Check is new version before download
        if qs_wfc.source_file_version == v_version and qs_wfc.chk_collect:
            logger(log, "w", f"{qs.connector}: Same version of the IGEM pass. The connector update was canceled")  # noqa E501
            # Stops this process and starts the next connector
            continue

        # NEW VERSION
        # Start Collect Process to new Version

        # Change the current version to Overwrite
        if not v_first_load:
            # Set actually instance to Overwrite
            qs_wfc.status = "o"
            qs_wfc.save()
            # Create a new instance as WorkProcess
            qs_wfc.pk = None
            qs_wfc.status = "w"
            qs_wfc.last_update = timezone.now()
        else:
            # First connector load
            qs_wfc.status = "w"

        # Start download
        if os.path.exists(v_target_file):
            os.remove(v_target_file)
            logger(log, "s", f"{qs.connector}: Target file deleted {v_target_file}")  # noqa E501

        if os.path.exists(v_source_file):
            os.remove(v_source_file)
            logger(log, "s", f"{qs.connector}: Source file deleted {v_source_file}")  # noqa E501

        r = requests.get(v_file_url, stream=True)
        with open(v_source_file, "wb") as f:
            # total_length = int(r.headers.get('content-length'))
            # for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1):  # noqa E501
            # TODO: Create a Variable System to control the Chunk Size.
            for chunk in r.iter_content(chunk_size=1000000):
                if chunk:
                    f.write(chunk)
                    f.flush()

        # Update LOG table if new version
        v_size = str(os.stat(v_source_file).st_size)
        logger(log, "s", f"{qs.connector}: Finished the file download")  # noqa E501

        # Unzip source file
        if qs.source_compact:
            try:
                print("Unzip start")  # noqa E501
                patoolib.extract_archive(
                    str(v_source_file), outdir=str(v_dir), verbosity=-1
                )  # noqa E501
                os.remove(v_source_file)
            except:  # noqa E722
                logger(log, "e", f"{qs.connector}: Failed to UNZIP file")
                # Stops this process and starts the next connector
                continue

        # XML files to CSV
        # TODO: This point is critical for memore consume
        file_name, ext = splitext(v_target_file)
        if str(qs.source_file_format).lower() == "xml":
            try:
                v_src = str(file_name + ".xml")
                DF = pd.read_xml(v_src)
                v_csv = str(v_target_file)
                DF.to_csv(v_csv, index=False)
                os.remove(v_src)
                logger(log, "s", f"{qs.connector}: XML file converted to standard IGEM")  # noqa E501
            except:  # noqa E722
                logger(log, "e", f"{qs.connector}: Failed to convert XML to CSV") # noqa E501
                # Stops this process and starts the next connector
                continue

        # Check if Target File is ok
        if not os.path.exists(v_target_file):
            qs_wfc.source_file_version = "ERROR SYSTEM"
            qs_wfc.last_update = timezone.now()
            qs_wfc.status = "w"
            qs_wfc.chk_collect = False
            qs_wfc.chk_prepare = False
            qs_wfc.chk_map = False
            qs_wfc.chk_reduce = False
            qs_wfc.save()
            for i in os.listdir(v_dir):
                os.remove(v_dir + "/" + i)
            logger(log, "e", f"{qs.connector}: Failed to Read IGEM Standard File. Check if the names of the source and destination files are correct in the connector")  # noqa E501
            # Stops this process and starts the next connector
            continue

        # Update WorkFlow Control table:
        # Get number of records and time process
        v_count = 0
        with open(v_target_file) as fp:
            for v_count, _ in enumerate(fp, 1):
                pass
        v_time = int(time.time() - v_time_ds)
        # Update instance variables
        qs_wfc.source_file_version = v_version
        qs_wfc.source_file_size = v_size
        qs_wfc.target_file_size = str(os.stat(v_target_file).st_size)  # noqa E501
        qs_wfc.last_update = timezone.now()
        qs_wfc.chk_collect = True
        qs_wfc.chk_prepare = False
        qs_wfc.chk_map = False
        qs_wfc.chk_reduce = False
        qs_wfc.status = "w"
        qs_wfc.time_collect = v_time
        qs_wfc.row_collect = v_count
        qs_wfc.save()
        logger(log, "s", f"{qs.connector}: COLLECT process was completed successfully in {v_time} seconds with {v_count} records")  # noqa E501

        # Check WFControl Table Integrity
        qs_wfc_audit = WFControl.objects.filter(
            connector_id=qs.id, status__in=["c", "w"]
        ).exclude(pk=qs_wfc.pk)
        # if has row, change to Overwrite
        if qs_wfc_audit:
            for qsa in qs_wfc_audit:
                qsa.status = "o"
                qsa.save()
                logger(log, "w", f"{qs.connector}: {qsa.pk} WFControl ID alter to Overwrite manually")  # noqa E501

    # End of COLLECT process
    # Log-Message
    v_time = int(time.time() - v_time_process)
    logger(log, "s", f"COLLECT process was completed {v_time} seconds")  # noqa E501

    return True
