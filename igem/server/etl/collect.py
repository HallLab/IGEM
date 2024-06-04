import os
import shutil
import sys
import time
from datetime import datetime

import requests
from django.conf import settings
from django.utils import timezone

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.utils import logger, start_logger
    from server.etl import extractors, utils
except Exception as e:
    raise RuntimeError(
        "An unexpected error occurred. Original error: " + str(e)
        )


def collect(connector="all") -> bool:
    """
    Consult the IGEM system manual for information on the ETL mechanism and
    the COLLECT process.
    """

    # Global Variables
    log_file = __name__
    v_path_file = str(settings.BASE_DIR)
    v_time_process = time.time()
    v_conn = connector.lower()
    v_process = "collect"

    # Start Log monitor
    log = start_logger(log_file)
    logger(log, "s", "Start of COLLECT process")

    # Get Connectors
    qs_queryset = utils.get_connectors(v_conn, log)

    # Process each connector
    for qs in qs_queryset:
        logger(log, "s", f"{qs.connector}: Start Collector Process")

        # QS Variables
        v_time_ds = time.time()
        v_dir = v_path_file + "/psa/" + str(qs.datasource) + "/" + qs.connector
        v_file_url = qs.source_path
        v_source_file = v_dir + "/" + qs.source_file_name
        v_target_file = v_dir + "/" + qs.target_file_name

        # Create folder to host file download
        if not os.path.isdir(v_dir):
            os.makedirs(v_dir)
            logger(log, "s", f"Folder created to host the files in {v_dir}")

        # VERSION CONTROL FASES #
        # ----------------------#
        v_date = ""
        v_etag = ""
        v_length = ""
        if v_file_url.startswith("http"):
            # Get file source header to check new versions
            response = requests.head(v_file_url, allow_redirects=True)
            # Check response status
            if response.status_code != 200:
                v_error = f"{qs.connector}: Expected a 200 code when querying the file header, but returned: {response.status_code}" # noqa E501
                logger(log, "e", v_error)
                continue
            if "Last-Modified" in response.headers:
                date_str = response.headers["Last-Modified"]
                date_obj = datetime.strptime(
                    date_str, "%a, %d %b %Y %H:%M:%S %Z")
                v_date = date_obj.strftime("%Y%m%d")
            if "ETag" in response.headers:
                v_etag = response.headers["ETag"]
            if "Content-Length" in response.headers:
                v_length = response.headers["Content-Length"]
        else:
            v_length = str(os.path.getsize(v_file_url))
            v_date_stamp = os.path.getmtime(v_file_url)
            v_date = datetime.fromtimestamp(
                v_date_stamp).strftime('%Y-%m-%d %H:%M:%S')
        # Create version control syntax
        v_version = (
            "ds:"
            + str(qs.datasource)
            + "__conn:"
            + str(qs.connector)
            + "__etag:"
            + str(v_etag)
            + "__lenght:"
            + str(v_length)
            + "__modified:"
            + str(v_date)
        )
        logger(log, "s", f"{qs.connector}: The current version of the data source: {v_version}")  # noqa E501

        # Get WorkFlow Control
        qs_wfc_all, v_first_load = utils.get_workflow(qs, v_process, log)

        # filter te Current Version
        # Used get to load (only one record = c)
        qs_wfc = qs_wfc_all.filter(status__in=["c", "w"]).first()

        # Check is new version before load
        if qs_wfc.source_file_version == v_version and qs_wfc.chk_collect:
            logger(log, "w", f"{qs.connector}: Same version of the IGEM pass. The connector update was canceled")  # noqa E501
            continue

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
            qs_wfc.status = "w"

        # Clean Cache Files
        utils.clean_folder(v_dir, log)

        # GET FILE FASES #
        # ---------------#
        # connector.source_web from WEB = True // Local Files = False
        if v_file_url.startswith("http"):
            try:
                r = requests.get(v_file_url, stream=True)
                if r.status_code == 200:
                    with open(v_source_file, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1000000):
                            if chunk:
                                f.write(chunk)
                                f.flush()
                else:
                    logger(log, "e", f"{qs.connector}: An error occurred while dowload the file")  # noqa E501
                    continue
            except Exception as e:
                logger(log, "e", f"{qs.connector}: An error occurred while dowload the file: {str(e)}")  # noqa E501
                continue
        else:
            if v_file_url != v_source_file:
                try:
                    shutil.copy(v_file_url, v_source_file)
                    logger(log, "s", f"{qs.connector}: File '{v_source_file}' copied to '{v_target_file}' successfully.")  # noqa E501
                except Exception as e:
                    logger(log, "e", f"{qs.connector}: An error occurred while copying the file: {str(e)}")  # noqa E501
                    continue

        # Update LOG table if new version
        v_size = str(os.stat(v_source_file).st_size)
        logger(log, "s", f"{qs.connector}: Finished the file download")  # noqa E501

        # TRANSFORMATION FASES #
        # ---------------------#
        # Transformation: Unzip source file
        if qs.source_compact:
            v_zip = extractors.extractor_zip(qs, v_dir, v_source_file, log)
            if not v_zip:
                continue
        # Transformation: XML files to CSV
        if str(qs.source_file_format).lower() == "xml":
            v_xml = extractors.extractor_xml(qs, v_target_file, log)
            if not v_xml:
                continue
        # Transformation: KEGG files to CSV
        if str(qs.datasource).lower() == "kegg":
            v_kegg = extractors.extractor_kegg(
                qs,
                v_source_file,
                v_target_file,
                log
                )
            if not v_kegg:
                continue

        # WORKFLOW CONTROL FASES #
        # -----------------------#
        # WorkFlow Control: Check if Target File is ok
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
        # WorkFlow Control: Update WorkFlow Control table
        v_count = 0
        with open(v_target_file) as fp:
            for v_count, _ in enumerate(fp, 1):
                pass
        v_time = int(time.time() - v_time_ds)
        str(os.stat(v_target_file).st_size)
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
        # WorkFlow Control: Check WFControl Table Integrity
        utils.audit_workprocess(qs, qs_wfc, log)

    # NEXT CONNECTOR

    # END OF COLLECT PROCESS
    v_time = int(time.time() - v_time_process)
    logger(log, "s", f"COLLECT process was completed {v_time} seconds")  # noqa E501
    return True
