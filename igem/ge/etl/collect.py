import os
import sys
import time
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
except:  # noqa E722
    raise


def collect(datasource="all", connector="all") -> bool:
    """
    Consult the IGEM system manual for information on the ETL mechanism and
    the COLLECT process.
    """

    v_path_file = str(settings.BASE_DIR) + "/psa/"

    def splitext_(path):
        if len(path.split(".")) > 2:
            return path.split(".")[0], ".".join(path.split(".")[-2:])
        return splitext(path)

    v_time_process = time.time()

    v_opt_ds = connector.lower()

    print("Start: Process to collect external datasources")  # noqa E501

    if v_opt_ds == "all":
        v_where_cs = {"update_ds": True}
    else:
        v_where_cs = {"update_ds": True, "connector": v_opt_ds}
    try:
        qs_queryset = Connector.objects.filter(**v_where_cs)
    except ObjectDoesNotExist:
        print("  connectors not found or disabled")
        sys.exit(2)
    if not qs_queryset:
        print("  connectors not found or disabled")
        sys.exit(2)

    for qs in qs_queryset:
        print(
            "  Start: Run datasource {0} on connector {1}".format(
                qs.datasource, qs.connector
            )
        )  # noqa E501
        v_time_ds = time.time()
        # Variables
        v_dir = v_path_file + str(qs.datasource) + "/" + qs.connector
        v_file_url = qs.source_path
        v_source_file = v_dir + "/" + qs.source_file_name
        v_target_file = v_dir + "/" + qs.target_file_name

        # Create folder to host file download
        if not os.path.isdir(v_dir):
            os.makedirs(v_dir)
            print("   Folder created to host the files in ", v_dir)

        # Get file source version from ETAG
        try:
            # v_version = str(requests.get(v_file_url, stream=True).headers["etag"])  # noqa E501
            v_version = requests.head(v_file_url).headers["Content-Length"]  # noqa E501
        except:  # noqa E5722
            print(
                "    Could not find the version of the file. Check content-length attr"  # noqa E501
            )  # noqa E501
        # Get WorkFlow Control
        try:
            qs_wfc = WFControl.objects.get(connector_id=qs.id)
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
            )
            qs_control.save()
            qs_wfc = WFControl.objects.get(connector_id=qs.id)

        # Check is new version before download
        if qs_wfc.source_file_version == v_version:
            print(
                "    Version already loaded in: {0}".format(
                    str(qs_wfc.last_update)[0:19]
                )
            )  # noqa E501
            continue

        # New file version, start download
        else:
            if os.path.exists(v_target_file):
                os.remove(v_target_file)
            if os.path.exists(v_source_file):
                os.remove(v_source_file)

            print("    Download start")  # noqa E501

            r = requests.get(v_file_url, stream=True)
            with open(v_source_file, "wb") as f:
                # total_length = int(r.headers.get('content-length'))
                # for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1):  # noqa E501
                for chunk in r.iter_content(chunk_size=1000000):
                    if chunk:
                        f.write(chunk)
                        f.flush()

            # Update LOG table if new version
            v_size = str(os.stat(v_source_file).st_size)
            # Create a LOG setting control (optional to log control)
            print("    Download finish")  # noqa E501
            # Unzip source file
            if qs.source_compact:
                try:
                    print("    Unzip start")  # noqa E501
                    patoolib.extract_archive(
                        str(v_source_file), outdir=str(v_dir), verbosity=-1
                    )  # noqa E501
                    os.remove(v_source_file)
                except:  # noqa E722
                    print("    Failed to unzip file")  # noqa E501
                    continue

            # XML files to CSV
            # This point is critical for memore consume
            file_name, ext = splitext(v_target_file)
            if str(qs.source_file_format).lower() == "xml":
                try:
                    v_src = str(file_name + ".xml")
                    DF = pd.read_xml(v_src)
                    v_csv = str(v_target_file)
                    DF.to_csv(v_csv, index=False)
                    os.remove(v_src)
                except:  # noqa E722
                    print("    Failed to convert XML to CSV")  # noqa E501
            # Check if target file is ok
            if not os.path.exists(v_target_file):
                print("    Failed to read file")  # noqa E501
                print(
                    "       Possible cause: check if the names of the source and destination files are correct in the connector table"  # noqa E501
                )  # noqa E501
                qs_wfc.source_file_version = "ERROR"
                qs_wfc.last_update = timezone.now()
                qs_wfc.save()
                for i in os.listdir(v_dir):
                    os.remove(v_dir + "/" + i)
                continue
            # # XML files to CSV
            # # This point is critical for memore consume
            # Update WorkFlow Control table:
            print("    Update workflow control")  # noqa E501
            qs_wfc.source_file_version = v_version
            qs_wfc.source_file_size = v_size
            qs_wfc.target_file_size = str(os.stat(v_target_file).st_size)  # noqa E501
            qs_wfc.last_update = timezone.now()
            qs_wfc.chk_collect = True
            qs_wfc.chk_prepare = False
            qs_wfc.chk_map = False
            qs_wfc.chk_reduce = False
            qs_wfc.save()

            print(
                "    connector loaded in {0} seconds".format(
                    int(time.time() - v_time_ds)
                )
            )  # noqa E501

    print(
        "End of process in {0} seconds".format(
            int(time.time() - v_time_process)
        )  # noqa E501
    )  # noqa E501

    return True
