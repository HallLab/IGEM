import sys
import time

import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Sum
from django.utils import timezone

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import Connector, TermMap, WFControl, WordMap
    from ge.utils import logger, start_logger
except:  # noqa E722
    raise


def reduce(connector="all", chunck=1000000) -> bool:
    """
    Consult the IGEM system manual for information on the ETL mechanism and
    the REDUCE process.
    """

    # Global Variables
    log_file = __name__
    v_time_proces = time.time()
    v_chunk = chunck
    v_opt_ds = connector.lower()

    log = start_logger(log_file)
    logger(log, "s", "Start of REDUCE process")

    # Get Connector parameters
    if v_opt_ds == "all":
        v_where_cs = {"update_ds": True}
    else:
        v_where_cs = {"update_ds": True, "connector": v_opt_ds}
    try:
        qs_queryset = Connector.objects.filter(**v_where_cs)
    except ObjectDoesNotExist:
        logger(log, "e", "Connectors not found or disabled, ObjectDoesNotExist" )  # noqa E501
        # Stops this process and return False
        return False
    if not qs_queryset:
        logger(log, "w", "Connectors not found or disabled, queryset in null")  # noqa E501
        # Stops this process and return False
        return False

    # Start process Connector
    for qs in qs_queryset:
        logger(log, "s", f"{qs.connector}: Start REDUCE Process")  # noqa E501

        # QS Variables
        v_time_ds = time.time()

        # Get WorkFlow Control
        try:
            qs_wfc = WFControl.objects.get(
                connector_id=qs.id,
                chk_collect=True,
                chk_prepare=True,
                chk_map=True,
                chk_reduce=False,
                status__in=["w"],
            )
        except ObjectDoesNotExist:
            logger(log, "w", f"{qs.connector}: Connector without workflow to process")  # noqa E501
            # Stops this process and starts the next connector
            continue

        # Here, the WordMap of the Records is read with both Term fields assigned and in an aggregated form. # noqa E501
        DFR = pd.DataFrame(
            WordMap.objects.values("connector_id", "term_1_id", "term_2_id")
            .filter(  # noqa E501
                connector_id=qs.id,
                term_1_id__isnull=False,
                term_2_id__isnull=False,  # noqa E501
            )
            .annotate(qtd_links=Sum("qtd_links")),
            columns=["connector_id", "term_1_id", "term_2_id", "qtd_links"],
        )  # noqa E501
        # .exclude(keyge1_id__isnull=True, keyge2_id__isnull=True, qtd_links=True) # noqa E501

        DFR = DFR.fillna(0)
        DFR.term_1_id = DFR.term_1_id.astype(int)
        DFR.term_2_id = DFR.term_2_id.astype(int)

        v_size = len(DFR.index)

        logger(log, "s", f"{qs.connector}: {v_size} records loaded from RoadMap will be aggregated")  # noqa E501

        if not DFR.empty:
            v_lower = 0
            v_upper = v_chunk

            # TODO: Make BKP before delete this data
            TermMap.objects.filter(connector_id=qs.id).delete()

            while v_upper <= (v_size + v_chunk):
                DFRC = DFR[v_lower:v_upper]

                model_TermMap = [
                    TermMap(
                        ckey=str(
                            str(record.connector_id) + "-" + str(record.Index)
                        ),  # noqa E501
                        connector_id=record.connector_id,
                        term_1_id=record.term_1_id,
                        term_2_id=record.term_2_id,
                        qtd_links=record.qtd_links,
                    )
                    for record in DFRC.itertuples()
                ]

                TermMap.objects.bulk_create(model_TermMap)

                # print('    Writing records from {0} to {1} on TermMap'.format(v_lower, v_upper)))  # noqa E501
                v_lower += v_chunk
                v_upper += v_chunk

        else:
              logger(log, "w", f"{qs.connector}: No data to update TermMap Table")  # noqa E501

        # Update WorkFlow Control table:
        # Get number of records and time process
        v_time = int(time.time() - v_time_ds)
        # Update instance variables
        qs_wfc.last_update = timezone.now()
        qs_wfc.chk_reduce = True
        qs_wfc.status = "c"
        qs_wfc.time_reduce = v_time
        qs_wfc.row_reduce = v_size
        qs_wfc.save()
        logger(log, "s", f"{qs.connector}: Map process was completed successfully in {v_time} seconds with {v_size} records")  # noqa E501

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

    # End of REDUCE process
    # Log-Message
    v_time = int(time.time() - v_time_proces)
    logger(log, "s", f"REDUCE process was completed in {v_time} seconds")  # noqa E501

    return True
