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
    from ge.models import Connector, Logs, TermMap, WFControl, WordMap
except:  # noqa E722
    raise


def reduce(connector="all", chunck=1000000) -> bool:
    """
    Consult the IGEM system manual for information on the ETL mechanism and
    the REDUCE process.
    """

    # Global Variables
    v_proc = "ETL/REDUCE"
    v_time_proces = time.time()
    v_chunk = chunck
    v_opt_ds = connector.lower()

    # Log-Message
    v_desc = "Start of REDUCE process"
    print(v_desc)
    Logs.objects.create(process=v_proc, description=v_desc, status="s")

    # Get Connector parameters
    if v_opt_ds == "all":
        v_where_cs = {"update_ds": True}
    else:
        v_where_cs = {"update_ds": True, "connector": v_opt_ds}
    try:
        qs_queryset = Connector.objects.filter(**v_where_cs)
    except ObjectDoesNotExist:
        # Log-Message
        v_desc = "Connectors not found or disabled, ObjectDoesNotExist"
        print(v_desc)
        Logs.objects.create(process=v_proc, description=v_desc, status="e")
        # Stops this process and return False
        return False
    if not qs_queryset:
        # Log-Message
        v_desc = "Connectors not found or disabled, queryset in null"
        print(v_desc)
        Logs.objects.create(process=v_proc, description=v_desc, status="w")
        # Stops this process and return False
        return False

    # Start process Connector
    for qs in qs_queryset:
        # Log-Message
        v_desc = f"{qs.connector}: Start REDUCE Process"
        print(v_desc)
        Logs.objects.create(process=v_proc, description=v_desc, status="s")

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
            # Log-Message
            v_desc = f"{qs.connector}: Connector without workflow to process."
            print(v_desc)
            Logs.objects.create(process=v_proc, description=v_desc, status="w")
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

        # Log-Message
        v_desc = f"{qs.connector}: {v_size} records loaded from RoadMap will be aggregated."  # noqa E501
        print(v_desc)
        Logs.objects.create(process=v_proc, description=v_desc, status="s")

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
            # Log-Message
            v_desc = f"{qs.connector}: No data to update TermMap Table."
            print(v_desc)
            Logs.objects.create(process=v_proc, description=v_desc, status="w")

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
        # Log-Message
        v_desc = f"{qs.connector}: Map process was completed successfully in {v_time} seconds with {v_size} records"  # noqa E501
        print(v_desc)
        Logs.objects.create(process=v_proc, description=v_desc, status="s")

        # Check WFControl Table Integrity
        qs_wfc_audit = WFControl.objects.filter(
            connector_id=qs.id, status__in=["c", "w"]
        ).exclude(pk=qs_wfc.pk)
        # if has row, change to Overwrite
        if qs_wfc_audit:
            for qsa in qs_wfc_audit:
                qsa.status = "o"
                qsa.save()
                # Log-Message
                v_desc = f"{qs.connector}: {qsa.pk} WFControl ID alter to Overwrite manually "  # noqa E501
                print(v_desc)
                Logs.objects.create(
                    process=v_proc, description=v_desc, status="w"
                )  # noqa E501

    # End of REDUCE process
    # Log-Message
    v_time = int(time.time() - v_time_proces)
    v_desc = f"REDUCE process was completed in {v_time} seconds"  # noqa E501
    print(v_desc)
    Logs.objects.create(process=v_proc, description=v_desc, status="s")

    return True
