import sys
import time

import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Sum

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import Connector, TermMap, WFControl, WordMap
except:  # noqa E722
    raise


def reduce(connector="all", chunck=1000000) -> bool:
    """
    Consult the IGEM system manual for information on the ETL mechanism and
    the REDUCE process.
    """

    v_time_proces = time.time()
    v_chunk = chunck
    v_opt_ds = connector.lower()

    print("Start: Process to reduce Terms combiantion on GE.db")

    if v_opt_ds == "all":
        v_where_cs = {"update_ds": True}
    else:
        v_where_cs = {"update_ds": True, "connector": v_opt_ds}
    try:
        qs_queryset = Connector.objects.filter(**v_where_cs)
    except ObjectDoesNotExist:
        print("  Connectors not found or disabled")
        sys.exit(2)
    if not qs_queryset:
        print("  Connectors not found or disabled")
        sys.exit(2)

    # Start process Connector
    for qs in qs_queryset:
        print(
            "  Start: Run datasource {0} on Connector {1}".format(
                qs.datasource, qs.connector
            )
        )  # noqa E501
        v_time_ds = time.time()

        # Check Workflow
        try:
            qs_wfc = WFControl.objects.get(
                connector_id=qs.id,
                chk_collect=True,
                chk_prepare=True,
                chk_map=True,
                chk_reduce=False,
            )
        except ObjectDoesNotExist:
            print("    Connector without workflow to process")
            continue

        # Here, the WordMap of the Records is read with both Keyge fields assigned and in an aggregated form. # noqa E501
        DFR = pd.DataFrame(
            WordMap.objects.values("connector_id", "term_1_id", "term_2_id")
            .filter(  # noqa E501
                connector_id=qs.id, term_1_id__isnull=False, term_2_id__isnull=False
            )
            .annotate(qtd_links=Sum("qtd_links")),
            columns=["connector_id", "term_1_id", "term_2_id", "qtd_links"],
        )  # noqa E501
        # .exclude(keyge1_id__isnull=True, keyge2_id__isnull=True, qtd_links=True) # noqa E501

        DFR = DFR.fillna(0)
        DFR.term_1_id = DFR.term_1_id.astype(int)
        DFR.term_2_id = DFR.term_2_id.astype(int)

        v_size = len(DFR.index)

        print(
            "    {0} records loaded from RoadMap will be aggregated".format(v_size)
        )  # noqa E501

        if not DFR.empty:
            v_lower = 0
            v_upper = v_chunk

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
            print(
                "    No data from {0} to update TermMap table".format(qs.connector)
            )  # noqa E501

        # Update WorkFlow Control Process
        qs_wfc.chk_reduce = True
        qs_wfc.save()

        print(
            "    Connector loaded in {0} seconds".format(int(time.time() - v_time_ds))
        )  # noqa E501

    print(
        "End of process in {0} seconds".format(int(time.time() - v_time_proces))
    )  # noqa E501

    return "success"
