import math
import os
import re
import sys
import time
from concurrent.futures import as_completed
from itertools import combinations

import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.utils import timezone
from django_thread import ThreadPoolExecutor

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import Connector, Term, WFControl, WordMap  # noqa E402
    from ge.utils import logger, start_logger
except:  # noqa E722
    raise


def chunkify(df: pd.DataFrame, chunk_size: int):
    start = 0
    length = df.shape[0]
    # If DF is smaller than the chunk, return the DF
    if length <= chunk_size:
        # yield list(df[:])
        yield df
        return
    # Yield individual chunks
    while start + chunk_size <= length:
        yield (df[start : chunk_size + start])  # noqa E203
        start = start + chunk_size
    # Yield the remainder chunk, if needed
    if start < length:
        yield (df[start:])


def mapper(lines):
    df_mapper = pd.DataFrame(columns=["word_1", "word_2", "qtd_links"])
    tmp = []
    for line in lines.itertuples(name=None, index=False):
        line = str(list(line))  # transf iterrows in string list
        # Data Cleaning
        line = line.replace("'", "")  # delete ' between words inside string
        RE_DIGIT = re.compile(r"\b(?<![0-9-])(\d+)(?![0-9-])\b")
        words = WORD_RE.findall(line)
        digits = RE_DIGIT.findall(str(words))  # Delete Numbers
        words.sort()
        words = list(set(words))
        words.sort()
        words = list(
            filter(lambda w: w not in digits, words)
        )  # Delete words with only number # noqa E501
        # Mapping
        for x, y in combinations(words, 2):
            if x < y:
                tmp.append([x, y, 1])
            else:
                tmp.append([y, x, 1])
    df_mapper = pd.DataFrame(tmp, columns=["word_1", "word_2", "qtd_links"])

    return df_mapper


def map(connector="all", chunck=1000000, schema=1) -> bool:
    """
    Consult the IGEM system manual for information on the ETL mechanism and
    the MAP process.
    """

    # Global Variables
    log_file = __name__
    v_time_process = time.time()
    v_opt_ds = connector.lower()
    v_chunk = chunck
    v_path_file = str(settings.BASE_DIR) + "/psa/"
    v_core: int = 1
    v_core = os.cpu_count()  # type: ignore

    log = start_logger(log_file)
    logger(log, "s", "Start of MAP process")

    if schema in [1, 2]:
        v_schema = schema
    else:
        logger(log, "e", "Only schemes 1 and 2 available")
        # Stops this process and return False
        return False

    # Get Connector parameters
    if v_opt_ds == "all":
        v_where_cs = {"update_ds": True}
    else:
        v_where_cs = {"update_ds": True, "connector": v_opt_ds}
    try:
        qs_queryset = Connector.objects.filter(**v_where_cs)
    except ObjectDoesNotExist:
        logger(log, "e", "Connectors not found/disabled, ObjectDoesNotExist")
        # Stops this process and return False
        return False
    if not qs_queryset:
        logger(log, "w", "Connectors not found or disabled, queryset in null")
        # Stops this process and return False
        return False

    # Ger Terms Master Data
    DF_KEY = pd.DataFrame(
        list(Term.objects.values("id", "term").order_by("term"))
    )  # noqa E501
    # If no Terms, only WordMap will be possible by schema = 1
    if DF_KEY.empty:
        logger(log, "w", "The Term table has no records")
        if v_schema == 2:
            logger(log, "e", "It will not be possible to perform MapReduce without data in the Term table with schema 2. Register new Term or change to schema 1 in which all the words will save on WORDMAP") # noqa E501
            # Stops this process and return False
            return False

    global WORD_RE
    WORD_RE = re.compile(r"[\w'\:\#-]+")  # type: ignore
    # WORD_RE = re.compile(r"\b\d*[^\W\d_][^\W_]*\b") # noqa E501

    for qs in qs_queryset:
        logger(log, "s", f"{qs.connector}: Start MAP Process")

        # QS Variables
        v_time_ds = time.time()
        v_erro = False
        v_dir = v_path_file + str(qs.datasource) + "/" + qs.connector
        v_target = v_dir + "/" + qs.connector + ".csv"

        # Get WorkFlow Control
        try:
            qs_wfc = WFControl.objects.get(
                connector_id=qs.id,
                chk_collect=True,
                chk_prepare=True,
                chk_map=False,
                status__in=["w"],
            )
        except ObjectDoesNotExist:
            logger(log, "w", f"{qs.connector}: Connector without workflow to process")  # noqa E501
            # Stops this process and starts the next connector
            continue

        # Check if file is available
        if not os.path.exists(v_target):
            logger(log, "w", f"{qs.connector}: No Prepare file on {v_target}")
            # Stops this process and starts the next connector
            continue

        # Not implemented yet
        if v_schema == 2:
            logger(log, "w", f"{qs.connector}: Option to eliminate words with no Term relationship is active (schema = 2)")  # noqa E501

        # Read and process prepare file in chunk
        v_idx = 1
        v_count = 0
        for fp in pd.read_csv(
            v_target,
            chunksize=v_chunk,
            low_memory=False,
            skipinitialspace=True,  # noqa E501
        ):  # noqa E501
            if v_idx == 1:
                logger(log, "s", f"{qs.connector}: Start mapper on {v_chunk} rows per block")  # noqa E501

            df_reducer = pd.DataFrame(
                columns=["word_1", "word_2", "qtd_links"]
            )  # noqa E501

            #  Define the number of records by process cores
            v_rows = math.ceil(int(len(fp.index)) / int(v_core))

            try:
                with ThreadPoolExecutor() as executor:
                    future = {
                        executor.submit(mapper, lines)
                        for lines in chunkify(fp, v_rows)  # noqa E501
                    }  # noqa E501

                for future_to in as_completed(future):
                    df_combiner = future_to.result()
                    df_reducer = pd.concat([df_reducer, df_combiner], axis=0)
            except Exception as e:
                logger(log, "e", f"{qs.connector}: Check for data in the file generated by the Prepare Process. Error {e}")  # noqa E501
                v_erro = True
                # Stops this process and starts the next connector
                continue

            DFR = df_reducer.groupby(["word_1", "word_2"], as_index=False)[
                "qtd_links"
            ].sum()  # noqa E501
            DFR["datasource_id"] = qs.datasource_id
            DFR["connector_id"] = qs.id
            if DF_KEY.empty:
                DFR["term_1_id"] = ""
                DFR["term_2_id"] = ""
            else:
                DFR["term_1_id"] = DFR.set_index("word_1").index.map(
                    DF_KEY.set_index("term")["id"]
                )  # noqa E501
                DFR["term_2_id"] = DFR.set_index("word_2").index.map(
                    DF_KEY.set_index("term")["id"]
                )  # noqa E501

            if v_schema == 2:
                DFR.dropna(axis=0, inplace=True)

            DFR = DFR.where(pd.notnull(DFR), "")
            DFR.insert(loc=0, column="index", value=DFR.reset_index().index)

            if (
                v_idx == 1
            ):  # first loop will delete all Connector registers on WORDMAP table # noqa E501
                WordMap.objects.filter(connector_id=qs.id).delete()
            v_idx += 1

            # save bulk data
            model_instances = [
                WordMap(
                    cword=str(record.connector_id)
                    + "-"
                    + str(v_idx)
                    + "-"
                    + str(record.index),  # noqa E501
                    word_1=record.word_1,
                    word_2=record.word_2,
                    qtd_links=record.qtd_links,
                    connector_id=record.connector_id,
                    datasource_id=record.datasource_id,
                    term_1_id=record.term_1_id,
                    term_2_id=record.term_2_id,
                )
                for record in DFR.itertuples()
            ]
            WordMap.objects.bulk_create(model_instances)
            v_row = len(DFR.index)
            v_count += v_row

            logger(log, "s", f"{qs.connector}: Block {v_idx} with {v_row} combinations processed")  # noqa E501

        # Update WorkFlow Control Process
        if v_erro:
            # Stops this process and starts the next connector
            # Stops this process and starts the next connector
            qs_wfc.chk_map = False
            qs_wfc.last_update = timezone.now()
            qs_wfc.status = "w"
            qs_wfc.save()
            continue

        # Update WorkFlow Control table:
        # Get number of records and time process
        v_time = int(time.time() - v_time_ds)
        # Update instance variables
        qs_wfc.last_update = timezone.now()
        qs_wfc.chk_map = True
        qs_wfc.status = "w"
        qs_wfc.time_map = v_time
        qs_wfc.row_map = v_count
        qs_wfc.save()
        logger(log, "s", f"{qs.connector}: Map process was completed successfully in {v_time} seconds with {v_count} records")  # noqa E501

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

    # End of MAP process
    # Log-Message
    v_time = int(time.time() - v_time_process)
    logger(log, "s", f"MAP process was completed in {v_time} seconds")  # noqa E501

    return True
