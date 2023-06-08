import os
import re
import sys
import time
import warnings
from concurrent.futures import as_completed

import numpy as np
import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.utils import timezone
from django_thread import ThreadPoolExecutor

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import Connector, DSTColumn, Logs, WFControl, WordTerm  # noqa E402
except:  # noqa E722
    raise

warnings.filterwarnings("ignore", category=UserWarning)


def mapper(lines):
    for idx, line in lines.iterrows():
        try:
            v_str = re.split(r"[\^\ \[\]]", str(line[0]))
            DF_KY_WD_TEMP = DF_KY_WD[
                DF_KY_WD["word"].str.contains(
                    r"\b(?:\s|^)(?:{})(?:\s|$\b)".format("|".join(v_str))
                )
            ]  # noqa E501

            s = DF_KY_WD_TEMP.word.str.len().sort_values(ascending=False).index
            DF_KY_WD_TEMP = DF_KY_WD_TEMP.reindex(s)
            DF_KY_WD_TEMP = DF_KY_WD_TEMP.reset_index(drop=True)

            line_pull = []
            for index, row in DF_KY_WD_TEMP.iterrows():
                if line[0].find(row["word"]) != -1:
                    v_key = str(row["term_id__term"])
                    line[0] = line[0].replace(row["word"], "")
                    line_pull.append(v_key)

            line_pull = " ".join(str(x) for x in set(line_pull))
            line[0] = line_pull
        except Exception as e:
            # Log-Message
            v_desc = f"CONNECTOR: Unable NLP Search, line {idx} {line}. Erro: {e}"  # noqa E501
            print(v_desc)
            Logs.objects.create(
                process="ETL/PREPARE", description=v_desc, status="e"
            )  # noqa E501
            # Stops this process and return False

            line[0] = "ERROR ON COMMUTE"

    lines_return = pd.DataFrame(lines)

    return lines_return


"""
Second process in the data flow and aims to preparing the source data
in an improved format before the MapReduce process

Subprocess:
    1. Elimination of header lines
    2. Deleting unnecessary columns
    3. Transforming ID columns with identifiers
    4. Replacement of terms
    5. Optional, delete source file

"""


def prepare(connector="all", chunk=1000000) -> bool:
    # config PSA folder (persistent staging area)
    """
    Consult the IGEM system manual for information on the ETL mechanism and
    the PREPARE process.
    """

    # Global Variables
    v_proc = "ETL/PREPARE"
    v_path_file = str(settings.BASE_DIR) + "/psa/"
    v_time_process = time.time()
    v_chunk = chunk
    v_opt_ds = connector.lower()

    # Log-Message
    v_desc = "Start of PREPARE process"
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

    # Only WordTerms with status and commute true
    # WordTerm table search the relationships between active words and key
    # DF_KY_WD = pd.DataFrame(list(WordTerm.objects.values('word',
    # 'keyge_id__keyge').filter(status=True, commute=True).order_by('word')))
    global DF_KY_WD
    DF_KY_WD = pd.DataFrame(
        list(
            WordTerm.objects.values("word", "term_id__term")
            .filter(status=True, commute=True)
            .order_by("word")
        )
    )
    # adicionar um check se nao tem informacao nessa tabela
    if DF_KY_WD.empty:
        # Log-Message
        v_desc = "No data on the relationship words and term"
        print(v_desc)
        Logs.objects.create(process=v_proc, description=v_desc, status="w")
        # Stops this process and return False
        return False

    # Process each connector in turn
    for qs in qs_queryset:
        # Log-Message
        v_desc = f"{qs.connector}: Start Prepare Process"
        print(v_desc)
        Logs.objects.create(process=v_proc, description=v_desc, status="s")

        # QS Variables
        v_time_ds = time.time()
        v_dir = v_path_file + str(qs.datasource) + "/" + qs.connector
        v_source = v_dir + "/" + qs.target_file_name
        v_target = v_dir + "/" + qs.connector + ".csv"
        v_skip = qs.source_file_skiprow
        v_tab = str(qs.source_file_sep)
        header = True

        # Get WorkFlow Control
        try:
            qs_wfc = WFControl.objects.get(
                connector_id=qs.id,
                chk_collect=True,
                chk_prepare=False,
                status__in=["w"],
            )
        except ObjectDoesNotExist:
            # Log-Message
            v_desc = f"{qs.connector}: Connector without workflow to process."
            print(v_desc)
            Logs.objects.create(process=v_proc, description=v_desc, status="w")
            # Stops this process and starts the next connector
            continue

        # Check if file is available
        if not os.path.exists(v_source):
            # Log-Message
            v_desc = f"{qs.connector}: No IGEM Standard File on {v_source}."
            print(v_desc)
            Logs.objects.create(process=v_proc, description=v_desc, status="w")
            # Stops this process and starts the next connector
            continue

        # Delete exiting target file
        if os.path.exists(v_target):
            os.remove(v_target)
            # Log-Message
            v_desc = f"{qs.connector}: The output file of the previous PREPARE process has been removed."  # noqa E501
            print(v_desc)
            Logs.objects.create(process=v_proc, description=v_desc, status="s")

        try:
            # read IGEM Standard file (COLLECT Output)
            if v_skip >= 1:
                v_read_file = {
                    "sep": v_tab,
                    "skiprows": v_skip,
                    "engine": "python",
                    "chunksize": v_chunk,
                }  # noqa E501
                # Log-Message
                v_desc = f"{qs.connector}: Open file with {v_skip} skipped rows and {v_chunk} rows per block process."  # noqa E501
                print(v_desc)
                Logs.objects.create(
                    process=v_proc, description=v_desc, status="s"
                )  # noqa E501
            else:
                v_read_file = {
                    "sep": v_tab,
                    "engine": "python",
                    "chunksize": v_chunk,
                }  # noqa E501
                # Log-Message
                v_desc = f"{qs.connector}: pen file without skips rows and {v_chunk} rows per block process."  # noqa E501
                print(v_desc)
                Logs.objects.create(
                    process=v_proc, description=v_desc, status="s"
                )  # noqa E501
            v_idx = 1
            for df_source in pd.read_csv(v_source, **v_read_file):
                v_col = len(df_source.columns)
                v_row = len(df_source.index)
                df_target = pd.DataFrame()

                for n in range(v_col):  # Read transformations columns
                    try:
                        try:
                            qs_col = DSTColumn.objects.get(
                                connector_id=qs.id, column_number=n
                            )  # noqa E501
                            v_col_idx = str(qs_col.column_name)
                        except ObjectDoesNotExist:
                            qs_col = None
                    except Exception as e:
                        # Log-Message
                        v_desc = f"{qs.connector}: Error on transformation rules. Check Connector settings and ID duplicity. {e}"  # noqa E501
                        print(v_desc)
                        Logs.objects.create(
                            process=v_proc, description=v_desc, status="e"
                        )  # noqa E501
                    if not qs_col:
                        # Rule 1: Columns not configured on Connector master data. The process will run and perform the Commute # noqa E501
                        if v_idx == 1:
                            # Log-Message
                            v_desc = f"{qs.connector}: No rules defines to column {n}. This column will consider on process."  # noqa E501
                            print(v_desc)
                            Logs.objects.create(
                                process=v_proc, description=v_desc, status="s"
                            )  # noqa E501

                        df_target[n] = df_source.iloc[:, n]
                        df_target = df_target.apply(
                            lambda x: x.astype(str).str.lower()
                        )  # Keep all words lower case to match # noqa E501
                        df_target[v_col_idx] = df_target.set_index(v_col_idx).index.map(
                            DF_KY_WD.set_index("word")["term_id__term"]
                        )  # noqa E501
                    else:
                        if qs_col.status:
                            if str(qs_col.pre_value) != "none":
                                # Rule 2: columns with defined prefixes / does not perform the commute process  # noqa E501
                                df_target[qs_col.column_name] = df_source.iloc[
                                    :, n
                                ].apply(
                                    lambda y: "{}{}".format(qs_col.pre_value, y)
                                )  # noqa E501
                                df_target = df_target.apply(
                                    lambda x: x.astype(str).str.lower()
                                )  # Keep all words lower case to match # noqa E501
                                # Stops this process and starts the next connector
                                continue
                            # Rule 3: Columns configured for the process with prefix None /Does not add prefix / Performs the Commute process in a single word # noqa E501
                            if qs_col.single_word:
                                df_target[qs_col.column_name] = df_source.iloc[
                                    :, n
                                ]  # noqa E501
                                df_target = df_target.apply(
                                    lambda x: x.astype(str).str.lower()
                                )  # Keep all words lower case to match # noqa E501
                                df_target[v_col_idx] = df_target.set_index(
                                    v_col_idx
                                ).index.map(
                                    DF_KY_WD.set_index("word")["term_id__term"]
                                )  # Commute Process # noqa E501
                                # Stops this process and starts the next connector
                                continue

                            # Rule 4: Columns configured for the process with prefix None /Does not add prefix / Performs the Commute process WORD by WORD on sentence # noqa E501
                            df_temp = pd.DataFrame()
                            df_combiner = pd.DataFrame()
                            df_reducer = pd.DataFrame(
                                columns=[qs_col.column_name]
                            )  # noqa E501
                            df_temp[qs_col.column_name] = df_source.iloc[:, n]
                            df_temp = df_temp.apply(
                                lambda x: x.astype(str).str.lower()
                            )  # Keep all words lower case to match  # noqa E501
                            list_df = np.array_split(df_temp, os.cpu_count() - 1)

                            try:
                                with ThreadPoolExecutor() as executor:
                                    future = {
                                        executor.submit(mapper, list_df[i])
                                        for i in range(len(list_df))
                                    }  # noqa E501

                                for future_to in as_completed(future):
                                    df_combiner = future_to.result()
                                    df_reducer = pd.concat(
                                        [df_reducer, df_combiner], axis=0
                                    )  # noqa E501
                            except Exception as e:
                                # Log-Message
                                v_desc = f"{qs.connector}: Error on commute word by word on sentence in row {n}. {e}"  # noqa E501
                                print(v_desc)
                                Logs.objects.create(
                                    process=v_proc, description=v_desc, status="e"
                                )  # noqa E501
                            df_reducer = df_reducer.sort_index()
                            df_target[qs_col.column_name] = df_reducer
                            # Stops this process and starts the next connector
                            continue

                        # Rule 5: Columns configured and not activated will not be processed   # noqa E501
                df_target.to_csv(
                    v_target, header=header, mode="a"
                )  # Write the file# noqa E501
                header = False  # Prevent creating new header lines
                # Log-Message
                v_desc = f"{qs.connector}:  Block {v_idx} with {v_row} records processed."  # noqa E501
                print(v_desc)
                Logs.objects.create(
                    process=v_proc, description=v_desc, status="s"
                )  # noqa E501
                v_idx += 1

            # Delete source file
            if not qs.target_file_keep:
                os.remove(v_source)
                # Log-Message
                v_desc = (
                    f"{qs.connector}: Deleted IGEM Standard File in PSA."  # noqa E501
                )
                print(v_desc)
                Logs.objects.create(
                    process=v_proc, description=v_desc, status="s"
                )  # noqa E501
            else:
                # Log-Message
                v_desc = (
                    f"{qs.connector}: Kept the IGEM Standard File in PSA."  # noqa E501
                )
                print(v_desc)
                Logs.objects.create(
                    process=v_proc, description=v_desc, status="s"
                )  # noqa E501

        except Exception as e:
            # Log-Message
            v_desc = f"{qs.connector}: Error when process. {e}."  # noqa E501
            print(v_desc)
            Logs.objects.create(
                process=v_proc, description=v_desc, status="e"
            )  # noqa E501
            # Stops this process and starts the next connector
            qs_wfc.chk_prepare = False
            qs_wfc.last_update = timezone.now()
            qs_wfc.status = "w"
            qs_wfc.save()
            continue

        # Update WorkFlow Control table:
        # Get number of records and time process
        v_count = 0
        with open(v_target) as fp:
            for v_count, _ in enumerate(fp, 1):
                pass
        v_time = int(time.time() - v_time_ds)
        # Update instance variables
        qs_wfc.last_update = timezone.now()
        qs_wfc.chk_prepare = True
        qs_wfc.status = "w"
        qs_wfc.time_prepare = v_time
        qs_wfc.row_prepare = v_count
        qs_wfc.save()
        # Log-Message
        v_desc = f"{qs.connector}: Prepare process was completed successfully in {v_time} seconds with {v_count} records"  # noqa E501
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
                Logs.objects.create(process=v_proc, description=v_desc, status="w")

    # End of COLLECT process
    # Log-Message
    v_time = int(time.time() - v_time_process)
    v_desc = f"PREPARE process was completed {v_time} seconds"  # noqa E501
    print(v_desc)
    Logs.objects.create(process=v_proc, description=v_desc, status="s")

    return True
