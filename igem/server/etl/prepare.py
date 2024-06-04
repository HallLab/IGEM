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
    from ge.models import Connector, DSTColumn, WFControl, WordTerm  # noqa E402
    from ge.utils import logger, start_logger
except:  # noqa E722
    raise


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

warnings.filterwarnings("ignore", category=UserWarning)

# Global Variables
df_mapping = pd.DataFrame()


# IGEM Search Engine
def search_engine(lines, log, connector):
    for idx, line in lines.iterrows():
        try:
            # Transf string (line) in a list of words
            # TODO: Some words has the (). ex: (iron, or iron) | fix?
            v_str = re.split(r"[\^\ \[\]]", str(line[0]))

            # List of items that you don't need
            items_to_exclude = ['', ',', ' ', ':', ';']

            # Remove items from the list that are in the items_to_exclude list
            # using list comprehension
            v_str = [
                word for word in v_str if word.strip() not in items_to_exclude
                ]

            # The error you encountered, "nothing to repeat," is because of
            # the presence of unescaped parentheses (( and )) and the plus
            # sign (+) in the regular expression pattern. In regular
            # expressions, the plus sign is a special character that means
            #  "one or more occurrences of the previous character or group."
            # To use a literal plus sign in the pattern, you need to escape
            # it with a backslash (\).
            v_str = [re.escape(word) for word in v_str]
            v_str = [word.rstrip(",;") for word in v_str]

            # Search im df_mapping all records that contain any word from line
            # df_mapping is all records from ge_wordterm
            # ge_wordterm is a pre-computing mapping of IGEM Terms
            # TODO: some interations returns errors from (). Check how to fix!
            df_mapping_TEMP = df_mapping[
                df_mapping["word"].str.contains(
                    r"\b(?:\s|^)(?:{})(?:\s|$\b)".format("|".join(v_str))
                )
            ]
            # Keep only records with full match with words in line
            df_mapping_TEMP = df_mapping_TEMP[
                df_mapping_TEMP['word'].apply(
                    lambda x: all(word in v_str for word in x.split())
                    )]

            if df_mapping_TEMP.empty:
                # logger(log, "e", f"{connector}: Do not found on wordterm Line: {idx}")  # noqa E501
                line[0] = "NWT"
                continue

            s = df_mapping_TEMP.word.str.len().sort_values(ascending=False).index  # noqa E501
            df_mapping_TEMP = df_mapping_TEMP.reindex(s)
            df_mapping_TEMP = df_mapping_TEMP.reset_index(drop=True)

            line_pull = []
            for index, row in df_mapping_TEMP.iterrows():
                if line[0].find(row["word"]) != -1:
                    v_key = str(row["term_id__term"])
                    line[0] = line[0].replace(row["word"], "")
                    line_pull.append(v_key)

            line_pull = " ".join(str(x) for x in set(line_pull))
            line[0] = line_pull
        except Exception as e:
            # Stops this process and return False
            logger(log, "e", f"{connector}: ERROR IGEM Search Engine, index: {idx}, error: {e}, Line: {line}")  # noqa E501
            line[0] = "error"

    lines_return = pd.DataFrame(lines)

    return lines_return

# Propose to IGEM Search Engine ()
# def teste(texts, log):
#     # load a pre-trained spaCy model
#     nlp = spacy.load("en_core_web_sm")
#     # your pre-computed mapping list
#     df_mapping = {
#       "lead": "c0001",
#       "lead poisoning, nervous system,
#       childhood": "c0002",
#       "lead magnesium niobate": "c0003"
#       }  # noqa E501
#     for text in texts:
#         # process the input text with spaCy
#         doc = nlp(text)
#         # iterate over the tokens in the processed text
#         for token in doc:
#             # check if the token is in the mapping list
#             if token.text.lower() in df_mapping:
#                 # if yes, replace the token with the corresponding term
#                 token.text = df_mapping[token.text.lower()]
#         # convert the processed text back to a string
#         result = doc.text
#         text[0] = result
#     lines_return = pd.DataFrame(texts)
#     return lines_return


def prepare(connector="all", chunk=1000) -> bool:
    # config PSA folder (persistent staging area)
    """
    Consult the IGEM system manual for information on the ETL mechanism and
    the PREPARE process.
    """

    # Variables
    log_file = __name__
    v_path_file = f"{settings.BASE_DIR}/psa/"
    start_time = time.time()
    v_chunk = chunk
    v_opt_ds = connector.lower()

    log = start_logger(log_file)

    logger(log, "s", "Start of PREPARE process")

    # Get Connector parameters
    if v_opt_ds == "all":
        v_where_cs = {"update_ds": True}
    else:
        v_where_cs = {"update_ds": True, "connector": v_opt_ds}
    try:
        qs_queryset = Connector.objects.filter(**v_where_cs)
    except ObjectDoesNotExist:
        logger(log, "e", "Connectors not found or disabled, ObjectDoesNotExist")  # noqa E501
        return False  # Stops this process and return False
    if not qs_queryset:
        logger(log, "w", "Connectors not found or disabled, queryset in null")  # noqa E501
        return False  # Stops this process and return False

    # Only WordTerms with status and commute true
    # WordTerm table search the relationships between active words and key
    global df_mapping
    df_mapping = pd.DataFrame(
        list(
            WordTerm.objects.values("word", "term_id__term")
            .filter(status=True)
            .order_by("word")
        )
    )

    # check memory size:
    memory_usage_bytes = df_mapping.memory_usage(deep=True).sum()
    memory_usage_mb = memory_usage_bytes / 1024 / 1024
    logger(log, "w", f"pre-computing mapping list memory usage: {memory_usage_mb:.2f} MB")  # noqa E501

    if df_mapping.empty:
        logger(log, "w", "No data on the relationship words and term")
        return False  # Stops this process and return False

    # Process each connector in turn
    for qs in qs_queryset:
        logger(log, "s", f"{qs.connector}: Start Prepare Process")

        # qs connector variables
        connector_time = time.time()
        v_dir = f"{v_path_file}{qs.datasource}/{qs.connector}"
        v_source = f"{v_dir}/{qs.target_file_name}"
        v_target = f"{v_dir}/{qs.connector}.csv"
        v_skip = qs.source_file_skiprow
        # v_tab = str(qs.source_file_sep)
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
            logger(log, "w", f"{qs.connector}: Connector without workflow to process")  # noqa E501
            continue  # Stops this process and starts the next connector

        # Check if file is available
        if not os.path.exists(v_source):
            logger(log, "w", f"{qs.connector}: No IGEM Standard File on {v_source}")  # noqa E501
            continue  # Stops this process and starts the next connector

        # Delete exiting target file
        if os.path.exists(v_target):
            os.remove(v_target)
            logger(log, "s",  f"{qs.connector}: The output file of the previous PREPARE process has been removed.")  # noqa E501

        # Parameters to load source file
        if v_skip >= 1:
            v_read_file = {
                # "sep": v_tab,
                "skiprows": v_skip,
                "engine": "python",
                "chunksize": v_chunk,
            }  # noqa E501
            logger(log, "s",  f"{qs.connector}: Open file with {v_skip} skipped rows and {v_chunk} rows per block process")  # noqa E501
        else:
            v_read_file = {
                # "sep": v_tab,
                "engine": "python",
                "chunksize": v_chunk,
            }  # noqa E501
            logger(log, "s", f"{qs.connector}: Open file without skips rows and {v_chunk} rows per block process")  # noqa E501

        # Load source file in defined chunk
        # Each interation will append the result on target file
        v_idx = 1
        for df_source in pd.read_csv(v_source, **v_read_file):
            v_col = len(df_source.columns)
            v_row = len(df_source.index)
            df_target = pd.DataFrame()  # df keep this results and save on target file  # noqa E501

            # Each column will be processed individually
            # Processing "RULES" are defined in the Connector
            for n in range(v_col):
                try:
                    qs_col = DSTColumn.objects.filter(connector_id=qs.id, column_number=n).first()  # noqa E501
                    v_col_idx = str(qs_col.column_name)
                except ObjectDoesNotExist:
                    qs_col = None
                # except Exception as e:
                #     logger(log, "e", f"{qs.connector}: Error on transformation rules. Check Connector settings and ID duplicity. {e}" )  # noqa E501

                # RULE 0: Column with Status False
                # If setup the column but with Status False, not process
                if not qs_col.status:
                    logger(log, "w", f"{qs.connector}: Column {qs_col.column_name} has a disabled status")  # noqa E501
                    continue  # next column

                # RULE 1: Columns not configured on Connector master data
                # NOT APPLY the: IGEM Search Engine
                # look up the exact term in the Pre-Computing Mapping List
                # TODO: Switch to IGEM Search Engine
                if not qs_col:
                    if v_idx == 1:
                        logger(log, "s", f"{qs.connector}: No rules defines to column {n}. This column will consider on process")  # noqa E501
                    df_target[n] = df_source.iloc[:, n]
                    df_target = df_target.apply(lambda x: x.astype(str).str.lower())  # Keep all words lower case to match # noqa E501
                    df_target[v_col_idx] = df_target.set_index(v_col_idx).index.map(df_mapping.set_index("word")["term_id__term"])  # noqa E501
                    continue  # next column

                # RULE 2: Columns with Prefixes
                # DO NOT APPLY the: IGEM Search Engine
                # Status and Single Word is True for this column
                # Has a PREFIX different of none
                # Ex.: c003918 --> chem:c003918
                if (qs_col.status is True) and (qs_col.single_word is True) and (str(qs_col.pre_value) != "none"):  # noqa E501
                    # TODO: is necessary? (str(qs_col.pre_value) != "none")
                    df_target[qs_col.column_name] = df_source.iloc[:, n].apply(
                        lambda y: "{}{}".format(qs_col.pre_value, y)
                        )

                    # TODO: this is a temporary solution / the paths come with prefix from ctd  # noqa E501
                    df_target[v_col_idx] = df_target[v_col_idx].str.replace('KEGG:', '').str.replace('REACT:', '')  # noqa E501
                    df_target[v_col_idx] = df_target[v_col_idx].str.replace('MESH:', '')  # noqa E501

                    df_target = df_target.apply(
                        lambda x: x.astype(str).str.lower()
                        )

                    continue  # next column

                # RULE 3: Columns without Prefixes
                # DO NOT APPLY the IGEM Search Engine
                # Status and Single Word is True for this column
                # Has a NONE PREFIX none
                # Ex.:  astiron --> chem:c003918
                if (qs_col.status is True) and (qs_col.single_word is True) and (str(qs_col.pre_value) == "none"):  # noqa E501
                    df_target[qs_col.column_name] = df_source.iloc[:, n]
                    df_target = df_target.apply(
                        lambda x: x.astype(str).str.lower()
                        )
                    df_target[v_col_idx] = df_target.set_index(
                        v_col_idx
                        ).index.map(
                        df_mapping.set_index("word")["term_id__term"])
                    continue  # next column

                # RULE 4: No Single Word Column
                # APPLY the IGEM Search Engine
                # Will search im Multi-Process
                # Status True and Single_Word False for this column
                # The prefix does not influence this rule
                # Ex.: The iron-manganese silicate oxide will combine with
                #      heparin-iron complex
                #      --> chem:c000717569 chem:c000615724
                if (qs_col.status is True) and (qs_col.single_word is False):
                    df_temp = pd.DataFrame()
                    # df_combiner = pd.DataFrame()
                    df_reducer = pd.DataFrame(columns=[qs_col.column_name])
                    ls_reducer = []
                    df_temp[qs_col.column_name] = df_source.iloc[:, n]
                    df_temp = df_temp.apply(lambda x: x.astype(str).str.lower())  # noqa E501
                    cpu_count = int(os.cpu_count()) - 1  # type: ignore
                    # cpu_count = 1  # TODO: remove (debug only)
                    ls_temp = np.array_split(df_temp, cpu_count)

                    try:
                        with ThreadPoolExecutor() as executor:
                            future = {
                                executor.submit(
                                    search_engine,
                                    ls_temp[i],
                                    log,
                                    qs.connector
                                    )
                                for i in range(len(ls_temp))
                            }
                        for future_to in as_completed(future):
                            # df_combiner = future_to.result()
                            # df_reducer = pd.concat([df_reducer, df_combiner], axis=0)  # noqa E501                      
                            ls_reducer.append(future_to.result())
                    except Exception as e:
                        logger(
                            log,
                            "e",
                            f"{qs.connector}: IGEM Search Engine function error, {v_idx} block - {qs_col.column_name} col. ERROR CODE: {e}")  # noqa E501

                    try:
                        df_reducer = pd.concat(ls_reducer, axis=0)
                        df_reducer = df_reducer.sort_index()
                        df_target[qs_col.column_name] = df_reducer
                    except Exception as e:
                        logger(
                            log,
                            "e",
                            f"{qs.connector}: IGEM Search Engine function error, {v_idx} block - {qs_col.column_name} col. ERROR CODE: {e}")  # noqa E501
                        continue  # Next column

                    continue  # Next column

                logger(log, "w", f"{qs.connector}: No rule has been applied to the {qs_col.column_number} - {qs_col.column_name} column")  # noqa E501

            # Save data to the Target File
            df_target.to_csv(v_target, header=header, mode="a")
            header = False  # Prevent creating new header lines
            logger(log, "s", f"{qs.connector}:  Block {v_idx} with {v_row} records processed")  # noqa E501
            v_idx += 1
            has_error = False

        # Go to next Connector if has error
        # Do not change the WFControl
        if has_error:
            logger(log, "e", f"{qs.connector}: Error on process rules")  # noqa E501
            continue  # Next Connector

        # Delete source file
        if not qs.target_file_keep:
            os.remove(v_source)
            logger(log, "s", f"{qs.connector}: Deleted IGEM Standard File in PSA")  # noqa E501
        else:
            logger(log, "s", f"{qs.connector}: Kept the IGEM Standard File in PSA")  # noqa E501

        # Update the WFControl
        v_count = 0
        with open(v_target) as fp:
            for v_count, _ in enumerate(fp, 1):
                pass
        v_time = int(time.time() - connector_time)
        # Update instance variables
        qs_wfc.last_update = timezone.now()
        qs_wfc.chk_prepare = True
        qs_wfc.status = "w"
        qs_wfc.time_prepare = v_time
        qs_wfc.row_prepare = v_count
        qs_wfc.save()
        logger(log, "s", f"{qs.connector}: Prepare process was completed successfully in {v_time} seconds with {v_count} records")  # noqa E501

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
    v_time = int(time.time() - start_time)
    logger(log, "s", f"PREPARE process was completed {v_time} seconds")  # noqa E501
    return True
