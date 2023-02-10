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
from django_thread import ThreadPoolExecutor

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import Connector, Term, WFControl, WordMap  # noqa E402
except:  # noqa E722
    raise


def chunkify(df: pd.DataFrame, chunk_size: int):
    start = 0
    length = df.shape[0]
    # If DF is smaller than the chunk, return the DF
    if length <= chunk_size:
        yield list(df[:])
        return
    # Yield individual chunks
    while start + chunk_size <= length:
        yield (df[start:chunk_size + start])
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
        words = list(filter(lambda w: w not in digits, words))  # Delete words with only number # noqa E501
        # Mapping
        for (x, y) in combinations(words, 2):
            if x < y:
                tmp.append([x, y, 1])
            else:
                tmp.append([y, x, 1])
    df_mapper = pd.DataFrame(tmp, columns=["word_1", "word_2", "qtd_links"])

    return df_mapper


def map(connector="all", chunck=1000000, schema=1):
    v_time_process = time.time()
    v_opt_ds = connector.lower()
    v_chunk = chunck

    if schema in [1, 2]:
        v_schema = schema
    else:
        print('Only schemes 1 and 2 available')
        sys.exit(2)

    print('Start: Process to map words from databases')

    if v_opt_ds == 'all':
        v_where_cs = {'update_ds': True}
    else:
        v_where_cs = {'update_ds': True, 'Connector': v_opt_ds}
    try:
        qs_queryset = Connector.objects.filter(**v_where_cs)
    except ObjectDoesNotExist:
        print('  Connectors not found or disabled')
        sys.exit(2)
    if not qs_queryset:
        print('  Connectors not found or disabled')
        sys.exit(2)

    DF_KEY = pd.DataFrame(list(Term.objects.values('id', 'term').order_by('term')))  # noqa E501
    if DF_KEY.empty:
        print('  The Term table has no records.')
        if v_schema == 2:
            print('  It will not be possible to perform MapReduce without data in the Term table with schema 2. \
                    Register new Term or change to schema 1 in which all the words will save on WORDMAP') # noqa E501
            sys.exit(2)

    # config PSA folder (persistent staging area)
    v_path_file = str(settings.BASE_DIR) + "/psa/"

    global WORD_RE
    WORD_RE = re.compile(r"[\w'\:\#]+")  # WORD_RE = re.compile(r"\b\d*[^\W\d_][^\W_]*\b") # noqa E501

    v_cores = os.cpu_count()
    print('  Process run with {0} cores on multiprocess'.format(v_cores))

    for qs in qs_queryset:
        print('  Start: Run database {0} on Connector {1}'.format(qs.datasource, qs.connector))  # noqa E501
        v_time_ds = time.time()
        v_erro = False

        # Check control proccess
        try:
            qs_wfc = WFControl.objects.get(
                connector_id=qs.id,
                chk_collect=True,
                chk_prepare=True,
                chk_map=False
                )
        except ObjectDoesNotExist:
            print('   Connector without workflow to process')
            continue

        v_dir = v_path_file + str(qs.datasource) + "/" + qs.connector
        v_target = v_dir + "/" + qs.connector + ".csv"

        if not os.path.exists(v_target):
            print("  File to process not available in " + v_target)
            print('    File not available to:  "%s"' % qs.connector)
            print('      check  on:  "%s"' % v_target)
            continue

        if v_schema == 2:
            print('     Option to eliminate words with no Term relationship is active (schema = 2)')  # noqa E501

        v_idx = 1
        for fp in pd.read_csv(v_target, chunksize=v_chunk, low_memory=False, skipinitialspace=True):  # noqa E501
            if v_idx == 1:
                print('    Start mapper on {0} rows per block'.format(v_chunk))

            df_reducer = pd.DataFrame(
                columns=["word_1", "word_2", "qtd_links"]
                )

            v_rows = math.ceil(len(fp.index)/v_cores)

            try:
                with ThreadPoolExecutor() as executor:
                    future = {executor.submit(mapper, lines) for lines in chunkify(fp, v_rows)} # noqa E501

                for future_to in as_completed(future):
                    df_combiner = future_to.result()
                    df_reducer = pd.concat([df_reducer, df_combiner], axis=0)
            except Exception as e:
                print(e)
                print('    Error on map sub-process. Check for data in the file generated by the Prepare Process.') # noqa E501
                v_erro = True
                continue

            DFR = df_reducer.groupby(["word_1", "word_2"], as_index=False)["qtd_links"].sum() # noqa E501
            DFR["datasource_id"] = qs.datasource_id
            DFR["connector_id"] = qs.id
            if DF_KEY.empty:
                DFR["term_1_id"] = ""
                DFR["term_2_id"] = ""
            else:
                DFR["term_1_id"] = DFR.set_index("word_1").index.map(DF_KEY.set_index("term")["id"]) # noqa E501
                DFR["term_2_id"] = DFR.set_index("word_2").index.map(DF_KEY.set_index("term")["id"]) # noqa E501

            if v_schema == 2:
                DFR.dropna(axis=0, inplace=True)

            DFR = DFR.where(pd.notnull(DFR), '')
            DFR.insert(loc=0, column="index", value=DFR.reset_index().index)

            if v_idx == 1:  # first loop will delete all Connector registers on WORDMAP table # noqa E501
                WordMap.objects.filter(connector_id=qs.id).delete()
            v_idx += 1

            model_instances = [WordMap(
                cword=str(record.connector_id) + '-' + str(v_idx) + '-' + str(record.index), # noqa E501
                word_1=record.word_1,
                word_2=record.word_2,
                qtd_links=record.qtd_links,
                connector_id=record.connector_id,
                datasource_id=record.datasource_id,
                term_1_id=record.term_1_id,
                term_2_id=record.term_2_id,
                ) for record in DFR.itertuples()]

            WordMap.objects.bulk_create(model_instances)

            v_row = len(DFR.index)
            print('      Block {0} with {1} combinations processed'.format(v_idx, v_row)) # noqa E501

        # Update WorkFlow Control Process
        if v_erro:
            continue

        qs_wfc.chk_map = True
        qs_wfc.save()

        print('    Connector loaded in {0} seconds'.format(int(time.time() - v_time_ds))) # noqa E501      

    print('End of process in {0} seconds'.format(int(time.time() - v_time_process)))  # noqa E501

    return "success"
