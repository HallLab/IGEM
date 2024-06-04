import os
import re
import sys
import warnings
from concurrent.futures import as_completed
from datetime import datetime

import numpy as np
import pandas as pd
from django.conf import settings
from django_thread import ThreadPoolExecutor

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge.models import WordTerm
except:  # noqa E722
    raise


warnings.filterwarnings(
    "ignore", "This pattern is interpreted as a regular expression"
)  # uncomment to suppress the UserWarning


def search_terms(lines):
    v_msm = False  # Use on Debug (if true will show messagens and processes data) # noqa E501

    # Structure to keep processed data
    df_mapper = pd.DataFrame(
        columns=[
            "row",
            "string",
            "word",
            "term",
            "term_id",
            "term_descr",
            "qtd_terms",
            "qtd_loops",
            "time",
        ]
    )
    v_iloc = 0

    for idx, line in lines.iterrows():
        v_chk = False  # Check if found term / if not will be False and create a row with this information # noqa E501
        v_line = line[
            0
        ]  # v_line used to fix the String Column on output file # noqa E501
        v_row = (
            line.name
        )  # Keep the source number row to used in Row Column on output file # noqa E501
        tm_start = datetime.now()  # set the start time of the line
        try:
            # It splits the entire string (line) for a list of values, where each value # noqa E501
            # will be a parameter for the search for combinations in the KeyWords table. # noqa E501
            v_str = re.split(r"[\^\ ]", str(line[0]))

            items_to_exclude = ['', ',', ' ', ':', ';']
            v_str = [
                word for word in v_str if word.strip() not in items_to_exclude
                ]
            v_str = [re.escape(word) for word in v_str]
            v_str = [word.rstrip(",;") for word in v_str]

            if v_msm:
                print("--> List value to search: ", v_str)
            # All records in the list value are searched for each element of the list. # noqa E501
            # The contains option results from a more significant number of records than # noqa E501
            # the Match option; when using the Match option, we would leave out the # noqa E501
            # combinations of words that in the list are in different elements, and in # noqa E501
            # KeyWord, we will have them together.
            DF_KY_WD_TEMP = DF_KY_WD[
                DF_KY_WD["word"].str.contains(
                    r"\b(?:\s|^)(?:{})(?:\s|$\b)".format("|".join(v_str))
                )
            ]  # CONTAINS

            # Keep only records with full match with words in line
            DF_KY_WD_TEMP = DF_KY_WD_TEMP[
                DF_KY_WD_TEMP['word'].apply(
                    lambda x: all(word in v_str for word in x.split())
                    )]

            # DF_KY_WD_TEMP = DF_KY_WD[DF_KY_WD['word'].str.match(r'\b(?:\s|^)(?:{})(?:\s|$\b)'.format('|'.join(v_str)))]  # MARCH # noqa E501
            if v_msm:
                print(
                    "--> Number of records found that match the list of searched elements: ",  # noqa E501
                    len(DF_KY_WD_TEMP.index),
                )  # noqa E501
                print("--> Selected Records to Association: ")  # noqa E501
                print(DF_KY_WD_TEMP)
                print("   ")

            # Sort the list from longest string to shortest. This process makes it possible # noqa E501
            # to associate compound words with simple words later.
            s = DF_KY_WD_TEMP.word.str.len().sort_values(ascending=False).index
            DF_KY_WD_TEMP = DF_KY_WD_TEMP.reindex(s)
            DF_KY_WD_TEMP = DF_KY_WD_TEMP.reset_index(drop=True)
            # DF_KY_WD_TEMP.to_csv('Key_word_temp.csv') # DEBUG USE: Views of records found on KeyWord with list filter # noqa E501

            # For each record found in KeyWord based on List as a filter, a loop will be # noqa E501
            # performed to find the best match with the List.

            # We have some issues and improvements in this process, such as:
            # - very complex strings with special characters are not working correctly.# noqa E501
            # - We have to run the iteration for all the records found, even though we # noqa E501
            #     have already finished the possible combinations.
            # - It is not considering the whole word; it can only replace the part found.# noqa E501
            line_pull = []
            v_seq = 0
            for index, row in DF_KY_WD_TEMP.iterrows():
                # In this first IF, find part or all of the word.
                if line[0].find(row["word"]) != -1:
                    if v_msm:
                        print(
                            "  --> First loop, interaction number: ",
                            v_seq,
                            " found: ",
                            row["word"],
                        )  # noqa E501

                    # In this second IF, it checks if the term is complete, avoiding associating  # noqa E501
                    # only part of a word from the list with a KeyWord record # noqa E501
                    # if 1==1:  # Disable this check
                    if re.search(r"\b" + row["word"] + r"\b", line[0]):
                        if v_msm:
                            print("  --> Second loop found: ", row["word"])
                            print("  --> String before association: ", line[0])

                        v_key = str(row["term_id__term"])
                        line[0] = line[0].replace(row["word"], "")
                        line_pull.append(v_key)
                        if v_msm:
                            print("    --> Associated term: ", v_key)
                            print(
                                "    --> Word or Words Associated: ",
                                row["word"],  # noqa E501
                            )  # noqa E501
                            print(
                                "    --> String after association: ", line[0]
                            )  # noqa E501
                            print("    --> End of interation: ", v_seq)
                            print(" ")

                        tm_end = datetime.now() - tm_start
                        df_mapper.loc[v_iloc] = [
                            v_row,
                            v_line,
                            row["word"],
                            v_key,
                            row["term_id"],
                            row["term_id__description"],
                            len(DF_KY_WD_TEMP.index),
                            v_seq,
                            tm_end,
                        ]
                        v_chk = True  # Set True to NOT create a empty output row # noqa E501
                        v_iloc += 1
                v_seq += 1

            # Did not find any term in the process, add a EMPTY line in df_mapper. # noqa E501
            if v_chk is False:
                tm_end = datetime.now() - tm_start
                df_mapper.loc[v_iloc] = [
                    v_row,
                    v_line,
                    "",
                    "",
                    "",
                    "",
                    len(DF_KY_WD_TEMP.index),
                    v_seq,
                    tm_end,
                ]
                v_iloc += 1
            if v_msm:
                print("--> Remaininf String: ", line[0])
            # line_pull = " ".join(str(x) for x in set(line_pull)) # Process A
            # line[0] = line_pull # Process A
        except Exception as e:
            if v_msm:
                print("Unable to process registration", idx, line, e)
                line[0] = "ERROR ON PROCESS "
            # ERROR: Output line will be created with the error message
            tm_end = datetime.now() - tm_start
            df_mapper.loc[v_iloc] = [
                v_row,
                v_line,
                "error",
                "error",
                "error",
                "error",
                "error",
                "error",
                tm_end,
            ]
            v_iloc += 1
    # lines_return = pd.DataFrame(lines) # Process A
    if v_msm:
        print(df_mapper)
    # return lines_return, df_mapper # process A
    return df_mapper


def word_to_term(path=None):
    """
    Perform a search for terms from a string base with the same ETL engine.

    Parameters
    ----------
    - path: str
        File with the strings for conversion into terms
        Only the first column of the file will be processed

    Return
    ------
    A file will be generated with the results in the same folder as the input
    strings file

    Examples:
    >>> from igem.ge import filter
    >>> filter.word_to_term(
            path='../../file.csv'
            )
    """

    v_input = path.lower()
    v_file_out = os.path.dirname(v_input) + "/output_word_to_term.csv"

    if v_input is None:
        print("  Inform the path to load")
        sys.exit(2)
    if not os.path.isfile(v_input):
        print("  File not found")
        print("  Inform the path and the file in CSV format to load")
        sys.exit(2)
    print("Start: Search for WORDS to term")

    # Only keywords with status and commute true
    # KeyWord table search the relationships between active words and key
    global DF_KY_WD
    DF_KY_WD = pd.DataFrame(
        list(
            WordTerm.objects.values(
                "word", "term_id", "term_id__term", "term_id__description"
            )
            .filter(status=True)
            .order_by("word")
        )
    )

    # Check Keyword has data
    if DF_KY_WD.empty:
        print("  No data on the relationship words and term")
        sys.exit(2)

    # Read file with list of WORD to search (each row will consider as a string) # noqa E501
    DF_INPUT = pd.read_csv(v_input, index_col=False)
    v_row = len(DF_INPUT.index)
    print("    Rows to process: %s rows" % v_row)
    print("    Rows on KeyWord: %s rows" % len(DF_KY_WD.index))

    df_combiner = pd.DataFrame()
    df_reducer = pd.DataFrame()

    df_temp = DF_INPUT.apply(
        lambda x: x.astype(str).str.lower()
    )  # Keep all words lower case to match
    list_df = np.array_split(
        df_temp, os.cpu_count() - 1
    )  # Convert to n list depend on number of cores

    # Multiprocess with dinamic distribution
    try:
        with ThreadPoolExecutor() as executor:
            future = {
                executor.submit(search_terms, list_df[i])
                for i in range(len(list_df))  # noqa E501
            }

        for future_to in as_completed(future):
            df_combiner = future_to.result()
            # data = future_to.result() # with has more than 1 return from MAPPER  # noqa E501
            # df_combiner = pd.DataFrame(data[0]) # with has more than 1 return from MAPPER  # noqa E501
            # df_mapper = pd.DataFrame(data[1]) # with has more than 1 return from MAPPER  # noqa E501
            df_reducer = pd.concat(
                [df_reducer, df_combiner], axis=0
            )  # trocar pot lista para ganhar performance / exemplo ncbi  # noqa E501
    except Exception as e:
        print("      Error on search multiprocess: ", e)

    df_reducer.sort_values(by=["row"], ascending=True, inplace=True)
    df_reducer.reset_index(drop=True, inplace=True)

    df_reducer = df_reducer[
        [
            "row",
            "string",
            "word",
            "term_id",
            "term",
            "term_descr",
            "qtd_terms",
            "qtd_loops",
            "time",
        ]
    ]
    df_reducer.to_csv(v_file_out, index=False)
    print("File with the results sucessfully created in %s" % str(v_file_out))

    return True
