import os
from os.path import splitext

import pandas as pd
import patoolib
from ge.utils import logger

""" Funcao em desenvolvimento """


def extractor_zip(qs, v_dir, v_source_file, log):
    try:
        print("Unzip start")  # noqa E501
        patoolib.extract_archive(
            str(v_source_file), outdir=str(v_dir), verbosity=-1
        )  # noqa E501
        os.remove(v_source_file)
        return True
    except:  # noqa E722
        logger(log, "e", f"{qs.connector}: Failed to UNZIP file")
        return False


def extractor_xml(qs, v_target_file, log):
    # TODO: This point is critical for memore consume
    file_name, ext = splitext(v_target_file)
    try:
        v_src = str(file_name + ".xml")
        DF = pd.read_xml(v_src)
        v_csv = str(v_target_file)
        DF.to_csv(v_csv, index=False)
        os.remove(v_src)
        logger(log, "s", f"{qs.connector}: XML file converted to standard IGEM")  # noqa E501
        return True
    except:  # noqa E722
        logger(log, "e", f"{qs.connector}: Failed to convert XML to CSV") # noqa E501
        # Stops this process and starts the next connector
        return False


def extractor_kegg(qs, v_source_file, v_target_file, log):
    try:
        v_source_file = v_source_file[:-3]  # cut the ".gz"
        with open(v_source_file, 'r') as file:
            data = file.read()

            # Split the data into individual records
            records = data.strip().split('///')

            # Process each record and extract the desired information
            results = []
            last_key = ''
            for record in records:
                if record == '':
                    continue

                lines = record.strip().split('\n')

                record_data = {}
                for line in lines:
                    key, value = line.split(' ', maxsplit=1)
                    if key == '':
                        record_data[last_key] = record_data[last_key] + ';' + value # noqa E501
                    else:
                        if last_key != 'REFERENCE':
                            last_key = key
                            record_data[key.strip()] = value.strip()
                        elif key != 'REFERENCE':
                            last_key = key
                            record_data[key.strip()] = value.strip()
                if 'ORGANISM' in record_data:
                    if record_data['ORGANISM'] == 'Homo sapiens (human) [GN:hsa]': # noqa E501
                        results.append(record_data)

        # Convert the list of dictionaries into a DataFrame
        df = pd.DataFrame(results)
        # Save the DataFrame as a CSV file
        df.to_csv(v_target_file, index=False)
        return True
    except:  # noqa E722
        logger(log, "e", f"{qs.connector}: Failed to convert KEGG to CSV") # noqa E501
        # Stops this process and starts the next connector
        return False
