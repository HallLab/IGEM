
import sys

import pandas as pd
from django.conf import settings

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    # from ge.models import TermMap, WordMap
    from omics.models import GeneMap
except Exception as e:
    print(e)
    raise


def search_gene_map_data(input_df, assembly='grch38', search_range=10000):
    # Create an empty list to store the results
    results = []

    # Iterate through the DataFrame
    for _, row in input_df.iterrows():
        chromosome = row['chromosome']
        position = row['position']

        # Calculate the start and end positions for the search range
        start_position = position - search_range
        end_position = position + search_range

        try:
            # Query the GeneMap model to find records that match the criteria
            gene_map_data = GeneMap.objects.filter(
                assembly=assembly,
                chromosome=chromosome,
                start_position__gte=start_position,
                end_position__lte=end_position,
                term__isnull=False
            )

            # Collect the data from GeneMap records
            for qry in gene_map_data:
                result_dict = {
                    "input_chromosome": chromosome,
                    "input_position": position,
                    "assembly": qry.assembly,
                    "gene_id": qry.gene_id,
                    "symbol": qry.symbol,
                    "chromosome": qry.chromosome,
                    "nucleotide_version": qry.nucleotide_version,
                    "start_position": qry.start_position,
                    "end_position": qry.end_position,
                    "orientation": qry.orientation,
                    "term_id": qry.term.id,
                    "term_name": qry.term.term,
                    "term_description": qry.term.description
                }
                results.append(result_dict)

            if not gene_map_data:
                result_dict = {
                    "input_chromosome": chromosome,
                    "input_position": position,
                    "assembly": '',
                    "gene_id": '',
                    "symbol": '',
                    "chromosome": '',
                    "nucleotide_version": '',
                    "start_position": '',
                    "end_position": '',
                    "orientation": '',
                    "term_id": '',
                    "term_name": ''
                }
                results.append(result_dict)

        except Exception as e:
            print(e)
            # Handle any exceptions gracefully and log them
            results.append({
                "input_chromosome": chromosome,
                "input_position": position,
                "assembly": '',
                "gene_id": '',
                "symbol": '',
                "chromosome": '',
                "nucleotide_version": '',
                "start_position": '',
                "end_position": '',
                "orientation": '',
                "term_id": '',
                "term_name": ''
            })

    result_df = pd.DataFrame(results)

    return result_df
