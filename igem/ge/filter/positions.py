import ast
import os

import pandas as pd
from django.db import models
from django.db.models import (
    Case,
    CharField,
    Count,
    ExpressionWrapper,
    F,
    Sum,
    Value,
    When,
)

# from django.db.models.functions import Coalesce
from ge.models import TermMap
from pyensembl import EnsemblRelease


# Define a function to calculate gene symbols
def calculate_gene_symbols(row, ensembl, boundaries, total_rows):
    # DEBUG: Show row number
    print(row.name)

    try:
        chromosome_intern = int(row['chromosome'])
    except ValueError:
        if row['chromosome'].startswith('chr'):
            if row['chromosome'] == 'chrX':
                chromosome_intern = 23
            elif row['chromosome'] == 'chrY':
                chromosome_intern = 24
            else:
                chromosome_intern = int(row['chromosome'][3:])
        else:
            # Handle other cases or raise an error if needed
            ...

    position = int(row['position'])

    # Query for genes at the given position
    genes = ensembl.genes_at_locus(chromosome_intern, position)

    if not genes and boundaries != 0:
        start_position = max(0, position - boundaries)

        # Get all genes for the chromosome
        # Start with a single position
        genes = ensembl.genes_at_locus(chromosome_intern, start_position)

    gene_symbols = []
    if genes:
        # Extract gene information
        for gene in genes:
            # print(f"Gene ID: {gene.gene_id}, Gene Name: {gene.gene_name}")  # noqa E501
            gene_symbols.append(str(gene.gene_name).lower())

    # pbar.close()
    return gene_symbols


def process_position(args):
    chromosome, position, gene_symbols, reference_name, term_data = args
    # Create an empty list to store the results
    results = []

    if gene_symbols:
        # Extract gene information
        for gene_simbol in gene_symbols:
            # Find the GeneMap entry based on the gene symbol
            try:
                gene_map_entry = term_data.loc[gene_simbol]
            except KeyError:
                # Handle the case where the symbol isn't found
                result_dict = {
                    "input_chromosome": chromosome,
                    "input_position": position,
                    "assembly_used": reference_name,
                    "gene_id": "",
                    "gene_symbol": gene_simbol,
                    "gene_term_id": "",
                    "interaction_term_id": "",
                    "interaction_term_name": "",
                    "interaction_term_category": "",
                    "observation": "Gene not found in IGEM"
                }
                results.append(result_dict)
                continue

            # Now access and process the 'term_map_entries' which match
            # the criteria.
            try:
                for index, term_map in gene_map_entry.iterrows():

                    if term_map["term_1__term_category__term_category"] == 'gene' and term_map["term_2__term_category__term_category"] == 'gene': # noqa E501
                        result_dict = {
                            "input_chromosome": chromosome,
                            "input_position": position,
                            "assembly_used": reference_name,
                            "gene_id": "",
                            "gene_symbol": gene_simbol,
                            "gene_term_id": "",
                            "interaction_term_id": "",
                            "interaction_term_name": "",
                            "interaction_term_category": "",
                            "observation": "both terms are genes"
                        }
                        results.append(result_dict)
                        continue

                    elif term_map["term_1__term_category__term_category"] == 'gene': # noqa E501
                        result_dict = {
                            "input_chromosome": chromosome,
                            "input_position": position,
                            "assembly_used": reference_name,
                            "gene_id": term_map["term_1__term"],
                            "gene_symbol": gene_simbol,
                            "gene_term_id": term_map["term_1__description"],
                            "interaction_term_id": term_map["term_2__term"],
                            "interaction_term_name": term_map["term_2__description"], # noqa E501
                            "interaction_term_category": term_map["term_2__term_category__term_category"], # noqa E501
                            "observation": ""
                        }
                        results.append(result_dict)
                        continue


                    elif term_map["term_2__term_category__term_category"] == 'gene': # noqa E501
                        result_dict = {
                            "input_chromosome": chromosome,
                            "input_position": position,
                            "assembly_used": reference_name,
                            "gene_id": term_map["term_2__term"],
                            "gene_symbol": gene_simbol,
                            "gene_term_id": term_map["term_2__description"],
                            "interaction_term_id": term_map["term_1__term"],
                            "interaction_term_name": term_map["term_1__description"], # noqa E501
                            "interaction_term_category": term_map["term_1__term_category__term_category"], # noqa E501
                            "observation": ""
                        }
                        results.append(result_dict)
                        continue

                    # TODO: Maybe this logic is not necessary
                    else: # noqa E501
                        result_dict = {
                            "input_chromosome": chromosome,
                            "input_position": position,
                            "assembly_used": reference_name,
                            "gene_id": "",
                            "gene_symbol": gene_simbol,
                            "gene_term_id": "",
                            "interaction_term_id": "",
                            "interaction_term_name": "",
                            "interaction_term_category": "",
                            "observation": "None of the terms are genes"
                        }
                        results.append(result_dict)
                        continue
            except Exception as e:
                result_dict = {
                    "input_chromosome": chromosome,
                    "input_position": position,
                    "assembly_used": reference_name,
                    "gene_id": "",
                    "gene_symbol": "",
                    "gene_term_id": "",
                    "interaction_term_id": "",
                    "interaction_term_name": "",
                    "interaction_term_category": "",
                    "observation": f"Error: {e}"
                }
                results.append(result_dict)

    else:
        result_dict = {
            "input_chromosome": chromosome,
            "input_position": position,
            "assembly_used": reference_name,
            "gene_id": "",
            "gene_symbol": "",
            "gene_term_id": "",
            "interaction_term_id": "",
            "interaction_term_name": "",
            "interaction_term_category": "",
            "observation": "No genes found at this position"
        }
        results.append(result_dict)

    print(chromosome, " - ", position)

    return results


def positions_to_term(
        input_df,
        exposomes,
        assembly=38,
        boundaries=0,
        delimiter=None,
        has_header=True
        ):

    # Validate input fields
    # Check if the input_df is a valid file
    if not os.path.isfile(input_df):
        raise ValueError(f"Input file '{input_df}' does not exist.")

    # Check if assembly is a valid number
    try:
        assembly = int(assembly)
    except ValueError:
        raise ValueError("Assembly must be a valid number.")

    # Check if search_range is a valid number
    try:
        search_range = int(boundaries)
    except ValueError:
        raise ValueError("Search range must be a valid number.")

    # Create an empty list to store the results
    # results = []

    # Define Assemble
    if assembly == 38:
        # Specify the Ensembl release version (e.g., GRCh38)
        ensembl = EnsemblRelease(99)
    elif assembly == 37:
        # Specify the Ensembl release version (e.g., GRCh37)
        ensembl = EnsemblRelease(75)
    else:
        raise ValueError("Ensembl not found")

    # Print and set parameters
    print(f"Input file: {input_df}")
    print(f"Assembly: {assembly}")
    print(f"Search range: {search_range}")
    print(ensembl)
    reference_name = ensembl.reference_name
    input_directory = os.path.dirname(input_df)
    # chunk_size = 50000
    # vcpu = os.cpu_count()
    # vcpu = 11

    # Read the files:
    # Define default values for delimiter and header
    if delimiter is None:
        delimiter = ','  # Default delimiter
    if has_header:
        header = 'infer'  # Infer header from the file
    else:
        header = None  # No header in the file

    # OBSERVATION:
    # because pyensembl use the SQLite too, we need to search genes here
    # Apply the calculate_gene_symbols function to each row and create a new column 'gene_symbols'  # noqa E501

    # From Chrom/Position and Ensembl get Genes
    # If the file was process, use it, else search genes
    output_filename = f"result_1_genes_{os.path.basename(input_df)}"
    output_path = os.path.join(input_directory, output_filename)
    if os.path.isfile(output_path):
        df = pd.read_csv(output_path)
        total_rows = len(df)
        # convert gene_symbols string to list (import to explode)
        df['gene_symbols'] = df['gene_symbols'].apply(lambda x: ast.literal_eval(x))  # noqa E501

    else:
        # Read file using pandas with flexible delimiter and header handling
        df = pd.read_csv(
            input_df,
            sep=delimiter,
            header=header,
            engine='python'
            )

        # Check if the DataFrame has the expected columns or assume column
        if 'chromosome' not in df.columns and 'position' not in df.columns:
            if len(df.columns) >= 2:
                df.columns = ['chromosome', 'position']
            else:
                raise ValueError("Input file must have 'chromosome' and 'position' columns.") # noqa E501

        total_rows = len(df)
        df['gene_symbols'] = df.apply(
            calculate_gene_symbols,
            axis=1,
            args=(ensembl, boundaries, total_rows)
            )
        # Save list of chrom / position / genes
        df.to_csv(output_path)
    # Create a clean df with only genes
    expanded_df = df.explode('gene_symbols')
    expanded_df = expanded_df[['gene_symbols']].copy()
    expanded_df.drop_duplicates(subset='gene_symbols', inplace=True)
    expanded_df.dropna(subset=['gene_symbols'], inplace=True)
    print(len(expanded_df), "genes were found in the input list")

    # READ EXPOSOME LIST PRE FILTER
    # The file need to be 1-String, 2-IGEM Term
    df_exposomes = pd.read_csv(exposomes)
    # Check if the DataFrame has the expected columns or assume column names
    if 'string' not in df_exposomes.columns and 'term' not in df_exposomes.columns:  # noqa E501
        if len(df_exposomes.columns) >= 2:
            df_exposomes.columns = ['string', 'term']
        else:
            raise ValueError("Input file for Exposomes must have 'string' and 'term' columns.") # noqa E501

    # If the Terms Interactions was previous process, use it:
    output_filename = f"result_2_terms_{os.path.basename(input_df)}"
    output_path = os.path.join(input_directory, output_filename)
    if os.path.isfile(output_path):
        term_data = pd.read_csv(output_path)
    else:
        # Create a queryset for TermMap with term_1 having term_category='gene'
        queryset = TermMap.objects.filter(
            models.Q(term_1__term_category_id=5) | models.Q(term_2__term_category_id=5)  # noqa E501
            ).values(
                'term_1',
                'term_2',
                'term_1__term',
                'term_2__term',
                'term_1__description',
                'term_2__description',
                'term_1__term_category__term_category',
                'term_2__term_category__term_category',
            )

        # Use Case/When to add the symbol from GeneMap if it exists
        queryset = queryset.annotate(
            total_qtd_links=Sum('qtd_links'),
            connector_count=Count('connector_id', distinct=True),
            gene_symbol=Case(
                When(
                    term_1__term_category__term_category='gene',
                    then=ExpressionWrapper(
                        F('term_1__genemap__symbol'),
                        output_field=CharField()
                        )
                    ),
                When(
                    term_2__term_category__term_category='gene',
                    then=ExpressionWrapper(
                        F('term_2__genemap__symbol'),
                        output_field=CharField()
                        )
                    ),
                # When(
                #     term_1__term_category__term_category='gene',
                #     then=Coalesce('term_1__genemap__symbol', Value(''))
                # ),
                # When(
                #     term_2__term_category__term_category='gene',
                #     then=Coalesce('term_2__genemap__symbol', Value(''))
                # ),
                default=Value(''),
                output_field=CharField()
            )
        )
        # Convert to DataFrame
        term_data = pd.DataFrame(list(queryset))

        # Set the index to 'gene_symbol' if it's not already set
        term_data.set_index(['gene_symbol'], inplace=True)
        # if 'gene_symbol' not in term_data.columns:
        #     term_data.set_index(['gene_symbol'], inplace=True)

        # Memory used
        # mem_usage_mb = (term_data.memory_usage(deep=True).sum()) / 1048576

        # Keep in term_data only term with genes found
        term_data = term_data[
            term_data.index.isin(expanded_df['gene_symbols'])
            ]

        # Normalize the df for better read / preparation for Expome filter
        # Create new columns
        term_data['Gene ID'] = ""
        term_data['Gene Term'] = ""
        term_data['Gene Description'] = ""
        term_data['Exposome ID'] = ""
        term_data['Exposome Term'] = ""
        term_data['Exposome Description'] = ""
        term_data['Exposome Category'] = ""
        term_data['Qtd Links'] = ""
        term_data['Qtd Sources'] = ""

        # Create boolean masks for gene and non-gene categories
        gene_mask = term_data['term_1__term_category__term_category'] == 'gene'

        # Create a mask for swapping the columns when the condition is False
        swap_mask = ~gene_mask

        # Assign values using loc
        term_data.loc[gene_mask, 'Gene ID'] = term_data.loc[gene_mask, 'term_1']  # noqa E501
        term_data.loc[gene_mask, 'Gene Term'] = term_data.loc[gene_mask, 'term_1__term']  # noqa E501
        term_data.loc[gene_mask, 'Gene Description'] = term_data.loc[gene_mask, 'term_1__description']  # noqa E501
        term_data.loc[gene_mask, 'Exposome ID'] = term_data.loc[gene_mask, 'term_2']  # noqa E501
        term_data.loc[gene_mask, 'Exposome Term'] = term_data.loc[gene_mask, 'term_2__term']  # noqa E501
        term_data.loc[gene_mask, 'Exposome Description'] = term_data.loc[gene_mask, 'term_2__description']  # noqa E501
        term_data.loc[gene_mask, 'Exposome Category'] = term_data.loc[gene_mask, 'term_2__term_category__term_category']  # noqa E501
        term_data.loc[gene_mask, 'Qtd Links'] = term_data.loc[gene_mask, 'total_qtd_links']  # noqa E501
        term_data.loc[gene_mask, 'Qtd Sources'] = term_data.loc[gene_mask, 'connector_count']  # noqa E501

        term_data.loc[swap_mask, 'Gene ID'] = term_data.loc[swap_mask, 'term_2']  # noqa E501
        term_data.loc[swap_mask, 'Gene Term'] = term_data.loc[swap_mask, 'term_2__term']  # noqa E501
        term_data.loc[swap_mask, 'Gene Description'] = term_data.loc[swap_mask, 'term_2__description']  # noqa E501
        term_data.loc[swap_mask, 'Exposome ID'] = term_data.loc[swap_mask, 'term_1']  # noqa E501
        term_data.loc[swap_mask, 'Exposome Term'] = term_data.loc[swap_mask, 'term_1__term']  # noqa E501
        term_data.loc[swap_mask, 'Exposome Description'] = term_data.loc[swap_mask, 'term_1__description']  # noqa E501
        term_data.loc[swap_mask, 'Exposome Category'] = term_data.loc[swap_mask, 'term_1__term_category__term_category']  # noqa E501
        term_data.loc[swap_mask, 'Qtd Links'] = term_data.loc[swap_mask, 'total_qtd_links']  # noqa E501
        term_data.loc[swap_mask, 'Qtd Sources'] = term_data.loc[swap_mask, 'connector_count']  # noqa E501

        # Drop old columns
        term_data.drop(columns=[
            'term_1',
            'term_2',
            'term_1__term',
            'term_2__term',
            'term_1__description',
            'term_2__description',
            'term_1__term_category__term_category',
            'term_2__term_category__term_category',
            'total_qtd_links',
            'connector_count',
            ], inplace=True)

        # TODO: Apply filter
        # string e term # noqa E501
        # filtered_term_data = term_data[term_data['exposome term'].isin(df_exposomes['term'])] # noqa E501
        # term_data = term_data.merge(df_exposomes[['term', 'string']], left_on='Exposome Term', right_on='term', how='inner')  # noqa E501
        term_data = term_data.reset_index().merge(df_exposomes[['term', 'string']], left_on='Exposome Term', right_on='term', how='inner').set_index('gene_symbol')   # noqa E501

        # Save list of terms interaction filtered
        output_filename = f"result_2_terms_{os.path.basename(input_df)}"
        output_path = os.path.join(input_directory, output_filename)
        term_data.to_csv(output_path)

    print(len(term_data), "term interactions were found in the input list")

    df = df.explode('gene_symbols')
    try:
        result_df = df.merge(term_data, left_on='gene_symbols', right_index=True, how='left')  # noqa E501
    except Exception as e:
        print(e)
        result_df = df.merge(term_data, left_on='gene_symbols', right_on='gene_symbol', how='left')  # noqa E501
    result_df['assembly'] = reference_name

    # Define the output file name with the prefix
    output_filename = f"result_3_interactions_{os.path.basename(input_df)}"
    output_path = os.path.join(input_directory, output_filename)
    result_df.to_csv(output_path)

    # create the model
    # chrom:position x Word
    result_df['chromosome:position'] = result_df['chromosome'] + ':' + result_df['position'].astype(str)  # noqa E501
    result_df = result_df[[
        'chromosome:position',
        'string',
        'Qtd Links',
        'Qtd Sources'
        ]]
    result_df.dropna(subset=['string'], inplace=True)

    output_filename = f"result_4_model_{os.path.basename(input_df)}"
    output_path = os.path.join(input_directory, output_filename)
    result_df.to_csv(output_path, index=False)

    return True
