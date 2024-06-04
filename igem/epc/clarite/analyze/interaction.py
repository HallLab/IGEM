# your_library.py
# your_library.py
import concurrent.futures
import os
from typing import List, Optional, Union

import clarite
import pandas as pd


def exe_pairwise(
    data: pd.DataFrame,
    outcomes: Union[str, List[str]],
    covariates: Optional[Union[str, List[str]]] = None,
    report_betas: bool = False,
    min_n: int = 200,
    process_num: Optional[int] = None,
):

    # define variables
    results = []
    combinations = []
    # processed_combinations: set[str] = set()

    # Define the number of worker processes
    if process_num is None:
        # Set according to the number of available cores
        num_workers = os.cpu_count()
    else:
        num_workers = process_num

    # Create a set of exposomes
    exposomes_set = set(data.columns) - set(outcomes) - set(covariates or [])
    # Convert the exposomes set to a list
    exposomes = list(exposomes_set)

    if len(exposomes) < 2:
        raise ValueError(
            f"{len(exposomes)} exposomes not enough to run pairwise." # noqa E501
        )

    # Create a list of combinations
    for i in range(len(exposomes)):
        for j in range(i + 1, len(exposomes)):
            combinations.append([exposomes[i], exposomes[j]])
    # print(combinations)

    def _process_combination(interactions):
        e1, e2 = interactions
        regression_results = clarite.analyze.interaction_study(
            data=data,
            outcomes=outcomes,
            interactions=[(e1, e2)],
            covariates=covariates,
            report_betas=report_betas,
            min_n=min_n,
            process_num=1,
        )
        # results.append(regression_results)
        return regression_results

    # Create a ThreadPoolExecutor with the specified number of workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:  # noqa E501
        # Submit the tasks for execution
        future_to = {
            executor.submit(
                _process_combination, combination
            ): combination for combination in combinations
        }
        for future in concurrent.futures.as_completed(future_to):
            regression = future.result()
            results.append(regression)

        # # Wait for all tasks to complete
        # concurrent.futures.wait(futures)

    # Convert the results list to a DataFrame
    result_df = pd.concat(results)
    result_df = result_df.sort_values(by="LRT_pvalue")
    # result_df = result_df.sort_values(
    #     by="LRT_pvalue",
    #     key=lambda x: x.astype(float), ignore_index=False
    #     )

    return result_df


def gxe_pairwise(
    data: pd.DataFrame,
    outcomes: Union[str, List[str]],
    exposomes: Union[str, List[str]],
    genomics: Union[str, List[str]],
    covariates: Optional[Union[str, List[str]]] = None,
    report_betas: bool = False,
    min_n: int = 200,
    process_num: Optional[int] = None,
):

    # Convert exposomes and genomics to a list if they are not already
    exposomes = list(exposomes) if not isinstance(exposomes, list) else exposomes  # noqa E501
    genomics = list(genomics) if not isinstance(genomics, list) else genomics

    # check if exposomes and genomics list is not empty
    if not exposomes:
        raise ValueError("Exposomes parameter cannot be empty.")
    if not genomics:
        raise ValueError("Genomics parameter cannot be empty.")

    # define variables
    results = []
    combinations = []

    # Define the number of worker processes
    if process_num is None:
        # Set according to the number of available cores
        num_workers = os.cpu_count()
    else:
        num_workers = process_num

    # Create a list of combinations
    for exposome in exposomes:
        for genomic in genomics:
            combinations.append([exposome, genomic])

    # Call Interaction function for each GxE
    def _process_combination(interactions):
        e1, e2 = interactions
        regression_results = clarite.analyze.interaction_study(
            data=data,
            outcomes=outcomes,
            interactions=[(e1, e2)],
            covariates=covariates,
            report_betas=report_betas,
            min_n=min_n,
            process_num=1,
        )
        # results.append(regression_results)
        return regression_results

    # Create a ThreadPoolExecutor with the specified number of workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:  # noqa E501
        # Submit the tasks for execution
        future_to = {
            executor.submit(
                _process_combination, combination
            ): combination for combination in combinations
        }
        for future in concurrent.futures.as_completed(future_to):
            regression = future.result()
            results.append(regression)

    # Convert the results list to a DataFrame
    result_df = pd.concat(results)
    result_df = result_df.sort_values(by="LRT_pvalue")

    return result_df
