from typing import Any, List, Optional, Tuple, Union

import clarite
import pandas as pd


def association_study(
    data: pd.DataFrame,
    outcomes: Union[str, List[str]],
    regression_variables: Optional[Union[str, List[str]]] = None,
    covariates: Optional[Union[str, List[str]]] = None,
    regression_kind: Optional[Union[str, List[str]]] = None,
    encoding: str = "additive",
    edge_encoding_info: Optional[pd.DataFrame] = None,
    **kwargs,
):
    df_result = clarite.analyze.association_study(
        data,
        outcomes,
        regression_variables,
        covariates,
        regression_kind,
        encoding,
        edge_encoding_info,
        **kwargs,
    )
    return df_result


def ewas(
    outcome: str,
    covariates: List[str],
    data: Any,
    regression_kind: Optional[Union[str, List[str]]] = None,
    **kwargs,
):
    df_result = clarite.analyze.ewas(
        outcome,
        covariates,
        data,
        regression_kind,
        **kwargs,
    )
    return df_result


def interaction_study(
    data: pd.DataFrame,
    outcomes: Union[str, List[str]],
    interactions: Optional[Union[List[Tuple[str, str]], str]] = None,
    covariates: Optional[Union[str, List[str]]] = None,
    encoding: str = "additive",
    edge_encoding_info: Optional[pd.DataFrame] = None,
    report_betas: bool = False,
    min_n: int = 200,
    process_num: Optional[int] = None,
):
    df_result = clarite.analyze.interaction_study(
        data,
        outcomes,
        interactions,
        covariates,
        encoding,
        edge_encoding_info,
        report_betas,
        min_n,
        process_num,
    )
    return df_result


def add_corrected_pvalues(
    data: pd.DataFrame,
    pvalue: str = "pvalue",
    groupby: Optional[Union[str, List[str]]] = None,
):
    df_result = clarite.analyze.add_corrected_pvalues(
        data,
        pvalue,
        groupby
    )
    return df_result
