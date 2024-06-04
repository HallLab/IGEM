"""
Analyze
========

Functions used for analyses such as EWAS

  .. autofunction:: association_study
  .. autofunction:: interaction_study
  .. autofunction:: add_corrected_pvalues

"""

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
    """
    Run an association study (EWAS, PhEWAS, GWAS, GxEWAS, etc)

    Individual regression classes selected with `regression_kind` may work
    slightly differently. Results are sorted in order of increasing `pvalue`

    Parameters
    ----------
    data: pd.DataFrame
        Contains all outcomes, regression_variables, and covariates
    outcomes: str or List[str]
        The exogenous variable (str) or variables (List) to be used as the
        output of each regression.
    regression_variables: str, List[str], or None
        The endogenous variable (str) or variables (List) to be used
        invididually as inputs into regression.
        If None, use all variables in `data` that aren't an outcome or a
        covariate
    covariates: str, List[str], or None (default)
        The variable (str) or variables (List) to be used as covariates in
        each regression.
    regression_kind: None, str or subclass of Regression
        This can be 'glm', 'weighted_glm', or 'r_survey' for built-in
        Regression types, or a custom subclass of Regression.  If None, it is
        set to 'glm' if a survey design is not specified and 'weighted_glm'
        if it is.
    kwargs: Keyword arguments specific to the Regression being used

    Returns
    -------
    df: pd.DataFrame
        Association Study results DataFrame with at least these columns: ['N',
        'pvalue', 'error', 'warnings'].
        Indexed by the outcome variable and the variable being assessed in
        each regression

    Examples
    --------

    >>> import igem
    >>> results = igem.epc.analyze.association_study(
                    outcomes="HI_CHOL",
                    covariates=["race", "agecat"],
                    data=df,
                    standardize_data=True,
                    )

    """

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
    """
    Run an Environment-Wide Association Study

    All variables in `data` other than the outcome (outcome) and covariates
    are tested individually. Individual regression classes selected with
    `regression_kind` may work slightly differently.
    Results are sorted in order of increasing `pvalue`

    Parameters
    ----------
    outcome: string
        The variable to be used as the output of the regressions
    covariates: list (strings),
        The variables to be used as covariates.  Any variables in the
        DataFrames not listed as covariates are regressed.
    data: Any, usually pd.DataFrame
        The data to be analyzed, including the outcome, covariates, and any
        variables to be regressed.
    regression_kind: str or subclass of Regression
        This can be 'glm', 'weighted_glm', or 'r_survey' for built-in
        Regression types, or a custom subclass of Regression
        None by default to maintain existing api (`glm` unless
        SurveyDesignSpec exists, in which case `weighted_glm`)
    kwargs: Keyword arguments specific to the Regression being used

    Returns
    -------
    df: pd.DataFrame
        EWAS results DataFrame with at least these columns: ['N', 'pvalue',
        'error', 'warnings']
        indexed by the outcome and the variable being assessed in each row

    Examples
    --------
    >>> ewas_discovery = igem.epc.analyze.ewas(
        "logBMI", covariates, nhanes_discovery
        )
    Running on a continuous variable
    """
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
    """Perform LRT tests comparing a model with interaction terms to one
    without.

    An intercept, covariates, and main effects of the variables used in the
    interactiona are included in both the full and restricted models.
    All variables in `data` other than the outcome and covariates are
    potential interaction variables.
    All pairwise interactions are tested unless specific.
    Results are sorted in order of increasing `pvalue`.

    Parameters
    ----------
    data: pd.DataFrame
        The data to be analyzed, including the outcome, covariates, and any
        variables to be regressed.
    outcomes: str or List[str]
        The exogenous variable (str) or variables (List) to be used as the
        output of each regression.
    interactions: list(tuple(strings)), str, or None
        Valid variables are those in the data that are not an outcome variable
        or a covariate.
        None: Test all pairwise interactions between valid variables
        String: Test all interactions of this valid variable with other valid
        variables
        List of tuples: Test specific interactions of valid variables
    covariates: str, List[str], or None (default)
        The variable (str) or variables (List) to be used as covariates in
        each regression.
    encoding: str, default "additive""
        Encoding method to use for any genotype data.  One of {'additive',
        'dominant', 'recessive', 'codominant', or 'edge'}
    edge_encoding_info: Optional pd.DataFrame, default None
        If edge encoding is used, this must be provided.
        See Pandas-Genomics documentation on edge encoding.
    report_betas: boolean
        False by default.
          If True, the results will contain one row for each interaction term
          and will include the beta value, standard error (SE), and beta
          pvalue for that specific interaction. The number of terms increases
          with the number of categories in each interacting variable.
    min_n: int or None
        Minimum number of complete-case observations (no NA values for outcome,
        covariates, or variable). Defaults to 200
    process_num: Optional[int]
        Number of processes to use when running the analysis, default is None
        (use the number of cores)

    Returns
    -------
    df: pd.DataFrame
        DataFrame with these columns: ['Test_Number', 'Converged', 'N', 'Beta',
        'SE', 'Beta_pvalue', 'LRT_pvalue']
        indexed by the interaction terms ("Term1", "Term2") and the outcome
        variable ("Outcome")
    """

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
    """
    Calculate bonferroni and FDR pvalues and sort by increasing FDR (in-place).
    Rows with a missing pvalue are not counted as a test.

    Parameters
    ----------
    data:
        A dataframe that will be modified in-place to add corrected pvalues
    pvalue:
        Name of a column in data that the calculations will be based on.
    groupby:
        A name or list of names of columns (including index columns) that will
        be used to group rows before performing calculations. This is meant to
        be used when multiple rows are present with repeated pvalues based on
        the same test. This will reduce the number of tests.  For example,
        grouping by ["Term1", "Term2"] in interaction results to apply
        corrections to the LRT_pvalue when betas are reported (which creates
        more rows than the number of tests).

    Returns
    -------
    None

    Examples
    --------
    >>> igem.epc.analyze.add_corrected_pvalues(ewas_discovery)

    >>> igem.epc.analyze.add_corrected_pvalues(
                    interaction_result,
                    pvalue='Beta_pvalue'
                    )

    >>> igem.epc.analyze.add_corrected_pvalues(
                    interaction_result,
                    pvalue='LRT_pvalue',
                    groupby=["Term1", "Term2"]
                    )
    """

    df_result = clarite.analyze.add_corrected_pvalues(data, pvalue, groupby)
    return df_result
