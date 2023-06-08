"""
Survey
========

Complex survey design

    .. autoclass:: SurveyDesignSpec

"""

from typing import Dict, Optional, Union

import clarite
import pandas as pd


def SurveyDesignSpec(
    survey_df: pd.DataFrame,
    strata: Optional[str] = None,
    cluster: Optional[str] = None,
    nest: bool = False,
    weights: Union[str, Dict[str, str]] = None,
    fpc: Optional[str] = None,
    single_cluster: Optional[str] = "fail",
    drop_unweighted: bool = False,
):
    """
    Holds parameters for building a statsmodels SurveyDesign object

    Parameters
    ----------
    survey_df: pd.DataFrame
        A DataFrame containing Cluster, Strata, and/or weights data.
        This should include all observations in the data analyzed using it
        (matching via index value)
    strata: string or None
        The name of the strata variable in the survey_df
    cluster: string or None
        The name of the cluster variable in the survey_df
    nest: bool, default False
        Whether or not the clusters are nested in the strata (The same cluster
        IDs are repeated in different strata)
    weights: string or dictionary(string:string)
        The name of the weights variable in the survey_df, or a dictionary
        mapping variable names to weight names
    fpc: string or None
        The name of the variable in the survey_df that contains the finite
        population correction information.
        This reduces variance when a substantial portion of the population is
        sampled.
        May be specified as the total population size, or the fraction of the
        population that was sampled.
    single_cluster: {'fail', 'adjust', 'average', 'certainty'}
        Setting controlling variance calculation in single-cluster
        ('lonely psu') strata
        'fail': default, throw an error
        'adjust': use the average of all observations (more conservative)
        'average': use the average value of other strata
        'certainty': that strata doesn't contribute to the variance (0 var)
    drop_unweighted: bool, default False
        If True, drop observations that are missing a weight value.  This may
        not be statistically sound. Otherwise the result for variables with
        missing weights (when the variable is not missing) is NULL.

    Attributes
    ----------

    Examples
    --------
    >>> import igem
    >>> igem.epc.analyze.SurveyDesignSpec(survey_df=survey_design_replication,
                                         strata="SDMVSTRA",
                                         cluster="SDMVPSU",
                                         nest=True,
                                         weights=weights_replication,
                                         fpc=None,
                                         single_cluster='fail')
    """

    df_result = clarite.survey.SurveyDesignSpec(
        survey_df,
        strata,
        cluster,
        nest,
        weights,
        fpc,
        single_cluster,
        drop_unweighted,
    )
    return df_result


def SurveyModel():
    """

    Parameters
    -------
    design : Instance of class SurveyDesign
    model_class : Instance of class GLM
    init_args : Dictionary of arguments
        when initializing the model
    fit_args : Dictionary of arguments
        when fitting the model

    Attributes
    ----------
    design : Instance of class SurveyDesign
    model : Instance of class GLM
    init_args : Dictionary of arguments
        when initializing the model
    fit_args : Dictionary of arguments
        when fitting the model
    params : (p, ) array
        Array of coefficients of model
    vcov : (p, p) array
        Covariance matrix
    stderr : (p, ) array
        Standard error of cofficients
    """

    return True
