Survey
======

Survey Design Specification
---------------------------

.. py:function:: SurveyDesignSpec(survey_df, strata=None, cluster=None, nest=False, weights=None, fpc=None, single_cluster='fail', drop_unweighted=False)
   :module: your_module_name

   Holds parameters for building a statsmodels SurveyDesign object.

   :param survey_df: A pandas DataFrame containing Cluster, Strata, and/or weights data.
   :type survey_df: pandas.DataFrame
   :param strata: The name of the strata variable in the survey_df, defaults to None.
   :type strata: str, optional
   :param cluster: The name of the cluster variable in the survey_df, defaults to None.
   :type cluster: str, optional
   :param nest: Whether or not the clusters are nested in the strata, defaults to False.
   :type nest: bool, optional
   :param weights: The name of the weights variable in the survey_df, or a dictionary mapping variable names to weight names, defaults to None.
   :type weights: str or dict, optional
   :param fpc: The name of the variable in the survey_df that contains the finite population correction information, defaults to None.
   :type fpc: str, optional
   :param single_cluster: Setting controlling variance calculation in single-cluster ('lonely psu') strata. Valid options are 'fail', 'adjust', 'average', and 'certainty'. Defaults to 'fail'.
   :type single_cluster: str, optional
   :param drop_unweighted: If True, drop observations that are missing a weight value. This may not be statistically sound. Otherwise, the result for variables with missing weights (when the variable is not missing) is NULL. Defaults to False.
   :type drop_unweighted: bool, optional
   :return: A SurveyDesignSpec object.
   :rtype: SurveyDesignSpec


Survey Model
------------

.. py:function:: SurveyModel()
   :module: your_module_name

   Creates a SurveyModel object used to fit a model to survey data.

   :return: True
   :rtype: bool
