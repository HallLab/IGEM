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
    return True
