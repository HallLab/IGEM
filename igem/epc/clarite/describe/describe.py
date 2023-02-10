import clarite
import pandas as pd


def correlations(data: pd.DataFrame, threshold: float = 0.75):
    df_result = clarite.describe.correlations(data, threshold)
    return df_result


def freq_table(data: pd.DataFrame):
    df_result = clarite.describe.freq_table(data)
    return df_result


def get_types(data: pd.DataFrame):
    df_result = clarite.describe.get_types(data)
    return df_result


def percent_na(data: pd.DataFrame):
    df_result = clarite.describe.percent_na(data)
    return df_result


def skewness(data: pd.DataFrame, dropna: bool = False):
    df_result = clarite.describe.skewness(data, dropna)
    return df_result


def summarize(data: pd.DataFrame):
    result = clarite.describe.summarize(data)
    return result
