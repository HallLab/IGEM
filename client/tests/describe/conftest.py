"""Fixtures for igem.modules.describe tests."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem.modules.data import Phenotypes


@pytest.fixture
def describe_phen() -> Phenotypes:
    """
    Wide phenotype frame with controlled values so summary statistics
    can be checked exactly: a clean continuous column, a column with a
    known fraction of missing values, a low-cardinality categorical,
    and a high-cardinality categorical.
    """
    df = pd.DataFrame(
        {
            "sample_id": [f"S{i}" for i in range(10)],
            # 0..9 → mean=4.5, min=0, max=9, q25=2.25, median=4.5, q75=6.75
            "AGE": list(range(10)),
            # 2 NaN out of 10 → 20% missing
            "BMI": [
                20.0, 22.5, 25.0, np.nan, 30.0,
                35.0, np.nan, 40.0, 28.0, 24.0,
            ],
            # Binary categorical: 6× "M", 4× "F"
            "SEX": ["M", "F", "M", "M", "F", "F", "M", "M", "F", "M"],
            # High-cardinality categorical (every row distinct)
            "ETHNICITY": [f"E{i}" for i in range(10)],
            # Constant column → n_unique=1
            "STUDY": ["NHANES"] * 10,
        }
    )
    return Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=["BMI"],
        covariates=["AGE", "SEX"],
    )
