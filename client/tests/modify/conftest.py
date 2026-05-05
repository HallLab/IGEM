"""Fixtures specific to igem.modules.modify tests."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem.modules.data import Phenotypes


@pytest.fixture
def transform_phen() -> Phenotypes:
    """
    Phenotype frame tailored to ``categorize`` / ``recode`` /
    ``drop_missing`` tests. BMI ranges 18..39 (8 evenly-spaced values
    for clean quantile bins), SEX uses a sentinel ``99`` for missing,
    and AGE has one NaN to exercise drop_missing.
    """
    df = pd.DataFrame(
        {
            "sample_id": [f"S{i}" for i in range(8)],
            "BMI": [18.0, 21.0, 24.0, 27.0, 30.0, 33.0, 36.0, 39.0],
            "SEX": [1, 2, 1, 2, 1, 2, 99, 2],
            "GLUCOSE": [80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0, 150.0],
            "AGE": [25.0, 35.0, 45.0, 55.0, 65.0, 75.0, 85.0, np.nan],
        }
    )
    return Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=["GLUCOSE"],
        covariates=["AGE", "SEX"],
        exposures=["BMI"],
    )
