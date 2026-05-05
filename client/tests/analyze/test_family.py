"""Tests for igem.modules.analyze._family."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem.modules.analyze import infer_family, validate_family


class TestInferFamily:
    def test_continuous_numeric_is_linear(self):
        s = pd.Series([1.5, 2.3, 3.1, 4.0])
        assert infer_family(s) == "linear"

    def test_binary_int_is_logistic(self):
        s = pd.Series([0, 1, 1, 0, 1])
        assert infer_family(s) == "logistic"

    def test_binary_float_is_logistic(self):
        s = pd.Series([0.0, 1.0, 0.0, 1.0])
        assert infer_family(s) == "logistic"

    def test_bool_is_logistic(self):
        s = pd.Series([True, False, True, True])
        assert infer_family(s) == "logistic"

    def test_three_unique_values_is_linear(self):
        s = pd.Series([0, 1, 2, 0, 1])
        assert infer_family(s) == "linear"

    def test_ignores_nan_when_inferring(self):
        s = pd.Series([0, 1, np.nan, 1, 0])
        assert infer_family(s) == "logistic"

    def test_object_dtype_raises(self):
        with pytest.raises(ValueError, match="cannot infer"):
            infer_family(pd.Series(["A", "B", "A"]))

    def test_all_nan_raises(self):
        with pytest.raises(ValueError, match="no non-NaN"):
            infer_family(pd.Series([np.nan, np.nan]))


class TestValidateFamily:
    def test_accepts_known(self):
        validate_family("linear")
        validate_family("logistic")

    def test_rejects_unknown(self):
        with pytest.raises(ValueError, match="family"):
            validate_family("poisson")
