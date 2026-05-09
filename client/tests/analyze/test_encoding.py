"""Tests for igem.modules.analyze._encoding."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem.modules.analyze._encoding import encode


def _dosage_frame() -> pd.DataFrame:
    """5 samples × 3 variants with one missing dosage on v1, sample S2."""
    return pd.DataFrame(
        {
            # v0: 0, 1, 2, 0, 1
            "v0": [0, 1, 2, 0, 1],
            # v1: 0, 1, missing, 2, 1
            "v1": [0, 1, -1, 2, 1],
            # v2: 2, 2, 0, 1, 0
            "v2": [2, 2, 0, 1, 0],
        },
        index=[f"S{i}" for i in range(5)],
        dtype=int,
    )


# ---------------------------------------------------------------------------
# additive
# ---------------------------------------------------------------------------
class TestAdditive:
    def test_passes_dosage_through(self):
        df = _dosage_frame()
        out = encode(df, method="additive")
        # v0 unchanged.
        assert out["v0"].tolist() == [0.0, 1.0, 2.0, 0.0, 1.0]

    def test_missing_becomes_nan(self):
        df = _dosage_frame()
        out = encode(df, method="additive")
        # v1[S2] is -1 → NaN.
        assert np.isnan(out.loc["S2", "v1"])

    def test_shape_preserved(self):
        df = _dosage_frame()
        out = encode(df, method="additive")
        assert out.shape == df.shape


# ---------------------------------------------------------------------------
# dominant
# ---------------------------------------------------------------------------
class TestDominant:
    def test_one_or_two_alts_become_one(self):
        df = _dosage_frame()
        out = encode(df, method="dominant")
        # v0: 0→0, 1→1, 2→1, 0→0, 1→1.
        assert out["v0"].tolist() == [0.0, 1.0, 1.0, 0.0, 1.0]

    def test_missing_propagates(self):
        df = _dosage_frame()
        out = encode(df, method="dominant")
        assert np.isnan(out.loc["S2", "v1"])


# ---------------------------------------------------------------------------
# recessive
# ---------------------------------------------------------------------------
class TestRecessive:
    def test_only_homozygous_alt_becomes_one(self):
        df = _dosage_frame()
        out = encode(df, method="recessive")
        # v0: 0→0, 1→0, 2→1, 0→0, 1→0.
        assert out["v0"].tolist() == [0.0, 0.0, 1.0, 0.0, 0.0]

    def test_missing_propagates(self):
        df = _dosage_frame()
        out = encode(df, method="recessive")
        assert np.isnan(out.loc["S2", "v1"])


# ---------------------------------------------------------------------------
# codominant
# ---------------------------------------------------------------------------
class TestCodominant:
    def test_emits_two_columns_per_variant(self):
        df = _dosage_frame()
        out = encode(df, method="codominant")
        for v in df.columns:
            assert f"{v}_het" in out.columns
            assert f"{v}_hom_alt" in out.columns

    def test_het_column_correct(self):
        df = _dosage_frame()
        out = encode(df, method="codominant")
        # v0: dosage [0, 1, 2, 0, 1] → het [0, 1, 0, 0, 1].
        assert out["v0_het"].tolist() == [0.0, 1.0, 0.0, 0.0, 1.0]

    def test_hom_alt_column_correct(self):
        df = _dosage_frame()
        out = encode(df, method="codominant")
        # v0: dosage [0, 1, 2, 0, 1] → hom_alt [0, 0, 1, 0, 0].
        assert out["v0_hom_alt"].tolist() == [0.0, 0.0, 1.0, 0.0, 0.0]

    def test_missing_propagates_to_both_columns(self):
        df = _dosage_frame()
        out = encode(df, method="codominant")
        assert np.isnan(out.loc["S2", "v1_het"])
        assert np.isnan(out.loc["S2", "v1_hom_alt"])


# ---------------------------------------------------------------------------
# edge
# ---------------------------------------------------------------------------
class TestEdge:
    def _info(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "score_0": [0.0,  0.5,  -1.0],
                "score_1": [0.5,  1.0,   0.0],
                "score_2": [1.0,  2.0,   2.5],
            },
            index=["v0", "v1", "v2"],
        )

    def test_per_variant_score_lookup(self):
        df = _dosage_frame()
        info = self._info()
        out = encode(df, method="edge", edge_encoding_info=info)
        # v0 dosage [0, 1, 2, 0, 1] → score [0.0, 0.5, 1.0, 0.0, 0.5].
        assert out["v0"].tolist() == [0.0, 0.5, 1.0, 0.0, 0.5]
        # v2 dosage [2, 2, 0, 1, 0] → score [2.5, 2.5, -1.0, 0.0, -1.0].
        assert out["v2"].tolist() == [2.5, 2.5, -1.0, 0.0, -1.0]

    def test_missing_dosage_propagates(self):
        df = _dosage_frame()
        info = self._info()
        out = encode(df, method="edge", edge_encoding_info=info)
        assert np.isnan(out.loc["S2", "v1"])

    def test_requires_info_for_every_variant(self):
        df = _dosage_frame()
        info = self._info().drop(index="v1")
        with pytest.raises(ValueError, match="missing entries"):
            encode(df, method="edge", edge_encoding_info=info)

    def test_requires_score_columns(self):
        df = _dosage_frame()
        bad = pd.DataFrame(
            {"score_0": [0.0], "score_1": [1.0]},  # no score_2
            index=["v0"],
        )
        with pytest.raises(ValueError, match="missing required columns"):
            encode(df, method="edge", edge_encoding_info=bad)

    def test_edge_without_info_raises(self):
        df = _dosage_frame()
        with pytest.raises(ValueError, match="edge_encoding_info"):
            encode(df, method="edge")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class TestEncodingValidation:
    def test_unknown_method_raises(self):
        df = _dosage_frame()
        with pytest.raises(ValueError, match="method must be"):
            encode(df, method="bogus")
