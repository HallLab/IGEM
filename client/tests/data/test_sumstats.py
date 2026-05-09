"""Tests for igem.modules.data.sumstats (read_sumstats GWAS reader).

The tests are organised so each one demonstrates a distinct way of
calling ``read_sumstats``: by tool preset, by custom schema, by
preset + schema override, and the error paths. Reading the test
file top-to-bottom should give a working reference of supported
invocations.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from igem.modules.data import CANONICAL_COLS, read_sumstats


# ---------------------------------------------------------------------------
# Fixtures: synthetic per-tool sumstats files
# ---------------------------------------------------------------------------
@pytest.fixture
def plink2_sumstats(tmp_path):
    """
    Synthetic PLINK 2 ``.glm.linear`` (3 variants) and the canonical
    DataFrame the loader is expected to return.

    Includes an EXTRA column outside the canonical schema to verify
    that non-mapped columns are dropped.
    """
    src = pd.DataFrame(
        {
            "#CHROM": [1, 1, 2],
            "POS": [1000, 2000, 3000],
            "ID": ["rs001", "rs002", "rs003"],
            "REF": ["A", "C", "G"],
            "A1": ["T", "G", "A"],
            "OBS_CT": [500, 500, 498],
            "BETA": [0.10, -0.05, 0.20],
            "SE": [0.02, 0.03, 0.04],
            "A1_FREQ": [0.21, 0.45, 0.10],
            "P": [1e-6, 0.10, 1e-12],
            "EXTRA": ["x", "y", "z"],  # noise — must not survive
        }
    )
    path = tmp_path / "trait.glm.linear"
    src.to_csv(path, sep="\t", index=False)

    expected = pd.DataFrame(
        {
            "variant_id": src["ID"],
            "chrom": src["#CHROM"],
            "pos": src["POS"],
            "effect_allele": src["A1"],
            "other_allele": src["REF"],
            "beta": src["BETA"],
            "se": src["SE"],
            "pval": src["P"],
            "n": src["OBS_CT"],
            "eaf": src["A1_FREQ"],
        }
    )
    return path, expected


@pytest.fixture
def regenie_sumstats(tmp_path):
    """
    Synthetic REGENIE output. The native ``LOG10P`` column equals
    ``-log10(p)``; the loader must invert that back to a plain p-value
    when ``preset="regenie"``.

    Returns ``(path, original_pvals_in_plain_units)``.
    """
    pvals = np.array([1e-6, 0.10, 1e-12])
    src = pd.DataFrame(
        {
            "CHROM": [1, 1, 2],
            "GENPOS": [1000, 2000, 3000],
            "ID": ["rs001", "rs002", "rs003"],
            "ALLELE0": ["A", "C", "G"],
            "ALLELE1": ["T", "G", "A"],
            "A1FREQ": [0.21, 0.45, 0.10],
            "N": [500, 500, 498],
            "BETA": [0.10, -0.05, 0.20],
            "SE": [0.02, 0.03, 0.04],
            "LOG10P": -np.log10(pvals),
        }
    )
    path = tmp_path / "trait.regenie"
    src.to_csv(path, sep="\t", index=False)
    return path, pvals


@pytest.fixture
def bolt_sumstats(tmp_path):
    """BOLT-LMM output — the preset declares ``n=None`` (BOLT does not
    emit a per-variant N column by default), so the canonical ``n``
    column is expected to be absent in the result."""
    src = pd.DataFrame(
        {
            "SNP": ["rs001", "rs002"],
            "CHR": [1, 2],
            "BP": [1000, 3000],
            "ALLELE0": ["A", "G"],
            "ALLELE1": ["T", "A"],
            "A1FREQ": [0.21, 0.10],
            "BETA": [0.10, 0.20],
            "SE": [0.02, 0.04],
            "P_BOLT_LMM": [1e-6, 1e-12],
        }
    )
    path = tmp_path / "trait.bolt"
    src.to_csv(path, sep="\t", index=False)
    return path


@pytest.fixture
def custom_sumstats(tmp_path):
    """
    Sumstats from an unsupported tool (lowercase column names that
    don't match any preset). Used to exercise the ``schema=`` override
    when no preset fits.
    """
    src = pd.DataFrame(
        {
            "snp": ["rs1", "rs2"],
            "chr": [1, 2],
            "bp": [100, 200],
            "ea": ["T", "A"],
            "oa": ["A", "G"],
            "effect": [0.1, 0.2],
            "stderr": [0.02, 0.03],
            "pvalue": [1e-5, 1e-9],
        }
    )
    path = tmp_path / "trait.tsv"
    src.to_csv(path, sep="\t", index=False)
    return path


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------
class TestReadSumstatsPresets:
    def test_plink2_round_trip(self, plink2_sumstats):
        # Usage: read_sumstats("trait.glm.linear", preset="plink2")
        path, expected = plink2_sumstats

        out = read_sumstats(path, preset="plink2")

        pd.testing.assert_frame_equal(
            out.reset_index(drop=True),
            expected.reset_index(drop=True),
            check_dtype=False,
        )

    def test_plink2_drops_non_canonical_columns(self, plink2_sumstats):
        # The source has an "EXTRA" column outside the canonical schema;
        # the loader must drop it (canonicalization, not pass-through).
        path, _ = plink2_sumstats

        out = read_sumstats(path, preset="plink2")

        assert "EXTRA" not in out.columns
        assert set(out.columns).issubset(CANONICAL_COLS)

    def test_regenie_log10p_is_converted_to_plain_pvalue(self, regenie_sumstats):
        # Usage: read_sumstats("trait.regenie", preset="regenie")
        # The preset knows REGENIE emits -log10(p), so it inverts it.
        path, original_pvals = regenie_sumstats

        out = read_sumstats(path, preset="regenie")

        np.testing.assert_allclose(
            out["pval"].to_numpy(), original_pvals, rtol=1e-12
        )

    def test_bolt_skips_columns_the_tool_does_not_emit(self, bolt_sumstats):
        # Usage: read_sumstats("trait.bolt", preset="bolt")
        path = bolt_sumstats

        out = read_sumstats(path, preset="bolt")

        assert "n" not in out.columns
        # Core columns are still there.
        assert {
            "variant_id", "chrom", "pos",
            "effect_allele", "other_allele",
            "beta", "se", "pval", "eaf",
        }.issubset(out.columns)


# ---------------------------------------------------------------------------
# Schema override
# ---------------------------------------------------------------------------
class TestReadSumstatsSchemaOverride:
    def test_custom_schema_without_preset(self, custom_sumstats):
        # Usage: read_sumstats(path, schema={...})
        # When no preset fits the tool, pass the full canonical->source map.
        path = custom_sumstats

        out = read_sumstats(
            path,
            schema={
                "variant_id": "snp",
                "chrom": "chr",
                "pos": "bp",
                "effect_allele": "ea",
                "other_allele": "oa",
                "beta": "effect",
                "se": "stderr",
                "pval": "pvalue",
            },
        )

        assert list(out.columns) == [
            "variant_id", "chrom", "pos",
            "effect_allele", "other_allele",
            "beta", "se", "pval",
        ]
        assert len(out) == 2
        assert out["pval"].iloc[0] == pytest.approx(1e-5)

    def test_schema_overrides_preset_entry_by_entry(self, plink2_sumstats):
        # Usage: read_sumstats(path, preset="plink2", schema={"variant_id": "RSID"})
        # Useful when one column deviates from the tool default but the
        # rest of the file is standard.
        path, _ = plink2_sumstats
        df = pd.read_csv(path, sep="\t").rename(columns={"ID": "RSID"})
        df.to_csv(path, sep="\t", index=False)

        out = read_sumstats(
            path, preset="plink2", schema={"variant_id": "RSID"}
        )

        assert list(out["variant_id"][:2]) == ["rs001", "rs002"]

    def test_schema_pval_override_disables_log10p_conversion(
        self, regenie_sumstats
    ):
        # When the user explicitly maps `pval` to a different source
        # column, the LOG10P→p auto-conversion of the regenie preset
        # is skipped (the user is in charge of pval semantics).
        path, _ = regenie_sumstats
        df = pd.read_csv(path, sep="\t")
        df["P_PLAIN"] = [1e-6, 0.10, 1e-12]
        df.to_csv(path, sep="\t", index=False)

        out = read_sumstats(
            path, preset="regenie", schema={"pval": "P_PLAIN"}
        )

        np.testing.assert_allclose(
            out["pval"].to_numpy(),
            [1e-6, 0.10, 1e-12],
            rtol=1e-12,
        )


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class TestReadSumstatsErrors:
    def test_unknown_preset_raises_value_error(self, plink2_sumstats):
        path, _ = plink2_sumstats

        with pytest.raises(ValueError, match="Unknown preset"):
            read_sumstats(path, preset="not-a-tool")

    def test_no_preset_and_no_schema_raises_value_error(self, plink2_sumstats):
        # The function refuses to guess — at least one of preset/schema
        # must be provided.
        path, _ = plink2_sumstats

        with pytest.raises(ValueError, match="preset"):
            read_sumstats(path)

    def test_missing_source_column_raises_keyerror(self, plink2_sumstats):
        # If the preset expects a column that isn't in the file, fail
        # loudly with the missing column name in the message.
        path, _ = plink2_sumstats
        df = pd.read_csv(path, sep="\t").drop(columns=["BETA"])
        df.to_csv(path, sep="\t", index=False)

        with pytest.raises(KeyError, match="BETA"):
            read_sumstats(path, preset="plink2")
