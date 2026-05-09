"""Fixtures for igem.modules.analyze tests."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from igem.modules.data import Genotypes, Phenotypes


@pytest.fixture
def linear_phen() -> Phenotypes:
    """
    200 samples with a continuous outcome (GLUCOSE) generated as:

        GLUCOSE = 80 + 0.5*AGE + 2.0*BMI + 5.0*SEX + N(0, 5)

    AGE and SEX are covariates. BMI has a strong effect (will pass
    EWAS); EXP_NULL_A / EXP_NULL_B are independent noise (should not
    pass at reasonable thresholds).
    """
    rng = np.random.default_rng(42)
    n = 200
    age = rng.normal(50, 10, n)
    sex = rng.integers(0, 2, n)
    bmi = rng.normal(25, 4, n)
    null_a = rng.normal(0, 1, n)
    null_b = rng.normal(0, 1, n)
    glucose = (
        80 + 0.5 * age + 2.0 * bmi + 5.0 * sex + rng.normal(0, 5, n)
    )
    df = pd.DataFrame(
        {
            "sample_id": [f"S{i:03d}" for i in range(n)],
            "AGE": age, "SEX": sex,
            "BMI": bmi,
            "EXP_NULL_A": null_a, "EXP_NULL_B": null_b,
            "GLUCOSE": glucose,
        }
    )
    return Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=["GLUCOSE"],
        covariates=["AGE", "SEX"],
        exposures=["BMI", "EXP_NULL_A", "EXP_NULL_B"],
    )


@pytest.fixture
def logistic_phen() -> Phenotypes:
    """
    200 samples with a binary outcome whose log-odds depend on BMI and
    AGE. ``EXP_NULL_*`` are independent of the outcome.
    """
    rng = np.random.default_rng(7)
    n = 200
    age = rng.normal(50, 10, n)
    bmi = rng.normal(25, 4, n)
    null_a = rng.normal(0, 1, n)
    log_odds = -7.0 + 0.04 * age + 0.15 * bmi
    p = 1.0 / (1.0 + np.exp(-log_odds))
    diabetes = (rng.uniform(0, 1, n) < p).astype(int)
    df = pd.DataFrame(
        {
            "sample_id": [f"S{i:03d}" for i in range(n)],
            "AGE": age,
            "BMI": bmi,
            "EXP_NULL_A": null_a,
            "DIABETES": diabetes,
        }
    )
    return Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=["DIABETES"],
        covariates=["AGE"],
        exposures=["BMI", "EXP_NULL_A"],
    )


@pytest.fixture
def survey_phen() -> Phenotypes:
    """
    150 samples with NHANES-style survey columns. The outcome depends
    on BMI; sample weights are intentionally non-uniform so a survey
    fit gives a measurably different beta vs the unweighted fit.
    """
    rng = np.random.default_rng(11)
    n = 150
    age = rng.normal(50, 10, n)
    sex = rng.integers(0, 2, n)
    bmi = rng.normal(25, 4, n)
    glucose = (
        80 + 0.5 * age + 2.0 * bmi + 5.0 * sex + rng.normal(0, 5, n)
    )
    # 3 PSUs in 2 strata; weights inflate every other PSU.
    psu = np.tile([0, 1, 2], n // 3 + 1)[:n]
    strata = (psu // 2).astype(int)
    weights = np.where(psu == 0, 2.0, 1.0)

    df = pd.DataFrame(
        {
            "sample_id": [f"S{i:03d}" for i in range(n)],
            "AGE": age, "SEX": sex,
            "BMI": bmi,
            "GLUCOSE": glucose,
            "WTMEC": weights,
            "SDMVPSU": psu,
            "SDMVSTRA": strata,
        }
    )
    return Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=["GLUCOSE"],
        covariates=["AGE", "SEX"],
        exposures=["BMI"],
        weights_col="WTMEC",
        cluster_col="SDMVPSU",
        strata_col="SDMVSTRA",
    )


@pytest.fixture
def gwas_geno_and_phen() -> tuple[Genotypes, Phenotypes]:
    """
    Tiny biallelic dataset (200 samples × 6 variants) where one
    variant has a real linear effect on the trait and the others are
    independent of it. The phenotype's sample IDs intersect with the
    geno's sample IDs (the geno has 5 extra samples that should be
    aligned out).
    """
    rng = np.random.default_rng(2026)
    n_samples_geno = 205
    n_samples_phen = 200
    n_variants = 6

    # Genotype dosages 0/1 per ploid (biallelic) — random for everyone.
    cg = rng.integers(0, 2, size=(n_variants, n_samples_geno, 2)).astype(np.int8)

    sample_ids = np.array(
        [f"S{i:03d}" for i in range(n_samples_geno)], dtype=object
    )
    variant_ids = np.array(
        [f"rs{i:05d}" for i in range(n_variants)], dtype=object
    )
    contigs = ["1"]

    ds = xr.Dataset(
        {
            "call_genotype": (
                ("variants", "samples", "ploidy"),
                cg,
            ),
            "variant_allele": (
                ("variants", "alleles"),
                np.array([[b"A", b"C"]] * n_variants, dtype="|S1"),
            ),
            "variant_id": ("variants", variant_ids),
            "sample_id": ("samples", sample_ids),
            "variant_contig": (
                "variants",
                np.zeros(n_variants, dtype=np.int32),
            ),
            "variant_position": (
                "variants",
                np.arange(n_variants, dtype=np.int32),
            ),
        },
        attrs={"contigs": contigs},
    )
    geno = Genotypes(ds)

    # Build phen using only the first 200 sample IDs of the geno; that
    # leaves 5 geno-only samples to verify alignment drops them.
    phen_sample_ids = sample_ids[:n_samples_phen].astype(str)
    age = rng.normal(50, 10, n_samples_phen)

    # Dosage of variant index 2 = sum of ploids for those samples.
    causal_dosage = cg[2, :n_samples_phen, :].sum(axis=1)
    glucose = (
        80 + 0.5 * age + 4.0 * causal_dosage + rng.normal(0, 3, n_samples_phen)
    )

    df = pd.DataFrame(
        {
            "sample_id": phen_sample_ids,
            "AGE": age,
            "GLUCOSE": glucose,
        }
    )
    phen = Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=["GLUCOSE"],
        covariates=["AGE"],
    )
    return geno, phen


class FakeReport:
    """Stand-in for ReportComponent used by annotate tests."""

    def __init__(self, table: pd.DataFrame) -> None:
        self._table = table
        self.last_call: dict | None = None

    def gene_annotations(
        self,
        input_values=None,
        assembly: str = "GRCh38.p14",
        **_kwargs,
    ):
        self.last_call = {
            "input_values": list(input_values) if input_values else [],
            "assembly": assembly,
        }

        class _Result:
            df = self._table

        return _Result()


@pytest.fixture
def fake_report() -> "FakeReport":
    """A two-row gene_annotations response covering rs00001 and TP53."""
    table = pd.DataFrame(
        [
            {
                "input_value": "rs00001",
                "gene_symbol": "APOE",
                "hgnc_id": "HGNC:613",
                "ensembl_id": "ENSG00000130203",
                "chromosome": "19",
                "start_position": 44905782,
                "end_position": 44909393,
                "status": "found",
            },
            {
                "input_value": "TP53",
                "gene_symbol": "TP53",
                "hgnc_id": "HGNC:11998",
                "ensembl_id": "ENSG00000141510",
                "chromosome": "17",
                "start_position": 7661779,
                "end_position": 7687546,
                "status": "found",
            },
        ]
    )
    return FakeReport(table)


@pytest.fixture
def errors_phen() -> Phenotypes:
    """
    Phenotype where one of the exposures is unfittable. ``ALL_NAN_EXP``
    is missing for every sample so per-exposure ``dropna`` produces an
    empty subset, hitting the error-handling path of ewas without
    affecting the other regressions.
    """
    rng = np.random.default_rng(0)
    n = 50
    df = pd.DataFrame(
        {
            "sample_id": [f"S{i:03d}" for i in range(n)],
            "AGE": rng.normal(50, 10, n),
            "BMI": rng.normal(25, 4, n),
            "ALL_NAN_EXP": np.full(n, np.nan),
            "GLUCOSE": rng.normal(100, 10, n),
        }
    )
    return Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=["GLUCOSE"],
        covariates=["AGE"],
        exposures=["BMI", "ALL_NAN_EXP"],
    )
