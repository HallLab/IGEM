"""Shared pytest fixtures for IGEM client tests."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import sgkit
import xarray as xr

from igem.modules.data import Genotypes


# ----------------------------------------------------------------------
# Synthetic sgkit dataset (uniform random genotypes)
# ----------------------------------------------------------------------
@pytest.fixture
def synthetic_geno_ds() -> xr.Dataset:
    """
    Small sgkit-format Dataset (8 samples × 20 variants) with stable
    sample_id and variant_id values for assertions.
    """
    ds = sgkit.simulate_genotype_call_dataset(
        n_variant=20, n_sample=8, seed=42
    )
    ds["variant_id"] = (
        "variants",
        np.array([f"rs{i:05d}" for i in range(ds.sizes["variants"])]),
    )
    ds["sample_id"] = (
        "samples",
        np.array([f"S{i:03d}" for i in range(ds.sizes["samples"])]),
    )
    return ds


@pytest.fixture
def synthetic_zarr_path(tmp_path: Path, synthetic_geno_ds: xr.Dataset) -> Path:
    """Path to a freshly-saved zarr store of the synthetic Dataset."""
    zarr_path = tmp_path / "synth.vcz"
    sgkit.save_dataset(synthetic_geno_ds, str(zarr_path))
    return zarr_path


# ----------------------------------------------------------------------
# Hand-built sgkit datasets with known properties
# (shared by modify and describe tests)
# ----------------------------------------------------------------------
def _build_biallelic_dataset(
    call_genotype: np.ndarray,
    variant_ids: list[str],
    sample_ids: list[str],
) -> Genotypes:
    """Wrap a (variants, samples, ploidy) call array into a Genotypes."""
    alleles = np.array(
        [[b"A", b"C"] for _ in variant_ids], dtype="|S1"
    )
    ds = xr.Dataset(
        {
            "call_genotype": (
                ("variants", "samples", "ploidy"),
                call_genotype.astype(np.int8),
            ),
            "variant_allele": (("variants", "alleles"), alleles),
            "variant_id": (
                "variants",
                np.array(variant_ids, dtype=object),
            ),
            "sample_id": (
                "samples",
                np.array(sample_ids, dtype=object),
            ),
            "variant_contig": (
                "variants",
                np.zeros(len(variant_ids), dtype=np.int32),
            ),
            "variant_position": (
                "variants",
                np.arange(len(variant_ids), dtype=np.int32),
            ),
        },
        attrs={"contigs": ["1"]},
    )
    return Genotypes(ds)


@pytest.fixture
def multiallelic_geno() -> Genotypes:
    """
    Dataset with ``alleles`` dim of size 3 and a single triallelic
    variant in the middle. Call genotypes are all-ref so structural
    filters can be tested in isolation.
    """
    variant_allele = np.array(
        [
            [b"A", b"C", b""],   # biallelic
            [b"A", b"C", b"G"],  # triallelic
            [b"T", b"G", b""],   # biallelic
        ]
    )
    call_genotype = np.zeros((3, 4, 2), dtype=np.int8)
    ds = xr.Dataset(
        {
            "variant_allele": (("variants", "alleles"), variant_allele),
            "call_genotype": (
                ("variants", "samples", "ploidy"),
                call_genotype,
            ),
            "variant_id": (
                "variants",
                np.array(["v1", "v2", "v3"], dtype=object),
            ),
            "sample_id": (
                "samples",
                np.array(["s1", "s2", "s3", "s4"], dtype=object),
            ),
        }
    )
    return Genotypes(ds)


@pytest.fixture
def maf_geno() -> Genotypes:
    """
    3 variants × 10 samples × ploidy 2 with controlled allele
    frequencies:

      - ``v_rare_a``: 1 alt / 20 → MAF = 0.05
      - ``v_common`` : 10 alt / 20 → MAF = 0.50
      - ``v_rare_b``: 19 alt / 20 → MAF = 0.05
    """
    gt_rare_a = np.zeros((10, 2), dtype=np.int8)
    gt_rare_a[0, 0] = 1

    gt_common = np.zeros((10, 2), dtype=np.int8)
    gt_common[:5, :] = 1

    gt_rare_b = np.ones((10, 2), dtype=np.int8)
    gt_rare_b[0, 0] = 0

    call_gt = np.stack([gt_rare_a, gt_common, gt_rare_b])
    return _build_biallelic_dataset(
        call_gt,
        variant_ids=["v_rare_a", "v_common", "v_rare_b"],
        sample_ids=[f"s{i:02d}" for i in range(10)],
    )


@pytest.fixture
def missingness_geno() -> Genotypes:
    """
    3 variants × 5 samples with -1 missing pattern:

      - ``v_clean``  : all genotypes present
      - ``v_part``   : missing for s3 and s4
      - ``v_missing``: missing for s1, s2, s3, s4 (only s0 has data)
    """
    call_gt = np.zeros((3, 5, 2), dtype=np.int8)
    call_gt[1, 3, :] = -1
    call_gt[1, 4, :] = -1
    call_gt[2, 1:, :] = -1
    return _build_biallelic_dataset(
        call_gt,
        variant_ids=["v_clean", "v_part", "v_missing"],
        sample_ids=[f"s{i}" for i in range(5)],
    )


@pytest.fixture
def hwe_geno() -> Genotypes:
    """
    3 variants × 100 samples. ``v_ok_a`` and ``v_ok_b`` are drawn from
    HW-consistent distributions; ``v_bad`` is a 50/50 split between
    hom_ref and hom_alt with zero heterozygotes — a strong HWE
    violation that filters can detect at common thresholds.
    """
    rng = np.random.default_rng(0)
    n_samples = 100

    def hwe_calls(p_alt: float) -> np.ndarray:
        probs = [(1 - p_alt) ** 2, 2 * (1 - p_alt) * p_alt, p_alt ** 2]
        dosage = rng.choice([0, 1, 2], size=n_samples, p=probs)
        return np.stack(
            [
                dosage // 2 + (dosage == 1).astype(int) * 0,
                dosage - (dosage // 2),
            ],
            axis=1,
        ).astype(np.int8)

    v_ok_a = hwe_calls(0.3)
    v_ok_b = hwe_calls(0.5)
    v_bad = np.zeros((n_samples, 2), dtype=np.int8)
    v_bad[n_samples // 2:, :] = 1

    call_gt = np.stack([v_ok_a, v_ok_b, v_bad])
    return _build_biallelic_dataset(
        call_gt,
        variant_ids=["v_ok_a", "v_ok_b", "v_bad"],
        sample_ids=[f"s{i:03d}" for i in range(n_samples)],
    )


# ----------------------------------------------------------------------
# Phenotype fixtures
# ----------------------------------------------------------------------
@pytest.fixture
def nhanes_phen_df() -> pd.DataFrame:
    """
    NHANES-shaped phenotype frame with explicit role candidates:
    SEQN as sample id, GLUCOSE outcome, AGE/SEX covariates, BMI exposure,
    and survey design columns (WTMEC, SDMVSTRA, SDMVPSU).
    """
    return pd.DataFrame(
        {
            "SEQN": ["S000", "S001", "S002", "S003"],
            "BMI": [22.1, 31.0, 27.5, 24.0],
            "AGE": [45, 52, 38, 60],
            "SEX": ["F", "M", "M", "F"],
            "GLUCOSE": [88.0, 142.0, 95.0, 110.0],
            "WTMEC": [1.1, 0.9, 1.0, 1.05],
            "SDMVSTRA": [1, 1, 2, 2],
            "SDMVPSU": [10, 11, 20, 21],
        }
    )
