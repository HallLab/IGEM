"""Tests for igem.modules.data.genotypes (Genotypes class + read_zarr)."""
from __future__ import annotations

import numpy as np
import pandas as pd

from igem.modules.data import Genotypes, read_zarr


# ---------------------------------------------------------------------------
# Genotypes class
# ---------------------------------------------------------------------------
class TestGenotypesAPI:
    def test_init_and_counts(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        assert geno.n_samples == 8
        assert geno.n_variants == 20

    def test_ds_passthrough(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        assert geno.ds is synthetic_geno_ds

    def test_samples_returns_named_index(self, synthetic_geno_ds):
        samples = Genotypes(synthetic_geno_ds).samples
        assert isinstance(samples, pd.Index)
        assert samples.name == "sample_id"
        assert list(samples)[:3] == ["S000", "S001", "S002"]

    def test_variants_dataframe_columns(self, synthetic_geno_ds):
        variants = Genotypes(synthetic_geno_ds).variants
        assert isinstance(variants, pd.DataFrame)
        assert "variant_id" in variants.columns
        assert "position" in variants.columns
        assert "ref" in variants.columns
        assert "alt" in variants.columns
        assert len(variants) == 20

    def test_repr_includes_counts(self, synthetic_geno_ds):
        text = repr(Genotypes(synthetic_geno_ds))
        assert "n_samples=8" in text
        assert "n_variants=20" in text


class TestGenotypesSelect:
    def test_select_by_sample_ids(self, synthetic_geno_ds):
        sub = Genotypes(synthetic_geno_ds).select(samples=["S001", "S004"])
        assert sub.n_samples == 2
        assert sub.n_variants == 20
        assert list(sub.samples) == ["S001", "S004"]

    def test_select_by_variant_ids(self, synthetic_geno_ds):
        sub = Genotypes(synthetic_geno_ds).select(
            variants=["rs00002", "rs00010"]
        )
        assert sub.n_variants == 2
        assert sub.n_samples == 8

    def test_select_combined_samples_and_variants(self, synthetic_geno_ds):
        sub = Genotypes(synthetic_geno_ds).select(
            samples=["S001"], variants=["rs00005"]
        )
        assert sub.n_samples == 1
        assert sub.n_variants == 1

    def test_select_with_boolean_variant_mask(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        mask = np.zeros(geno.n_variants, dtype=bool)
        mask[:5] = True
        sub = geno.select(variant_mask=mask)
        assert sub.n_variants == 5

    def test_select_with_boolean_sample_mask(self, synthetic_geno_ds):
        geno = Genotypes(synthetic_geno_ds)
        mask = np.array([True, False, True, False, True, False, False, False])
        sub = geno.select(sample_mask=mask)
        assert sub.n_samples == 3

    def test_select_unknown_returns_empty(self, synthetic_geno_ds):
        sub = Genotypes(synthetic_geno_ds).select(samples=["NOT_REAL"])
        assert sub.n_samples == 0


class TestGenotypesToNumpy:
    def test_to_numpy_shape_after_subset(self, synthetic_geno_ds):
        sub = Genotypes(synthetic_geno_ds).select(
            samples=["S000", "S001"], variants=["rs00000", "rs00005"]
        )
        arr = sub.to_numpy()
        # sgkit convention: (variants, samples, ploidy)
        assert arr.shape == (2, 2, 2)
        assert arr.dtype.kind in {"i", "u", "f"}


# ---------------------------------------------------------------------------
# read_zarr (integration with sgkit's load_dataset)
# ---------------------------------------------------------------------------
class TestReadZarr:
    def test_returns_genotypes(self, synthetic_zarr_path):
        geno = read_zarr(synthetic_zarr_path)
        assert isinstance(geno, Genotypes)
        assert geno.n_samples == 8
        assert geno.n_variants == 20

    def test_select_then_to_numpy_after_load(self, synthetic_zarr_path):
        geno = read_zarr(synthetic_zarr_path)
        sub = geno.select(variants=["rs00003"])
        arr = sub.to_numpy()
        assert arr.shape == (1, 8, 2)

    def test_accepts_string_path(self, synthetic_zarr_path):
        geno = read_zarr(str(synthetic_zarr_path))
        assert geno.n_samples == 8
