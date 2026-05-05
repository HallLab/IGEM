from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Union

import numpy as np
import pandas as pd
import xarray as xr


PathLike = Union[str, Path]


class Genotypes:
    """
    Thin wrapper around an sgkit-format ``xarray.Dataset``.

    Convenience properties cover common inspection needs (sample/variant
    counts, sample IDs, a pandas-friendly variants table). The underlying
    Dataset stays accessible as ``.ds`` so power users can drop into sgkit
    or xarray directly without going through this wrapper.

    The Dataset is expected to follow sgkit conventions:
    https://sgkit-dev.github.io/sgkit/latest/getting_started.html
    """

    def __init__(self, ds: xr.Dataset) -> None:
        self.ds = ds

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------
    @property
    def n_samples(self) -> int:
        return int(self.ds.sizes.get("samples", 0))

    @property
    def n_variants(self) -> int:
        return int(self.ds.sizes.get("variants", 0))

    @property
    def samples(self) -> pd.Index:
        if "sample_id" not in self.ds.variables:
            return pd.RangeIndex(self.n_samples, name="sample")
        return pd.Index(
            np.asarray(self.ds["sample_id"].values), name="sample_id"
        )

    @property
    def variants(self) -> pd.DataFrame:
        """
        Variants as a pandas DataFrame with one row per variant.

        Columns when available: variant_id, contig (resolved name),
        position, ref, alt. Multi-allelic sites collapse alt alleles
        into a comma-separated string.
        """
        ds = self.ds
        cols: list[str] = []
        if "variant_id" in ds.variables:
            cols.append("variant_id")
        if "variant_contig" in ds.variables:
            cols.append("variant_contig")
        if "variant_position" in ds.variables:
            cols.append("variant_position")

        if not cols:
            return pd.DataFrame(index=pd.RangeIndex(self.n_variants))

        df = ds[cols].to_dataframe().reset_index(drop=True)

        contigs = ds.attrs.get("contigs")
        if contigs is not None and "variant_contig" in df.columns:
            df.insert(
                df.columns.get_loc("variant_contig"),
                "contig",
                np.asarray(contigs)[df["variant_contig"].values],
            )

        if "variant_allele" in ds.variables:
            alleles = np.asarray(ds["variant_allele"].values)
            if alleles.ndim == 2:
                df["ref"] = _decode_alleles(alleles[:, 0])
                if alleles.shape[1] > 1:
                    df["alt"] = [
                        ",".join(_decode_alleles(row)) for row in alleles[:, 1:]
                    ]

        df = df.rename(
            columns={"variant_position": "position"},
        )
        return df

    # ------------------------------------------------------------------
    # Subsetting
    # ------------------------------------------------------------------
    def select(
        self,
        *,
        samples: Optional[Iterable] = None,
        variants: Optional[Iterable] = None,
        sample_mask: Optional[np.ndarray] = None,
        variant_mask: Optional[np.ndarray] = None,
    ) -> "Genotypes":
        """
        Return a new Genotypes restricted to the requested samples/variants.

        Subsetting is positional under the hood (xarray ``isel``) so the
        operation stays lazy when the underlying Dataset is Dask-backed.
        ``samples`` matches against ``sample_id``; ``variants`` against
        ``variant_id``. Boolean masks are applied as-is.
        """
        ds = self.ds

        if samples is not None:
            wanted = pd.Index([str(s) for s in samples])
            current = pd.Index(np.asarray(ds["sample_id"].values).astype(str))
            mask = current.isin(wanted)
            ds = ds.isel(samples=np.where(mask)[0])
        if sample_mask is not None:
            ds = ds.isel(samples=np.where(np.asarray(sample_mask))[0])

        if variants is not None:
            wanted = pd.Index([str(v) for v in variants])
            current = pd.Index(np.asarray(ds["variant_id"].values).astype(str))
            mask = current.isin(wanted)
            ds = ds.isel(variants=np.where(mask)[0])
        if variant_mask is not None:
            ds = ds.isel(variants=np.where(np.asarray(variant_mask))[0])

        return Genotypes(ds)

    # ------------------------------------------------------------------
    # Materialization
    # ------------------------------------------------------------------
    def to_numpy(self) -> np.ndarray:
        """
        Materialize ``call_genotype`` as a numpy array of shape
        ``(n_variants, n_samples, ploidy)``. Loads into RAM; use
        ``.select(...)`` first to scope down on biobank-scale data.
        """
        return np.asarray(self.ds["call_genotype"].values)

    def __repr__(self) -> str:
        return (
            f"<Genotypes n_samples={self.n_samples} "
            f"n_variants={self.n_variants}>"
        )


# ----------------------------------------------------------------------
# Readers
# ----------------------------------------------------------------------
def read_plink(path: PathLike, **kwargs) -> Genotypes:
    """
    Read a PLINK ``.bed/.bim/.fam`` triplet.

    Parameters
    ----------
    path : str | Path
        File prefix (e.g. ``"data"`` resolves to ``data.bed``,
        ``data.bim``, ``data.fam``).
    **kwargs : forwarded to ``sgkit.read_plink``.

    Notes
    -----
    Backed by ``sgkit.read_plink`` (which uses ``bed-reader`` under
    the hood). The returned Dataset is Dask-backed; computations are
    lazy until you call ``.to_numpy()`` or compute over ``.ds``.

    ``sgkit.read_plink`` is deprecated in sgkit 0.11; we will migrate
    to ``bio2zarr.plink2zarr`` + ``sgkit.load_dataset`` when sgkit 0.11
    is available with a stable zarr-3 path.
    """
    import sgkit

    ds = sgkit.read_plink(path=str(path), **kwargs)
    return Genotypes(ds)


def read_zarr(path: PathLike, **kwargs) -> Genotypes:
    """
    Load a VCZ/Zarr store into a Genotypes wrapper.

    Accepts any zarr produced by ``bio2zarr.vcf.convert`` or by sgkit
    itself. ``**kwargs`` are forwarded to ``sgkit.load_dataset`` (and
    onward to ``xarray.open_zarr``).
    """
    import sgkit

    ds = sgkit.load_dataset(str(path), **kwargs)
    return Genotypes(ds)


def read_vcf(
    path: PathLike,
    *,
    vcz_path: Optional[PathLike] = None,
    force: bool = False,
    show_progress: bool = False,
    worker_processes: int = 0,
    **convert_kwargs,
) -> Genotypes:
    """
    Read a VCF (or BCF / .vcf.gz) by converting it to VCZ via
    ``bio2zarr.vcf.convert`` and loading the resulting Zarr.

    The conversion is the expensive step; subsequent calls with the
    same VCF reuse the cached VCZ store unless ``force=True``.

    Parameters
    ----------
    path : str | Path
        Path to the VCF / BCF file.
    vcz_path : str | Path, optional
        Where to write the VCZ store. Defaults to a sibling of the
        VCF named ``<basename>.vcz``.
    force : bool
        Re-run the conversion even if ``vcz_path`` already exists.
    show_progress : bool
        Forwarded to ``bio2zarr.vcf.convert``.
    worker_processes : int
        Parallel workers for the conversion stage. 0 means in-process.
    **convert_kwargs : forwarded to ``bio2zarr.vcf.convert``.
    """
    from bio2zarr import vcf as b2z_vcf
    import sgkit

    vcf_path = Path(path)
    target = Path(vcz_path) if vcz_path is not None else _default_vcz_path(vcf_path)

    if force or not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        b2z_vcf.convert(
            [str(vcf_path)],
            vcz_path=str(target),
            show_progress=show_progress,
            worker_processes=worker_processes,
            **convert_kwargs,
        )

    ds = sgkit.load_dataset(str(target))
    return Genotypes(ds)


def _default_vcz_path(vcf_path: Path) -> Path:
    name = vcf_path.name
    for suffix in (".vcf.gz", ".vcf.bgz", ".vcf", ".bcf"):
        if name.endswith(suffix):
            stem = name[: -len(suffix)]
            return vcf_path.with_name(stem + ".vcz")
    return vcf_path.with_suffix(".vcz")


def _decode_alleles(arr: np.ndarray) -> list[str]:
    out: list[str] = []
    for a in arr:
        if isinstance(a, bytes):
            out.append(a.decode("utf-8"))
        else:
            out.append(str(a))
    return out
