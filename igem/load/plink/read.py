import warnings
from collections import OrderedDict as odict
from glob import glob
from typing import Optional

import dask.array as da
import pandas as pd
from xarray import DataArray

from ._bed_read import read_bed
from .allele import Allele
from .chunk import Chunk
from .util import last_replace

__all__ = ["plink1_xa"]


def plink1_xa(
    bed: str,
    bim: Optional[str] = None,
    fam: Optional[str] = None,
    verbose: bool = True,
    ref: str = "a1",
    chunk: Chunk = Chunk(),
) -> DataArray:

    bed_files = sorted(glob(bed))
    if len(bed_files) == 0:
        raise ValueError("No BED file has been found.")

    if bim is None:
        bim_files = [last_replace(f, ".bed", ".bim") for f in bed_files]
    else:
        bim_files = sorted(glob(bim))
    if len(bim_files) == 0:
        raise ValueError("No BIM file has been found.")

    if fam is None:
        fam_files = [last_replace(f, ".bed", ".fam") for f in bed_files]
    else:
        fam_files = sorted(glob(fam))
    if len(fam_files) == 0:
        raise ValueError("No FAM file has been found.")

    if len(bed_files) != len(bim_files):
        raise ValueError("The numbers of BED and BIM files must match.")

    if len(fam_files) > 1:
        msg = "More than one FAM file has been specified."
        msg += "Only the first one will beconsidered."
        if verbose:
            warnings.warn(msg, UserWarning)
        fam_files = fam_files[:1]

    bims = _read_file(bim_files, lambda f: _read_bim_noi(f))
    nmarkers = {bed_files[i]: b.shape[0] for i, b in enumerate(bims)}
    bim_df = pd.concat(bims, axis=0, ignore_index=True)
    fam_df = _read_file(fam_files, lambda f: _read_fam_noi(f))[0]

    nsamples = fam_df.shape[0]
    sample_ids = fam_df["iid"]
    nvariants = bim_df.shape[0]
    variant_ids = [f"variant{i}" for i in range(nvariants)]

    if ref == "a1":
        ref_al = Allele.a1
    elif ref == "a0":
        ref_al = Allele.a0
    else:
        raise ValueError("Unknown reference allele.")

    G = _read_file(
        bed_files, lambda f: _read_bed(f, nsamples, nmarkers[f], ref_al, chunk)
    )
    G = da.concatenate(G, axis=1)

    G = DataArray(G, dims=["sample", "variant"], coords=[
        sample_ids,
        variant_ids
        ])
    sample = {c: ("sample", fam_df[c]) for c in fam_df.columns}
    variant = {c: ("variant", bim_df[c]) for c in iter(bim_df.columns)}
    G = G.assign_coords(**sample)
    G = G.assign_coords(**variant)
    G.name = "genotype"

    return G


def _read_file(fn, read_func):
    data = []
    for f in fn:
        data.append(read_func(f))
    return data


def _read_csv(fn, header) -> pd.DataFrame:

    df = pd.read_csv(
        fn,
        delim_whitespace=True,
        header=None,
        names=list(header.keys()),
        dtype=header,
        compression=None,
        engine="c",
        iterator=False,
    )
    assert isinstance(df, pd.DataFrame)
    return df


def _read_bim_noi(fn):
    from .type import bim

    header = odict(
        [
            ("chrom", bim["chrom"]),
            ("snp", bim["snp"]),
            ("cm", bim["cm"]),
            ("pos", bim["pos"]),
            ("a0", bim["a0"]),
            ("a1", bim["a1"]),
        ]
    )
    return _read_csv(fn, header)


def _read_fam_noi(fn):
    from .type import fam

    header = odict(
        [
            ("fid", fam["fid"]),
            ("iid", fam["iid"]),
            ("father", fam["father"]),
            ("mother", fam["mother"]),
            ("gender", fam["gender"]),
            ("trait", fam["trait"]),
        ]
    )

    return _read_csv(fn, header)


def _read_bed(fn, nsamples, nvariants, ref: Allele, chunk: Chunk):
    _check_bed_header(fn)
    major = _major_order(fn)

    # Assume major == "variant".
    nrows = nvariants
    ncols = nsamples
    row_chunk = nrows if chunk.nvariants is None else min(
        nrows,
        chunk.nvariants
        )
    col_chunk = ncols if chunk.nsamples is None else min(
        ncols,
        chunk.nsamples
        )

    if major == "sample":
        nrows, ncols = ncols, nrows
        row_chunk, col_chunk = col_chunk, row_chunk

    max_npartitions = 16_384
    row_chunk = max(nrows // max_npartitions, row_chunk)
    col_chunk = max(ncols // max_npartitions, col_chunk)

    G = read_bed(fn, nrows, ncols, row_chunk, col_chunk, ref)
    if major == "variant":
        G = G.T

    return G


def _check_bed_header(fn):
    with open(fn, "rb") as f:
        arr = f.read(2)
        if len(arr) < 2:
            raise ValueError("Couldn't read BED header: %s." % fn)
        ok = arr[0] == 108 and arr[1] == 27
        if not ok:
            raise ValueError("Invalid BED file: %s." % fn)


def _major_order(fn):
    """
    Major order.

    Variant-major lists all samples for first variant, all samples for second
    variant, and so on. Sample-major lists all variants for first sample, all
    variants for second sample, and so on.
    """
    with open(fn, "rb") as f:
        f.seek(2)
        arr = f.read(1)
        if len(arr) < 1:
            raise ValueError("Couldn't read column order: %s." % fn)
        if arr[0] == 1:
            return "variant"
        elif arr[0] == 0:
            return "sample"
        msg = "Invalid matrix layout. Maybe it is a PLINK2 file?"
        msg += " PLINK2 is not supported yet."
        raise ValueError(msg)
