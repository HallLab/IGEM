from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional, Union

from igem.core.components.base_component import BaseComponent
from igem.modules import data as _data

if TYPE_CHECKING:
    import pandas as pd

    from igem.modules.data import Genotypes, Phenotypes

PathLike = Union[str, Path]


class DataComponent(BaseComponent):
    """
    Local data loading: PLINK, VCF, Zarr, NHANES XPT, CSV/TSV.

    Thin wrappers around :mod:`igem.modules.data` that emit a header /
    footer log so the user has a record of what was loaded into memory.
    Free functions in :mod:`igem.modules.data` remain importable for
    callers who do not need an :class:`IGEM` instance.
    """

    # ------------------------------------------------------------------
    # Genotypes
    # ------------------------------------------------------------------
    def read_plink(self, path: PathLike, **kwargs) -> "Genotypes":
        self.core.logger.log(f"[data] read_plink({path})", "INFO")
        geno = _data.read_plink(path, **kwargs)
        self.core.logger.footer(
            f"[data] loaded {geno.n_samples} samples × "
            f"{geno.n_variants} variants from PLINK"
        )
        return geno

    def read_vcf(
        self,
        path: PathLike,
        *,
        vcz_path: Optional[PathLike] = None,
        force: bool = False,
        show_progress: bool = False,
        worker_processes: int = 0,
        **convert_kwargs,
    ) -> "Genotypes":
        self.core.logger.log(f"[data] read_vcf({path})", "INFO")
        geno = _data.read_vcf(
            path,
            vcz_path=vcz_path,
            force=force,
            show_progress=show_progress,
            worker_processes=worker_processes,
            **convert_kwargs,
        )
        self.core.logger.footer(
            f"[data] loaded {geno.n_samples} samples × "
            f"{geno.n_variants} variants from VCF"
        )
        return geno

    def read_zarr(self, path: PathLike, **kwargs) -> "Genotypes":
        self.core.logger.log(f"[data] read_zarr({path})", "INFO")
        geno = _data.read_zarr(path, **kwargs)
        self.core.logger.footer(
            f"[data] loaded {geno.n_samples} samples × "
            f"{geno.n_variants} variants from Zarr"
        )
        return geno

    # ------------------------------------------------------------------
    # Phenotypes
    # ------------------------------------------------------------------
    def read_phenotypes(
        self,
        source: Union[PathLike, "pd.DataFrame"],
        *,
        sample_id_col: str = "sample_id",
        outcomes: Optional[Iterable[str]] = None,
        covariates: Optional[Iterable[str]] = None,
        exposures: Optional[Iterable[str]] = None,
        weights_col: Optional[str] = None,
        strata_col: Optional[str] = None,
        cluster_col: Optional[str] = None,
        **read_kwargs,
    ) -> "Phenotypes":
        self.core.logger.log(f"[data] read_phenotypes(source={source!r})", "INFO")
        phen = _data.read_phenotypes(
            source,
            sample_id_col=sample_id_col,
            outcomes=outcomes,
            covariates=covariates,
            exposures=exposures,
            weights_col=weights_col,
            strata_col=strata_col,
            cluster_col=cluster_col,
            **read_kwargs,
        )
        self.core.logger.footer(
            f"[data] loaded {phen.n_samples} samples "
            f"(outcomes={len(phen.outcomes)}, "
            f"covariates={len(phen.covariates)}, "
            f"exposures={len(phen.exposures)})"
        )
        return phen

    def read_nhanes_xpt(
        self,
        path: PathLike,
        *,
        sample_id_col: str = "SEQN",
        **phen_kwargs,
    ) -> "Phenotypes":
        self.core.logger.log(f"[data] read_nhanes_xpt({path})", "INFO")
        phen = _data.read_nhanes_xpt(
            path,
            sample_id_col=sample_id_col,
            **phen_kwargs,
        )
        self.core.logger.footer(
            f"[data] loaded {phen.n_samples} NHANES samples"
        )
        return phen
