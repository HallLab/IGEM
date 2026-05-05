from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Union

import pandas as pd


PathLike = Union[str, Path]
PhenSource = Union[PathLike, pd.DataFrame]


class Phenotypes:
    """
    Wrapper around a ``pandas.DataFrame`` with role metadata.

    Tracks which columns are *outcomes*, *covariates*, and *exposures*
    so downstream analysis modules can act on the right columns
    without re-passing long lists. Survey-design columns (weights,
    strata, cluster) are also tracked for use by ``igem.survey``.

    The underlying frame stays accessible as ``.df``.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        *,
        sample_id_col: str = "sample_id",
        outcomes: Optional[Iterable[str]] = None,
        covariates: Optional[Iterable[str]] = None,
        exposures: Optional[Iterable[str]] = None,
        weights_col: Optional[str] = None,
        strata_col: Optional[str] = None,
        cluster_col: Optional[str] = None,
    ) -> None:
        if sample_id_col not in df.columns:
            raise ValueError(
                f"sample_id_col {sample_id_col!r} not found in dataframe "
                f"columns: {list(df.columns)}"
            )
        self._df = df.copy()
        self.sample_id_col = sample_id_col
        self.outcomes = list(outcomes or [])
        self.covariates = list(covariates or [])
        self.exposures = list(exposures or [])
        self.weights_col = weights_col
        self.strata_col = strata_col
        self.cluster_col = cluster_col
        self._validate_columns()

    def _validate_columns(self) -> None:
        unknown_lists: dict[str, list[str]] = {}
        for name, cols in (
            ("outcomes", self.outcomes),
            ("covariates", self.covariates),
            ("exposures", self.exposures),
        ):
            missing = [c for c in cols if c not in self._df.columns]
            if missing:
                unknown_lists[name] = missing

        missing_scalar: dict[str, str] = {}
        for name, col in (
            ("weights_col", self.weights_col),
            ("strata_col", self.strata_col),
            ("cluster_col", self.cluster_col),
        ):
            if col is not None and col not in self._df.columns:
                missing_scalar[name] = col

        if unknown_lists or missing_scalar:
            parts: list[str] = []
            parts.extend(f"{k}={v}" for k, v in unknown_lists.items())
            parts.extend(f"{k}={v!r}" for k, v in missing_scalar.items())
            raise ValueError(
                "Phenotypes columns not present in dataframe: "
                + ", ".join(parts)
            )

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------
    @property
    def df(self) -> pd.DataFrame:
        return self._df

    @property
    def n_samples(self) -> int:
        return len(self._df)

    @property
    def samples(self) -> pd.Index:
        return pd.Index(self._df[self.sample_id_col], name=self.sample_id_col)

    def outcomes_df(self) -> pd.DataFrame:
        return self._df[[self.sample_id_col, *self.outcomes]]

    def covariates_df(self) -> pd.DataFrame:
        return self._df[[self.sample_id_col, *self.covariates]]

    def exposures_df(self) -> pd.DataFrame:
        return self._df[[self.sample_id_col, *self.exposures]]

    # ------------------------------------------------------------------
    # Subsetting
    # ------------------------------------------------------------------
    def select_samples(self, sample_ids: Iterable) -> "Phenotypes":
        wanted = {str(s) for s in sample_ids}
        ids = self._df[self.sample_id_col].astype(str)
        sub = self._df.loc[ids.isin(wanted)].copy()
        return Phenotypes(
            sub,
            sample_id_col=self.sample_id_col,
            outcomes=self.outcomes,
            covariates=self.covariates,
            exposures=self.exposures,
            weights_col=self.weights_col,
            strata_col=self.strata_col,
            cluster_col=self.cluster_col,
        )

    def __repr__(self) -> str:
        parts = [f"n_samples={self.n_samples}"]
        for name, cols in (
            ("outcomes", self.outcomes),
            ("covariates", self.covariates),
            ("exposures", self.exposures),
        ):
            if cols:
                parts.append(f"{name}={len(cols)}")
        survey = [
            n for n, c in (
                ("weights", self.weights_col),
                ("strata", self.strata_col),
                ("cluster", self.cluster_col),
            )
            if c is not None
        ]
        if survey:
            parts.append(f"survey={'+'.join(survey)}")
        return "<Phenotypes " + " ".join(parts) + ">"


# ----------------------------------------------------------------------
# Readers
# ----------------------------------------------------------------------
def read_phenotypes(
    source: PhenSource,
    *,
    sample_id_col: str = "sample_id",
    outcomes: Optional[Iterable[str]] = None,
    covariates: Optional[Iterable[str]] = None,
    exposures: Optional[Iterable[str]] = None,
    weights_col: Optional[str] = None,
    strata_col: Optional[str] = None,
    cluster_col: Optional[str] = None,
    **read_kwargs,
) -> Phenotypes:
    """
    Load phenotypes from CSV / TSV / SAS XPT, or wrap an existing DataFrame.

    File format inferred from the suffix:
      - ``.xpt``               -> SAS XPT (NHANES) via ``pd.read_sas``
      - ``.tsv`` / ``.txt``    -> tab-separated via ``pd.read_table``
      - any other suffix       -> CSV via ``pd.read_csv``

    Pass a ``pandas.DataFrame`` directly to skip file I/O. Extra keyword
    arguments are forwarded to the underlying pandas reader.
    """
    if isinstance(source, pd.DataFrame):
        df = source
    else:
        df = _read_phen_file(Path(source), **read_kwargs)

    return Phenotypes(
        df,
        sample_id_col=sample_id_col,
        outcomes=outcomes,
        covariates=covariates,
        exposures=exposures,
        weights_col=weights_col,
        strata_col=strata_col,
        cluster_col=cluster_col,
    )


def read_nhanes_xpt(
    path: PathLike,
    *,
    sample_id_col: str = "SEQN",
    **phen_kwargs,
) -> Phenotypes:
    """
    Convenience wrapper for NHANES XPT files. Defaults ``sample_id_col``
    to ``SEQN`` (the NHANES participant identifier).
    """
    return read_phenotypes(
        Path(path),
        sample_id_col=sample_id_col,
        **phen_kwargs,
    )


def _read_phen_file(path: Path, **read_kwargs) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".xpt":
        return pd.read_sas(path, format="xport", encoding="utf-8")
    if suffix in (".tsv", ".txt"):
        return pd.read_table(path, **read_kwargs)
    return pd.read_csv(path, **read_kwargs)
