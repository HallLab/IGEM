"""
Pfam Domain Definitions DTP.

Pipeline role:
- Standalone lookup-table loader; no Entity dependency.
- Must run BEFORE dtp_protein_uniprot so ProteinPfamLink resolution works.

What is loaded:
- ProteinPfam: one row per Pfam domain/family entry.
  - pfam_acc (PF00001), pfam_id (7tm_1), description, type, long_description, clan_acc
  - clan_name is not present in the source file and left NULL.
- Upserted by pfam_acc (UPDATE on re-run, INSERT on first run).
"""

from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

# Columns in the pfamA.txt.gz MySQL dump (tab-separated, no header).
# Full schema has 25 columns; we only need these indices.
_COL_IDX = {
    "pfam_acc":        0,
    "pfam_id":         1,
    "description":     3,
    "type":            6,
    "long_description": 7,
}
# clan_acc is always the last column (index 24)
_CLAN_COL = 24


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_protein_pfam"
    DTP_VERSION = "1.0.0"
    DTP_TYPE = "master"
    ROLLBACK_STRATEGY = "deactivate"

    _FILE = "pfamA.txt.gz"

    def __init__(self, logger, debug_mode, datasource, package, session, db):
        super().__init__()
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db

    # -------------------------------------------------------------------------
    # EXTRACT
    # -------------------------------------------------------------------------
    def extract(
        self, raw_dir: str
    ) -> tuple[bool, str, Optional[str], ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Extract starting...", "INFO")
        try:
            landing = self._dtp_dir(raw_dir)
            file_path = landing / self._FILE

            url = self.data_source.source_url
            self.logger.log(f"Downloading Pfam from {url}", "INFO")

            resp = requests.get(url, stream=True, timeout=300)
            resp.raise_for_status()

            with open(file_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

            file_hash = self._hash_file(file_path)
            file_size = file_path.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = f"Pfam data downloaded to {file_path}"
            self.logger.log(msg, "INFO")
            return True, msg, file_hash, stats

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Extract failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, None, ETLStepStats(errors=1)

    # -------------------------------------------------------------------------
    # TRANSFORM
    # -------------------------------------------------------------------------
    def transform(
        self, raw_dir: str, processed_dir: str
    ) -> tuple[bool, str, ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Transform starting...", "INFO")
        try:
            input_file = self._dtp_dir(raw_dir) / self._FILE
            if not input_file.exists():
                return (
                    False,
                    f"Input file not found: {input_file}",
                    ETLStepStats(errors=1),
                )

            output_dir = self._dtp_dir(processed_dir)
            out_parquet = output_dir / "master_data.parquet"

            df_raw = pd.read_csv(
                input_file,
                sep="\t",
                header=None,
                dtype=str,
                na_filter=False,
                compression="gzip",
                on_bad_lines="skip",
            )

            col_count = df_raw.shape[1]
            df = pd.DataFrame({
                "pfam_acc":         df_raw.iloc[:, _COL_IDX["pfam_acc"]],
                "pfam_id":          df_raw.iloc[:, _COL_IDX["pfam_id"]],
                "description":      df_raw.iloc[:, _COL_IDX["description"]],
                "type":             df_raw.iloc[:, _COL_IDX["type"]],
                "long_description": df_raw.iloc[:, _COL_IDX["long_description"]],
                "clan_acc": (
                    df_raw.iloc[:, _CLAN_COL]
                    if col_count > _CLAN_COL
                    else None
                ),
            })

            # Replace empty strings with None for optional fields
            for col in ("clan_acc", "long_description"):
                df[col] = df[col].replace("", None)

            # Drop rows with no accession
            df = df[df["pfam_acc"].str.strip() != ""]

            df.to_parquet(out_parquet, index=False)
            if self.debug_mode:
                df.to_csv(output_dir / "master_data.csv", index=False)

            stats = ETLStepStats(
                total=len(df),
                columns=len(df.columns),
                output_size_bytes=out_parquet.stat().st_size,
            )
            msg = f"Transformed {len(df)} Pfam entries -> {out_parquet}"
            self.logger.log(msg, "INFO")
            return True, msg, stats

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Transform failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, ETLStepStats(errors=1)

    # -------------------------------------------------------------------------
    # LOAD
    # -------------------------------------------------------------------------
    def load(self, processed_dir: str) -> tuple[bool, str, ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Load starting...", "INFO")

        parquet_file = self._dtp_dir(processed_dir) / "master_data.parquet"
        if not parquet_file.exists():
            return (
                False,
                f"Processed file not found: {parquet_file}",
                ETLStepStats(errors=1),
            )

        try:
            df = pd.read_parquet(parquet_file, engine="pyarrow")
        except Exception as e:
            return False, f"Could not read parquet: {e}", ETLStepStats(errors=1)

        from igem_backend.modules.db.models.model_proteins import ProteinPfam

        # Cache existing accessions to avoid per-row SELECT
        existing: dict[str, int] = {
            r.pfam_acc: r.id
            for r in self.session.query(ProteinPfam.pfam_acc, ProteinPfam.id).all()
        }

        total = created = updated = warnings = 0
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1
            pfam_acc = str(row.get("pfam_acc") or "").strip()
            if not pfam_acc:
                warnings += 1
                continue

            description = self.guard_short(row.get("description") or None)
            long_desc = self.guard_short(row.get("long_description") or None)
            pfam_type = self.guard_short(row.get("type") or None)
            clan_acc = self.guard_short(row.get("clan_acc") or None)
            pfam_id = self.guard_short(row.get("pfam_id") or None)

            if pfam_acc in existing:
                obj = (
                    self.session.query(ProteinPfam)
                    .filter_by(pfam_acc=pfam_acc)
                    .one_or_none()
                )
                if obj:
                    obj.pfam_id = pfam_id
                    obj.description = description
                    obj.long_description = long_desc
                    obj.type = pfam_type
                    obj.clan_acc = clan_acc
                    updated += 1
            else:
                obj = ProteinPfam(
                    pfam_acc=pfam_acc,
                    pfam_id=pfam_id,
                    description=description,
                    long_description=long_desc,
                    type=pfam_type,
                    clan_acc=clan_acc,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(obj)
                existing[pfam_acc] = -1
                created += 1

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    self.logger.log(
                        f"[{self.DTP_NAME}] Committed batch {i + 1}/{total}",
                        "DEBUG",
                    )
                except Exception as e:
                    self.session.rollback()
                    return (
                        False,
                        f"Batch commit failed at row {i + 1}: {e}",
                        ETLStepStats(errors=1),
                    )

        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            return False, f"Final commit failed: {e}", ETLStepStats(errors=1)

        self._log_trunc_summary()
        stats = ETLStepStats(
            total=total,
            created=created,
            updated=updated,
            warnings=warnings,
        )
        msg = (
            f"[{self.DTP_NAME}] Load complete: "
            f"total={total} created={created} updated={updated} warnings={warnings}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats
