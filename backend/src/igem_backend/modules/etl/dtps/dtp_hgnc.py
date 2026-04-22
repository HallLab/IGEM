"""
HGNC Gene Baseline DTP.

Pipeline role:
- First step in the Gene ingestion pipeline.
- Establishes the canonical Gene entity universe in IGEM.
- Later DTPs (gene_ncbi, CTD, etc.) reference gene entities created here.

What is loaded:
- Entity (group=Genes) per approved HGNC gene symbol
- EntityAlias records: symbol, hgnc_id, ensembl_id, entrez_id, ucsc_id,
  gene name, previous symbols, aliases
"""

import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_hgnc"
    DTP_VERSION = "1.0.0"

    ALIAS_SCHEMA = {
        "symbol":         ("preferred", "HGNC",    True),
        "hgnc_id":        ("code",      "HGNC",    None),
        "ensembl_gene_id":("code",      "ENSEMBL", None),
        "entrez_id":      ("code",      "NCBI",    None),
        "ucsc_id":        ("code",      "UCSC",    None),
        "name":           ("synonym",   "HGNC",    None),
        "prev_symbol":    ("synonym",   "HGNC",    None),
        "prev_name":      ("synonym",   "HGNC",    None),
        "alias_symbol":   ("synonym",   "HGNC",    None),
        "alias_name":     ("synonym",   "HGNC",    None),
    }

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
    def extract(self, raw_dir: str) -> tuple[bool, str, Optional[str]]:
        self.logger.log(f"[{self.DTP_NAME}] Extract starting...", "INFO")
        try:
            landing = self._dtp_dir(raw_dir)
            file_path = landing / "hgnc_data.json"

            url = self.data_source.source_url
            self.logger.log(f"Fetching HGNC JSON from {url}", "INFO")

            resp = requests.get(url, headers={"Accept": "application/json"}, timeout=60)
            resp.raise_for_status()

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(resp.text)

            file_hash = self._hash_file(file_path)
            msg = f"HGNC data downloaded to {file_path}"
            self.logger.log(msg, "INFO")
            return True, msg, file_hash

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Extract failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # -------------------------------------------------------------------------
    # TRANSFORM
    # -------------------------------------------------------------------------
    def transform(self, raw_dir: str, processed_dir: str) -> tuple[bool, str]:
        self.logger.log(f"[{self.DTP_NAME}] Transform starting...", "INFO")
        try:
            input_file = self._dtp_dir(raw_dir) / "hgnc_data.json"
            if not input_file.exists():
                msg = f"Input file not found: {input_file}"
                return False, msg

            output_dir = self._dtp_dir(processed_dir)
            out_parquet = output_dir / "master_data.parquet"

            with open(input_file, encoding="utf-8") as f:
                data = json.load(f)

            df = pd.DataFrame(data["response"]["docs"])
            df.to_parquet(out_parquet, index=False)

            if self.debug_mode:
                df.to_csv(output_dir / "master_data.csv", index=False)

            msg = f"Transformed {len(df)} HGNC records -> {out_parquet}"
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Transform failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # -------------------------------------------------------------------------
    # LOAD
    # -------------------------------------------------------------------------
    def load(self, processed_dir: str) -> tuple[bool, str]:
        self.logger.log(f"[{self.DTP_NAME}] Load starting...", "INFO")

        parquet_file = self._dtp_dir(processed_dir) / "master_data.parquet"
        if not parquet_file.exists():
            msg = f"Processed file not found: {parquet_file}"
            return False, msg

        try:
            df = pd.read_parquet(parquet_file, engine="pyarrow")
        except Exception as e:
            return False, f"Could not read parquet: {e}"

        try:
            group_id = self.get_entity_group_id("Genes")
        except ValueError as e:
            return False, str(e)

        total = created = warnings = 0

        for _, row in df.iterrows():
            total += 1
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                warnings += 1
                continue

            is_active = row.get("status") == "Approved"
            aliases = self.build_aliases(row, self.ALIAS_SCHEMA)

            primary = next((a for a in aliases if a.get("is_primary")), None)
            if not primary:
                warnings += 1
                continue

            secondary = [a for a in aliases if a is not primary]

            entity_id, is_new = self.get_or_create_entity(
                name=primary["alias_value"],
                group_id=group_id,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                alias_type=primary["alias_type"],
                xref_source=primary["xref_source"],
                alias_norm=primary["alias_norm"],
                is_active=is_active,
                auto_commit=False,
            )

            if entity_id is None:
                warnings += 1
                continue

            if is_new:
                created += 1

            self.add_aliases(
                entity_id=entity_id,
                group_id=group_id,
                aliases=secondary,
                is_active=is_active,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                auto_commit=False,
            )

        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            return False, f"Final commit failed: {e}"

        self._log_trunc_summary()
        msg = (f"[{self.DTP_NAME}] Load complete: "
               f"total={total} created={created} warnings={warnings}")
        self.logger.log(msg, "INFO")
        return True, msg
