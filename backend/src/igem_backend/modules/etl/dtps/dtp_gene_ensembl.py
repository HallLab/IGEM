"""
Ensembl gene coordinate enrichment DTP.

Pipeline role:
- Third step in the Gene ingestion pipeline.
- Depends on existing GeneMaster rows (from HGNC + NCBI DTPs).
- Maps gene symbols to GRCh38 genomic coordinates via Ensembl GFF3.

What is loaded:
- EntityLocation: chromosome, start_pos, end_pos, strand per gene
- Matched by (symbol.upper(), chromosome_int) against GeneMaster index
- Upserted into entity_locations by (entity_id, assembly_id)
- Genes not found in GeneMaster are skipped with a DEBUG log
"""

import gzip
import re
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_gene_ensembl"
    DTP_VERSION = "1.0.0"
    DTP_TYPE = "master"
    ROLLBACK_STRATEGY = "deactivate"

    # Local filename used for the downloaded GFF3 — version-agnostic
    GFF3_LOCAL = "ensembl_genes.gff3.gz"

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
            file_path = landing / self.GFF3_LOCAL

            url = self._resolve_gff3_url(self.data_source.source_url)
            self.logger.log(f"Downloading GFF3 from {url}", "INFO")

            resp = requests.get(url, stream=True, timeout=600)
            resp.raise_for_status()

            with open(file_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

            file_hash = self._hash_file(file_path)
            file_size = file_path.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = f"GFF3 downloaded to {file_path}"
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
            input_file = self._dtp_dir(raw_dir) / self.GFF3_LOCAL
            if not input_file.exists():
                return (
                    False,
                    f"Input file not found: {input_file}",
                    ETLStepStats(errors=1),
                )

            output_dir = self._dtp_dir(processed_dir)
            out_parquet = output_dir / "master_data.parquet"

            records = []
            with gzip.open(input_file, "rt", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("#"):
                        continue
                    cols = line.strip().split("\t")
                    if len(cols) < 9:
                        continue

                    chrom, _, _, start, end, _, strand, _, attrs = cols

                    # Parse "key=val;key=val" attribute string
                    attr_dict: dict[str, str] = {}
                    for entry in attrs.split(";"):
                        if "=" in entry:
                            k, v = entry.split("=", 1)
                            attr_dict[k.strip()] = v.strip()

                    # Keep only gene-level features
                    if not attr_dict.get("ID", "").startswith("gene:"):
                        continue

                    strand_val = strand if strand in ("+", "-") else None
                    records.append({
                        "gene_id":     attr_dict.get("ID"),
                        "gene_symbol": attr_dict.get("Name"),
                        "biotype":     attr_dict.get("biotype"),
                        "chromosome":  chrom,
                        "start":       int(start),
                        "end":         int(end),
                        "strand":      strand_val,
                    })

            df = pd.DataFrame(records)
            df.to_parquet(out_parquet, index=False)
            if self.debug_mode:
                df.to_csv(output_dir / "master_data.csv", index=False)

            stats = ETLStepStats(
                total=len(df),
                columns=len(df.columns),
                output_size_bytes=out_parquet.stat().st_size,
            )
            msg = (
                f"Transformed {len(df)} Ensembl gene records"
                f" -> {out_parquet}"
            )
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
            return (
                False,
                f"Could not read parquet: {e}",
                ETLStepStats(errors=1),
            )

        # Drop rows missing gene symbol or chromosome
        initial_len = len(df)
        df = df.dropna(subset=["gene_symbol", "chromosome"])
        df = df[df["gene_symbol"].astype(str).str.strip() != ""]
        dropped = initial_len - len(df)
        if dropped:
            self.logger.log(
                f"Dropped {dropped} rows with missing symbol/chromosome",
                "DEBUG",
            )

        if df.empty:
            return (
                False,
                "No valid rows after filtering",
                ETLStepStats(errors=1),
            )

        from igem_backend.modules.db.models.model_config import (
            GenomeAssembly,
        )
        from igem_backend.modules.db.models.model_entities import (
            EntityLocation,
        )
        from igem_backend.modules.db.models.model_genes import GeneMaster

        # --- Resolve GRCh38.p14 assembly (single row) ---
        assembly = (
            self.session.query(GenomeAssembly)
            .filter_by(assembly_name="GRCh38.p14")
            .first()
        )
        if not assembly:
            return (
                False,
                "GenomeAssembly 'GRCh38.p14' not found — run db upgrade",
                ETLStepStats(errors=1),
            )
        assembly_id = assembly.id

        # --- Resolve Genes entity type ---
        try:
            type_id = self.get_entity_type_id("Genes")
        except ValueError as e:
            return False, str(e), ETLStepStats(errors=1)

        # --- Build gene index: (symbol_upper, chrom_int) → entity_id ---
        gene_index: dict[tuple[str, int], int] = {}
        for gm in self.session.query(GeneMaster).all():
            if gm.symbol and gm.chromosome is not None:
                gene_index[(gm.symbol.upper(), int(gm.chromosome))] = (
                    gm.entity_id
                )

        if not gene_index:
            self.logger.log(
                "GeneMaster index is empty — run HGNC and NCBI DTPs first",
                "WARNING",
            )

        # --- Build EntityLocation records ---
        records: list[dict] = []
        seen_keys: set[tuple[int, int]] = set()
        skipped = created = 0

        for _, row in df.iterrows():
            symbol = str(row["gene_symbol"]).strip()
            chrom_raw = str(row["chromosome"]).strip()

            chrom_int = self._map_chrom_to_int(chrom_raw)
            if chrom_int is None:
                skipped += 1
                continue

            entity_id = gene_index.get((symbol.upper(), chrom_int))
            if entity_id is None:
                skipped += 1
                self.logger.log(
                    f"Gene not found: symbol={symbol} chrom={chrom_raw}",
                    "DEBUG",
                )
                continue

            loc_key = (entity_id, assembly_id)
            if loc_key in seen_keys:
                skipped += 1
                continue
            seen_keys.add(loc_key)

            try:
                start_pos = int(row["start"])
                end_pos = int(row["end"])
            except (ValueError, TypeError):
                skipped += 1
                continue

            records.append({
                "entity_id":      entity_id,
                "entity_type_id": type_id,
                "assembly_id":    assembly_id,
                "chromosome":     chrom_int,
                "start_pos":      start_pos,
                "end_pos":        end_pos,
                "strand":         row.get("strand"),
                "region_label":   None,
                "data_source_id": self.data_source.id,
                "etl_package_id": self.package.id,
            })
            created += 1

        if records:
            try:
                self._upsert_locations(records, EntityLocation)
            except Exception as e:
                self.session.rollback()
                return (
                    False,
                    f"Upsert EntityLocation failed: {e}",
                    ETLStepStats(errors=1),
                )

        stats = ETLStepStats(
            total=len(df),
            created=created,
            skipped=skipped,
        )
        msg = (
            f"[{self.DTP_NAME}] Load complete: "
            f"upserted={created} skipped={skipped}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    def _resolve_gff3_url(self, url: str) -> str:
        """
        Return the direct download URL for the Ensembl GFF3 file.

        If *url* already ends in '.gz' it is used as-is (pinned version).
        Otherwise it is treated as a directory URL and we discover the
        current release by scanning the FTP index for the canonical
        'Homo_sapiens.GRCh38.<release>.chr.gff3.gz' filename.
        This makes the seed URL release-agnostic:
            source_url = "https://ftp.ensembl.org/pub/current_gff3/homo_sapiens/"
        """
        if url.rstrip("/").endswith(".gz"):
            return url

        dir_url = url if url.endswith("/") else url + "/"
        self.logger.log(
            f"Discovering current GFF3 release from {dir_url}", "INFO"
        )
        resp = requests.get(dir_url, timeout=30)
        resp.raise_for_status()

        pattern = r"Homo_sapiens\.GRCh38\.\d+\.chr\.gff3\.gz"
        matches = re.findall(pattern, resp.text)
        if not matches:
            raise ValueError(
                f"No GFF3 file found at {dir_url} — "
                "check Ensembl FTP structure"
            )
        filename = sorted(matches)[-1]
        self.logger.log(f"Resolved GFF3 file: {filename}", "INFO")
        return dir_url + filename

    @staticmethod
    def _map_chrom_to_int(chrom_raw: str) -> Optional[int]:
        """Map GFF3 chromosome label to integer (1-22, 23=X, 24=Y, 25=MT)."""
        chrom = str(chrom_raw).replace("chr", "").strip()
        if chrom.upper() == "X":
            return 23
        if chrom.upper() == "Y":
            return 24
        if chrom.upper() in ("MT", "M"):
            return 25
        try:
            val = int(chrom)
            return val if 1 <= val <= 22 else None
        except ValueError:
            return None

    def _upsert_locations(self, records: list[dict], model) -> None:
        """
        Bulk upsert EntityLocation using dialect-aware ON CONFLICT.

        Chunk sizes respect SQLite's 999-parameter limit:
        10 columns × 99 rows = 990 params < 999.
        PostgreSQL can handle much larger batches.
        """
        from sqlalchemy import insert
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        dialect = self.session.get_bind().dialect.name

        if dialect == "postgresql":
            insert_cls = pg_insert(model)
            chunk_size = 5000
        elif dialect == "sqlite":
            insert_cls = sqlite_insert(model)
            chunk_size = 99
        else:
            insert_cls = insert(model)
            chunk_size = 500

        update_cols = [
            "chromosome", "start_pos", "end_pos", "strand",
            "entity_type_id", "region_label",
            "data_source_id", "etl_package_id",
        ]

        for start in range(0, len(records), chunk_size):
            chunk = records[start: start + chunk_size]
            stmt = insert_cls.values(chunk)

            if dialect in ("postgresql", "sqlite"):
                stmt = stmt.on_conflict_do_update(
                    index_elements=["entity_id", "assembly_id"],
                    set_={
                        col: getattr(stmt.excluded, col)
                        for col in update_cols
                    },
                )

            self.session.execute(stmt)
            self.logger.log(
                f"[{self.DTP_NAME}] Upserted batch "
                f"{start + len(chunk)}/{len(records)}",
                "DEBUG",
            )

        self.session.commit()
