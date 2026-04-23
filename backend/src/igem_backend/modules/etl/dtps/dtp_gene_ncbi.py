"""
NCBI Gene supplemental DTP.

Pipeline role:
- Second step in the Gene ingestion pipeline.
- Adds human genes not curated by HGNC (no hgnc_id match).
- Ensembl DTP (step 3) adds genomic coordinates for all loaded genes.

What is loaded:
- Entity (type=Genes) + EntityAlias per symbol (human tax_id=9606 only)
- GeneMaster with NCBI-derived locus_group, hgnc_status="Gene from NCBI"
- GeneGroupMembership to the "NCBI Gene" fallback group
"""

import gc
import re
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin


def _extract_dbxref(dbxrefs: str, prefix: str) -> Optional[str]:
    """Extract a value from the '|'-delimited NCBI dbXrefs string."""
    if not dbxrefs or dbxrefs == "-":
        return None
    for item in dbxrefs.split("|"):
        if item.startswith(prefix):
            return item.split(":")[-1]
    return None


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_gene_ncbi"
    DTP_VERSION = "1.0.0"
    DTP_TYPE = "master"
    ROLLBACK_STRATEGY = "deactivate"

    ALIAS_SCHEMA = {
        "symbol":     ("preferred", "NCBI",    True),
        "hgnc_id":    ("code",      "HGNC",    None),
        "ensembl_id": ("code",      "ENSEMBL", None),
        "entrez_id":  ("code",      "NCBI",    None),
        "full_name":  ("synonym",   "NCBI",    None),
        "synonyms":   ("synonym",   "NCBI",    None),
    }

    # Map NCBI type_of_gene → HGNC-compatible locus_group names
    _LOCUS_GROUP_MAP = {
        "protein-coding": "protein-coding gene",
        "pseudo":         "pseudogene",
        "ncRNA":          "RNA gene",
        "rRNA":           "RNA gene",
        "tRNA":           "RNA gene",
        "snRNA":          "RNA gene",
        "snoRNA":         "RNA gene",
        "miscRNA":        "RNA gene",
        "other":          "other",
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
    def extract(
        self, raw_dir: str
    ) -> tuple[bool, str, Optional[str], ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Extract starting...", "INFO")
        try:
            landing = self._dtp_dir(raw_dir)
            gz_path = landing / "gene_info.gz"

            url = self.data_source.source_url
            self.logger.log(f"Downloading gene_info.gz from {url}", "INFO")

            resp = requests.get(url, stream=True, timeout=300)
            resp.raise_for_status()

            with open(gz_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

            file_hash = self._hash_file(gz_path)
            file_size = gz_path.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = f"gene_info.gz downloaded to {gz_path}"
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
            input_file = self._dtp_dir(raw_dir) / "gene_info.gz"
            if not input_file.exists():
                return (
                    False,
                    f"Input file not found: {input_file}",
                    ETLStepStats(errors=1),
                )

            output_dir = self._dtp_dir(processed_dir)
            out_parquet = output_dir / "master_data.parquet"

            # gene_info.gz is ~9 GB — process in 1M-row chunks
            columns = [
                "#tax_id",
                "GeneID",
                "Symbol",
                "Synonyms",
                "dbXrefs",
                "chromosome",
                "map_location",
                "description",
                "type_of_gene",
                "Full_name_from_nomenclature_authority",
            ]
            chunks = []
            reader = pd.read_csv(
                input_file,
                sep="\t",
                compression="gzip",
                dtype=str,
                usecols=columns,
                chunksize=1_000_000,
            )
            for chunk in reader:
                filtered = chunk[chunk["#tax_id"] == "9606"].copy()
                chunks.append(filtered)
                del chunk
                gc.collect()

            df = pd.concat(chunks, ignore_index=True)

            # Rename and derive cross-reference columns
            df["entrez_id"] = df["GeneID"]
            df["symbol"] = df["Symbol"]
            df["synonyms"] = df["Synonyms"]
            df["hgnc_id"] = df["dbXrefs"].apply(
                lambda x: (
                    f"HGNC:{_extract_dbxref(x, 'HGNC:HGNC')}"
                    if _extract_dbxref(x, "HGNC:HGNC")
                    else None
                )
            )
            df["ensembl_id"] = df["dbXrefs"].apply(
                lambda x: _extract_dbxref(x, "Ensembl")
            )
            df["full_name"] = df["Full_name_from_nomenclature_authority"]

            out_df = df[[
                "entrez_id", "symbol", "synonyms", "hgnc_id", "ensembl_id",
                "full_name", "description", "chromosome",
                "map_location", "type_of_gene",
            ]]

            out_df.to_parquet(out_parquet, index=False)
            if self.debug_mode:
                out_df.to_csv(output_dir / "master_data.csv", index=False)

            stats = ETLStepStats(
                total=len(out_df),
                columns=len(out_df.columns),
                output_size_bytes=out_parquet.stat().st_size,
            )
            msg = (
                f"Transformed {len(out_df)} NCBI human gene records"
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
            return False, f"Could not read parquet: {e}", ETLStepStats(errors=1)

        # Keep only genes NOT curated by HGNC
        df = df[df["hgnc_id"].isnull()]
        # Drop invalid symbols
        invalid_symbols = {"-", "unknown", "n/a"}
        df = df[~df["symbol"].str.lower().isin(invalid_symbols)]
        # Drop genes with no region hint
        df = df[df["map_location"] != "-"]

        try:
            type_id = self.get_entity_type_id("Genes")
        except ValueError as e:
            return False, str(e), ETLStepStats(errors=1)

        from igem_backend.modules.db.models.model_genes import (
            GeneGroup,
            GeneGroupMembership,
            GeneLocusGroup,
            GeneLocusType,
            GeneMaster,
        )

        # Pre-load caches
        locus_group_cache: dict[str, int] = {
            r.name: r.id for r in self.session.query(GeneLocusGroup).all()
        }
        locus_type_cache: dict[str, int] = {
            r.name: r.id for r in self.session.query(GeneLocusType).all()
        }
        gene_group_cache: dict[str, int] = {
            r.name: r.id for r in self.session.query(GeneGroup).all()
        }

        # Ensure "NCBI Gene" group and "unknown" locus type exist
        ncbi_group_id = self._get_or_create_gene_group(
            GeneGroup, gene_group_cache, "NCBI Gene"
        )
        unknown_locus_type_id = self._get_or_create_locus(
            GeneLocusType, locus_type_cache, "unknown"
        )

        total = created = updated = warnings = 0
        gene_masters_created = gene_masters_updated = 0
        locus_groups_before = len(locus_group_cache)
        BATCH = 500

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                warnings += 1
                continue

            aliases = self.build_aliases(row, self.ALIAS_SCHEMA)
            # Drop blank or placeholder alias values
            aliases = [
                a for a in aliases
                if str(a.get("alias_value", "")).strip() not in {"", "-"}
            ]
            primary = next((a for a in aliases if a.get("is_primary")), None)
            if not primary:
                warnings += 1
                continue

            secondary = [a for a in aliases if a is not primary]

            entity_id, is_new = self.get_or_create_entity(
                name=primary["alias_value"],
                type_id=type_id,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                alias_type=primary["alias_type"],
                xref_source=primary["xref_source"],
                alias_norm=primary["alias_norm"],
                is_active=True,
                auto_commit=False,
            )
            if entity_id is None:
                warnings += 1
                continue

            if is_new:
                created += 1
            else:
                updated += 1

            self.add_aliases(
                entity_id=entity_id,
                type_id=type_id,
                aliases=secondary,
                is_active=True,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                auto_commit=False,
            )

            # --- GeneMaster ---
            type_of_gene = str(row.get("type_of_gene") or "").strip()
            locus_group_name = self._LOCUS_GROUP_MAP.get(
                type_of_gene, type_of_gene or "other"
            )
            locus_group_id = self._get_or_create_locus(
                GeneLocusGroup, locus_group_cache, locus_group_name
            )
            chromosome = self._parse_chromosome(row.get("chromosome"))

            gm = (
                self.session.query(GeneMaster)
                .filter_by(entity_id=entity_id)
                .one_or_none()
            )
            if gm is None:
                gm = GeneMaster(
                    entity_id=entity_id,
                    symbol=symbol,
                    hgnc_status="Gene from NCBI",
                    chromosome=chromosome,
                    locus_group_id=locus_group_id,
                    locus_type_id=unknown_locus_type_id,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(gm)
                gene_masters_created += 1
            else:
                gm.symbol = symbol
                gm.chromosome = chromosome
                gm.locus_group_id = locus_group_id
                gene_masters_updated += 1

            self.session.flush()

            # Attach to "NCBI Gene" group
            exists = (
                self.session.query(GeneGroupMembership)
                .filter_by(gene_id=gm.id, group_id=ncbi_group_id)
                .one_or_none()
            )
            if exists is None:
                self.session.add(GeneGroupMembership(
                    gene_id=gm.id,
                    group_id=ncbi_group_id,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                ))

            if (i + 1) % BATCH == 0:
                try:
                    self.session.commit()
                    self.logger.log(
                        f"[{self.DTP_NAME}] Committed batch {i + 1}",
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
            extras={
                "gene_masters_created": gene_masters_created,
                "gene_masters_updated": gene_masters_updated,
                "locus_groups_created": (
                    len(locus_group_cache) - locus_groups_before
                ),
                "truncations": self.trunc_metrics.copy(),
            },
        )
        msg = (
            f"[{self.DTP_NAME}] Load complete: "
            f"total={total} created={created} "
            f"updated={updated} warnings={warnings}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _parse_chromosome(location) -> Optional[int]:
        """Encode NCBI chromosome string to 1-22 autosomes, 23=X, 24=Y, 25=MT."""
        if not location or isinstance(location, float):
            return None
        loc = str(location).strip().lower()
        if not loc or loc in ("nan", "reserved", "-", "un", "unknown"):
            return None
        if "mitochondria" in loc or loc in ("mt", "m"):
            return 25
        if loc.startswith("x"):
            return 23
        if loc.startswith("y"):
            return 24
        m = re.match(r"^(\d+)", loc)
        if m:
            n = int(m.group(1))
            return n if 1 <= n <= 22 else None
        return None

    def _get_or_create_locus(
        self, model_class, cache: dict, name
    ) -> Optional[int]:
        if not name or isinstance(name, float):
            return None
        name = str(name).strip()
        if not name or name == "nan":
            return None
        if name in cache:
            return cache[name]
        obj = (
            self.session.query(model_class).filter_by(name=name).one_or_none()
        )
        if obj is None:
            obj = model_class(
                name=name,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(obj)
            self.session.flush()
        cache[name] = obj.id
        return obj.id

    def _get_or_create_gene_group(
        self, model_class, cache: dict, name: str
    ) -> int:
        if name in cache:
            return cache[name]
        obj = (
            self.session.query(model_class).filter_by(name=name).one_or_none()
        )
        if obj is None:
            obj = model_class(
                name=name,
                data_source_id=self.data_source.id,
                etl_package_id=self.package.id,
            )
            self.session.add(obj)
            self.session.flush()
        cache[name] = obj.id
        return obj.id
