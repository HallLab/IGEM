"""
HMDB NLP Relationship DTP.

Pipeline role:
- Runs AFTER chemical_hmdb (master).
- Reads master_data.parquet produced by dtp_chemical_hmdb transform.
- Runs the IGEM entity resolver over each metabolite description.
- Produces entity_matches.parquet (transform) and persists EntityMatch +
  EntityRelationship rows to the database (load).

What is loaded:
  EntityMatch  — one row per resolved entity mention in an HMDB description
  EntityRelationship — co-occurrence pairs (relation_type=co_occurs_with,
                       discovery_method="nlp") with evidence_count = number
                       of HMDB records where both entities were co-mentioned.

  Only descriptions that resolve ≥ 2 distinct entities produce relationships.
  Records with > 20 distinct entity matches are skipped (noise guard).

Performance note:
  The AliasDictionary is built once in transform() and reused for all records.
  Expect ~30–60 s for dictionary load + automaton build on a full IGEM DB,
  then ~1–5 ms per description for the Aho-Corasick scan.
"""

import gc
import hashlib
from pathlib import Path
from typing import Optional

import pandas as pd

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.nlp import EntityResolver, OutputMode
from igem_backend.modules.nlp.persister import persist_matches
from igem_backend.modules.nlp.relation_builder import build_from_matches

_HMDB_SOURCE_SYSTEM = "HMDB"
_HMDB_DTP = "chemical_hmdb"
_MATCHES_FILE = "entity_matches.parquet"


class DTP(DTPBase):

    DTP_NAME = "nlp_hmdb"
    DTP_VERSION = "1.0.0"
    DTP_TYPE = "relationship"
    ROLLBACK_STRATEGY = "delete"

    def __init__(self, logger, debug_mode, datasource, package, session, db):
        super().__init__()
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db

    # -------------------------------------------------------------------------
    # EXTRACT — no-op
    # -------------------------------------------------------------------------
    def extract(
        self, raw_dir: str
    ) -> tuple[bool, str, Optional[str], ETLStepStats]:
        msg = (
            f"[{self.DTP_NAME}] Extract not applicable — "
            "input data comes from dtp_chemical_hmdb processed output."
        )
        self.logger.log(msg, "INFO")
        return True, msg, None, ETLStepStats()

    # -------------------------------------------------------------------------
    # TRANSFORM — run entity resolver over HMDB descriptions
    # -------------------------------------------------------------------------
    def transform(
        self, raw_dir: str, processed_dir: str
    ) -> tuple[bool, str, ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Transform starting...", "INFO")
        try:
            hmdb_parquet = (
                Path(processed_dir)
                / _HMDB_SOURCE_SYSTEM
                / _HMDB_DTP
                / "master_data.parquet"
            )
            if not hmdb_parquet.exists():
                return (
                    False,
                    f"HMDB master_data.parquet not found at {hmdb_parquet} — "
                    "run chemical_hmdb (extract + transform) first.",
                    ETLStepStats(errors=1),
                )

            df_hmdb = pd.read_parquet(hmdb_parquet, engine="pyarrow")
            if df_hmdb.empty:
                return True, "No HMDB records to process.", ETLStepStats()

            # Filter rows that have a non-empty description
            df_hmdb = df_hmdb[
                df_hmdb["description"].notna()
                & (df_hmdb["description"].str.strip() != "")
            ]
            if df_hmdb.empty:
                return (
                    True,
                    "No HMDB records with descriptions to process.",
                    ETLStepStats(),
                )

            n_total = len(df_hmdb)
            self.logger.log(
                f"  {n_total:,} HMDB records with descriptions", "INFO"
            )

            # Build AliasDictionary + EntityResolver once
            self.logger.log(
                "Building AliasDictionary from DB...", "INFO"
            )
            resolver = EntityResolver(
                session=self.session,
                confidence_threshold=0.9,
            )
            self.logger.log(
                f"  Dictionary ready: "
                f"{resolver._dictionary.entry_count:,} aliases",
                "INFO",
            )

            # Streaming write: flush a chunk of matches to a partitioned
            # parquet every CHUNK_RECORDS records, then clear the buffer.
            # This prevents unbounded memory growth on the full HMDB scan.
            CHUNK_RECORDS = 1_000  # smaller chunks → tighter memory bound
            COLUMNS = [
                "source_record_id", "source_field", "text_hash",
                "matched_text", "span_start", "span_end", "context",
                "alias_id", "entity_id", "entity_type_id",
                "entity_type_name", "entity_domain",
                "match_method", "confidence", "review_status",
            ]

            out = self._dtp_dir(processed_dir)
            parquet_path = out / _MATCHES_FILE
            # Remove any stale parquet from a previous run
            if parquet_path.exists():
                parquet_path.unlink()
            # Also clean any orphan chunk files left by a killed previous run
            for stale_chunk in out.glob("_matches_chunk_*.parquet"):
                stale_chunk.unlink()

            buffer: list[dict] = []
            n_with_matches = 0
            n_total_matches = 0
            chunks_written = 0

            def _flush_buffer() -> None:
                nonlocal buffer, chunks_written
                if not buffer:
                    return
                df_chunk = pd.DataFrame(buffer, columns=COLUMNS)
                chunk_file = (
                    out / f"_matches_chunk_{chunks_written:06d}.parquet"
                )
                df_chunk.to_parquet(chunk_file, index=False)
                chunks_written += 1
                buffer.clear()
                # Force release of any cyclically-referenced match dicts;
                # without this, peak RSS keeps climbing across iterations.
                del df_chunk
                gc.collect()

            for i, (_, row) in enumerate(df_hmdb.iterrows()):
                accession = str(row.get("accession") or "").strip()
                description = str(row.get("description") or "").strip()
                if not accession or not description:
                    continue

                text_hash = hashlib.sha256(
                    description.encode()
                ).hexdigest()[:16]

                # BEST_MATCH mode: top-1 per span. SMART was equivalent
                # to ALL_CANDIDATES on this dataset because most aliases
                # are ambiguous (1/N < threshold), exploding match count.
                matches = resolver.resolve_text(
                    description,
                    mode=OutputMode.BEST_MATCH,
                    source_record_id=accession,
                    source_field="description",
                )

                # Per-record dedupe by entity_id: for co-occurrence
                # relationship building we only need to know that entity X
                # was mentioned in record Y, not every span where it
                # appeared. Keeping one match per (record, entity_id)
                # collapses repeated mentions of the same compound and
                # cuts the buffer ~10× on dense biomedical descriptions.
                seen_entities: set[int] = set()
                n_kept = 0
                for m in matches:
                    if m.entity_id in seen_entities:
                        continue
                    seen_entities.add(m.entity_id)
                    n_kept += 1
                    buffer.append({
                        "source_record_id": accession,
                        "source_field":     "description",
                        "text_hash":        text_hash,
                        "matched_text":     m.matched_text,
                        "span_start":       m.span_start,
                        "span_end":         m.span_end,
                        "context":          m.context,
                        "alias_id":         m.alias_id,
                        "entity_id":        m.entity_id,
                        "entity_type_id":   m.entity_type_id,
                        "entity_type_name": m.entity_type_name,
                        "entity_domain":    m.entity_domain,
                        "match_method":     m.match_method,
                        "confidence":       m.confidence,
                        "review_status":    m.review_status,
                    })

                if n_kept:
                    n_with_matches += 1
                    n_total_matches += n_kept

                if (i + 1) % CHUNK_RECORDS == 0:
                    _flush_buffer()

                if (i + 1) % 10_000 == 0:
                    self.logger.log(
                        f"  Processed {i + 1:,}/{n_total:,} records "
                        f"({n_with_matches:,} with matches, "
                        f"{n_total_matches:,} matches total, "
                        f"{chunks_written} chunks flushed)...",
                        "INFO",
                    )

            _flush_buffer()

            # Merge all chunks into the canonical parquet file
            chunk_files = sorted(out.glob("_matches_chunk_*.parquet"))
            if chunk_files:
                self.logger.log(
                    f"  Merging {len(chunk_files)} chunks into "
                    f"{_MATCHES_FILE}...",
                    "INFO",
                )
                df_matches = pd.concat(
                    [
                        pd.read_parquet(p, engine="pyarrow")
                        for p in chunk_files
                    ],
                    ignore_index=True,
                    copy=False,
                )
                df_matches.to_parquet(parquet_path, index=False)
                if self.debug_mode:
                    df_matches.to_csv(
                        out / "entity_matches.csv", index=False
                    )
                # Remove temporary chunk files
                for p in chunk_files:
                    p.unlink()
            else:
                # No matches found at all — write empty parquet
                df_matches = pd.DataFrame(columns=COLUMNS)
                df_matches.to_parquet(parquet_path, index=False)

            coverage = n_with_matches / n_total * 100 if n_total else 0
            stats = ETLStepStats(
                total=n_total,
                output_size_bytes=parquet_path.stat().st_size,
                extras={
                    "matches": len(df_matches),
                    "records_with_matches": n_with_matches,
                    "coverage_pct": round(coverage, 1),
                },
            )
            msg = (
                f"[{self.DTP_NAME}] Transform complete: "
                f"{n_total:,} records → {len(df_matches):,} matches "
                f"({coverage:.1f}% coverage)"
            )
            self.logger.log(msg, "INFO")
            return True, msg, stats

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Transform failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, ETLStepStats(errors=1)

    # -------------------------------------------------------------------------
    # LOAD — persist EntityMatch + build co-occurrence EntityRelationship
    # -------------------------------------------------------------------------
    def load(self, processed_dir: str) -> tuple[bool, str, ETLStepStats]:
        self.logger.log(f"[{self.DTP_NAME}] Load starting...", "INFO")

        parquet_path = self._dtp_dir(processed_dir) / _MATCHES_FILE
        if not parquet_path.exists():
            return (
                False,
                f"{_MATCHES_FILE} not found at {parquet_path} — "
                "run transform first.",
                ETLStepStats(errors=1),
            )

        try:
            df = pd.read_parquet(parquet_path, engine="pyarrow")
        except Exception as e:
            return (
                False,
                f"Could not read parquet: {e}",
                ETLStepStats(errors=1),
            )

        if df.empty:
            return True, "No entity matches to load.", ETLStepStats()

        # --- Step 1: Persist EntityMatch rows ---
        self.logger.log(
            f"  Persisting {len(df):,} EntityMatch rows...", "INFO"
        )
        em_created, em_errors = persist_matches(
            df=df,
            session=self.session,
            etl_package_id=self.package.id,
        )
        self.logger.log(
            f"  EntityMatch: created={em_created:,} errors={em_errors:,}",
            "INFO",
        )

        # --- Step 2: Build co-occurrence EntityRelationship pairs ---
        self.logger.log(
            "  Building co-occurrence EntityRelationship pairs...", "INFO"
        )
        rel_created, rel_skipped = build_from_matches(
            df=df,
            session=self.session,
            data_source_id=self.data_source.id,
            etl_package_id=self.package.id,
        )
        self.logger.log(
            f"  EntityRelationship: created={rel_created:,} "
            f"skipped={rel_skipped:,}",
            "INFO",
        )

        stats = ETLStepStats(
            total=len(df),
            created=em_created + rel_created,
            skipped=rel_skipped,
            warnings=em_errors,
            extras={
                "entity_matches_created": em_created,
                "relationships_created":  rel_created,
                "relationships_skipped":  rel_skipped,
            },
        )
        msg = (
            f"[{self.DTP_NAME}] Load complete: "
            f"matches={em_created:,} relationships={rel_created:,} "
            f"rel_skipped={rel_skipped:,}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats
