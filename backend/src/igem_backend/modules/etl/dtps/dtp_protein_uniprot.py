"""
UniProt Human Protein Master DTP.

Pipeline role:
- Master DTP for the Proteins entity universe.
- Depends on dtp_protein_pfam (for ProteinPfamLink resolution).
  If Pfam is not loaded, Pfam links are skipped with a WARNING.

What is loaded:
- Entity (type=Proteins) + EntityAlias per reviewed human protein.
  Primary alias: UniProt accession (e.g. P04637).
  Secondary aliases: UniProt entry name (e.g. P53_HUMAN), gene symbol,
  full name.
- ProteinMaster: function, subcellular location, tissue expression.
- ProteinEntity: links Entity → ProteinMaster (is_isoform=False for canonical).
- Isoforms: separate Entity + EntityAlias + ProteinEntity (is_isoform=True).
- ProteinPfamLink: protein → Pfam domain associations.

Source: UniProt human reference proteome (UP000005640), reviewed
Swiss-Prot entries.
Format: gzip-compressed UniProt XML.
"""

import gzip
import json
import xml.etree.ElementTree as ET
from typing import Optional

import pandas as pd
import requests

from igem_backend.modules.etl.mixins.base_dtp import DTPBase, ETLStepStats
from igem_backend.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

_NS = "https://uniprot.org/uniprot"
_TAG = f"{{{_NS}}}"
_FILE = "uniprot_human.xml.gz"

# Only include canonical human proteins
_HUMAN_TAX_ID = "9606"


def _text(element, path: str, ns: dict) -> Optional[str]:
    """Find a child element by path and return its text, or None."""
    el = element.find(path, ns)
    return el.text.strip() if el is not None and el.text else None


class DTP(DTPBase, EntityQueryMixin):

    DTP_NAME = "dtp_protein_uniprot"
    DTP_VERSION = "1.0.0"
    DTP_TYPE = "master"
    ROLLBACK_STRATEGY = "deactivate"

    ALIAS_SCHEMA = {
        "uniprot_id":   ("code",      "UNIPROT", True),
        "uniprot_name": ("preferred", "UNIPROT", None),
        "gene_symbol":  ("synonym",   "UNIPROT", None),
        "full_name":    ("synonym",   "UNIPROT", None),
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
            file_path = landing / _FILE

            url = self.data_source.source_url
            self.logger.log(f"Downloading UniProt XML from {url}", "INFO")

            resp = requests.get(url, stream=True, timeout=600)
            resp.raise_for_status()

            with open(file_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

            file_hash = self._hash_file(file_path)
            file_size = file_path.stat().st_size
            stats = ETLStepStats(file_size_bytes=file_size)
            msg = f"UniProt XML downloaded to {file_path}"
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
            input_file = self._dtp_dir(raw_dir) / _FILE
            if not input_file.exists():
                return (
                    False,
                    f"Input file not found: {input_file}",
                    ETLStepStats(errors=1),
                )

            output_dir = self._dtp_dir(processed_dir)
            out_parquet = output_dir / "master_data.parquet"

            ns = {"up": _NS}
            records = []
            skipped_non_human = 0

            with gzip.open(input_file, "rb") as gz:
                context = ET.iterparse(gz, events=("end",))
                for event, elem in context:
                    if elem.tag != f"{_TAG}entry":
                        continue

                    # Filter: only human entries
                    tax_ref = elem.find(
                        "up:organism/up:dbReference[@type='NCBI Taxonomy']", ns
                    )
                    tax_id = (
                        tax_ref.attrib.get("id")
                        if tax_ref is not None
                        else None
                    )
                    if tax_id != _HUMAN_TAX_ID:
                        skipped_non_human += 1
                        elem.clear()
                        continue

                    row = self._parse_entry(elem, ns)
                    if row:
                        records.append(row)

                    elem.clear()

            if skipped_non_human:
                self.logger.log(
                    f"Skipped {skipped_non_human} non-human entries", "DEBUG"
                )

            # Store list columns as JSON strings for parquet compatibility.
            df = pd.DataFrame(records)
            list_cols = (
                "secondary_ids", "isoforms", "pfam_ids", "go_terms", "refseq_ids"
            )
            for col in list_cols:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda v: json.dumps(v) if isinstance(v, list) else v
                    )

            # --- master_data.parquet ---
            master_cols = [
                "uniprot_id", "uniprot_name", "gene_symbol", "full_name",
                "function", "location", "tissue_expression",
                "secondary_ids", "isoforms", "pfam_ids",
            ]
            df[master_cols].to_parquet(out_parquet, index=False)
            if self.debug_mode:
                df[master_cols].to_csv(
                    output_dir / "master_data.csv", index=False
                )

            # --- relationship_data.parquet ---
            rel_rows: list[dict] = []
            for _, row in df.iterrows():
                uid = row["uniprot_id"]

                if row.get("hgnc_id"):
                    rel_rows.append({
                        "source_id":   uid,
                        "target_id":   str(row["hgnc_id"]),
                        "source_type": "Proteins",
                        "target_type": "Genes",
                        "relation_type": "encodes",
                    })

                for go_id in self._as_list(row.get("go_terms")):
                    rel_rows.append({
                        "source_id":   uid,
                        "target_id":   go_id,
                        "source_type": "Proteins",
                        "target_type": "Gene Ontology",
                        "relation_type": "part_of",
                    })

                if row.get("kegg_id"):
                    rel_rows.append({
                        "source_id":   uid,
                        "target_id":   str(row["kegg_id"]),
                        "source_type": "Proteins",
                        "target_type": "Pathways",
                        "relation_type": "in_pathway",
                    })

                for refseq_id in self._as_list(row.get("refseq_ids")):
                    rel_rows.append({
                        "source_id":   uid,
                        "target_id":   refseq_id,
                        "source_type": "Proteins",
                        "target_type": "Transcriptomics",
                        "relation_type": "has_transcript",
                    })

            rel_df = pd.DataFrame(rel_rows)
            out_rel = output_dir / "relationship_data.parquet"
            rel_df.to_parquet(out_rel, index=False)
            if self.debug_mode:
                rel_df.to_csv(
                    output_dir / "relationship_data.csv", index=False
                )

            self.logger.log(
                f"Relationship rows generated: {len(rel_df)}", "INFO"
            )

            stats = ETLStepStats(
                total=len(df),
                columns=len(df[master_cols].columns),
                output_size_bytes=out_parquet.stat().st_size,
                extras={"relationship_rows": len(rel_df)},
            )
            msg = f"Transformed {len(df)} UniProt entries -> {out_parquet}"
            self.logger.log(msg, "INFO")
            return True, msg, stats

        except Exception as e:
            msg = f"[{self.DTP_NAME}] Transform failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg, ETLStepStats(errors=1)

    def _parse_entry(self, entry, ns: dict) -> Optional[dict]:
        accessions = entry.findall("up:accession", ns)
        if not accessions:
            return None
        uniprot_id = accessions[0].text.strip()
        secondary_ids = [a.text.strip() for a in accessions[1:] if a.text]

        uniprot_name = _text(entry, "up:name", ns)
        gene_symbol = _text(
            entry, "up:gene/up:name[@type='primary']", ns
        )
        full_name = _text(
            entry,
            "up:protein/up:recommendedName/up:fullName",
            ns,
        )

        function = self._comment_text(entry, "function", ns)
        location = self._subcellular_locations(entry, ns)
        tissue_expression = self._comment_text(
            entry, "tissue specificity", ns
        )

        isoforms = self._isoform_ids(entry, ns)
        pfam_ids = self._db_ids(entry, "Pfam", ns)
        hgnc_id = self._db_id(entry, "HGNC", ns)
        go_terms = self._db_ids(entry, "GO", ns)
        kegg_id = self._db_id(entry, "KEGG", ns)
        refseq_ids = self._db_ids(entry, "RefSeq", ns)

        return {
            "uniprot_id":        uniprot_id,
            "uniprot_name":      uniprot_name,
            "gene_symbol":       gene_symbol,
            "full_name":         full_name,
            "function":          function,
            "location":          location,
            "tissue_expression": tissue_expression,
            "secondary_ids":     secondary_ids or None,
            "isoforms":          isoforms or None,
            "pfam_ids":          pfam_ids or None,
            "hgnc_id":           hgnc_id or None,
            "go_terms":          go_terms or None,
            "kegg_id":           kegg_id or None,
            "refseq_ids":        refseq_ids or None,
        }

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

        try:
            type_id = self.get_entity_type_id("Proteins")
        except ValueError as e:
            return False, str(e), ETLStepStats(errors=1)

        from igem_backend.modules.db.models.model_proteins import (
            ProteinEntity,
            ProteinMaster,
            ProteinPfam,
            ProteinPfamLink,
        )

        # Pre-load Pfam accession cache
        pfam_cache: dict[str, int] = {
            r.pfam_acc: r.id
            for r in self.session.query(ProteinPfam).all()
        }
        if not pfam_cache:
            self.logger.log(
                "ProteinPfam table is empty — run protein_pfam DTP first "
                "to populate Pfam domain links",
                "WARNING",
            )

        total = created = updated = warnings = 0
        isoforms_created = 0
        pfam_links_created = 0
        BATCH = 100

        for i, (_, row) in enumerate(df.iterrows()):
            total += 1
            uniprot_id = str(row.get("uniprot_id") or "").strip()
            if not uniprot_id:
                warnings += 1
                continue

            # Deserialize JSON-encoded list columns
            secondary_ids = self._decode_list(row.get("secondary_ids"))
            isoform_list = self._decode_list(row.get("isoforms"))
            pfam_id_list = self._decode_list(row.get("pfam_ids"))

            gene_symbol = str(row.get("gene_symbol") or "").strip() or None
            uniprot_name = str(row.get("uniprot_name") or "").strip() or None
            full_name = str(row.get("full_name") or "").strip() or None

            # --- Build alias list for secondary aliases ---
            secondary_aliases: list[dict] = []
            for val, atype, xref in [
                (uniprot_name, "preferred", "UNIPROT"),
                (gene_symbol,  "synonym",   "UNIPROT"),
                (full_name,    "synonym",   "UNIPROT"),
            ]:
                if val:
                    secondary_aliases.append({
                        "alias_value": val,
                        "alias_type":  atype,
                        "xref_source": xref,
                        "alias_norm":  self._normalize(val),
                        "locale":      "en",
                    })
            for sec_id in secondary_ids:
                secondary_aliases.append({
                    "alias_value": sec_id,
                    "alias_type":  "code",
                    "xref_source": "UNIPROT",
                    "alias_norm":  self._normalize(sec_id),
                    "locale":      "en",
                })

            # --- Entity + primary alias ---
            entity_id, is_new = self.get_or_create_entity(
                name=uniprot_id,
                type_id=type_id,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                alias_type="code",
                xref_source="UNIPROT",
                alias_norm=self._normalize(uniprot_id),
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

            if secondary_aliases:
                self.add_aliases(
                    entity_id=entity_id,
                    type_id=type_id,
                    aliases=secondary_aliases,
                    is_active=True,
                    data_source_id=self.data_source.id,
                    package_id=self.package.id,
                    auto_commit=False,
                )

            # --- ProteinMaster (upsert) ---
            pm = (
                self.session.query(ProteinMaster)
                .filter_by(protein_id=uniprot_id)
                .one_or_none()
            )
            function_val = self.safe_truncate(
                row.get("function") or None, 512, "function"
            )
            location_val = self.guard_short(row.get("location") or None)
            tissue_val = self.guard_short(row.get("tissue_expression") or None)

            if pm is None:
                pm = ProteinMaster(
                    protein_id=uniprot_id,
                    function=function_val,
                    location=location_val,
                    tissue_expression=tissue_val,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(pm)
                self.session.flush()
            else:
                pm.function = function_val
                pm.location = location_val
                pm.tissue_expression = tissue_val
                self.session.flush()

            # --- ProteinEntity (canonical) ---
            pe = (
                self.session.query(ProteinEntity)
                .filter_by(entity_id=entity_id, protein_id=pm.id)
                .one_or_none()
            )
            if pe is None:
                self.session.add(ProteinEntity(
                    entity_id=entity_id,
                    protein_id=pm.id,
                    is_isoform=False,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                ))

            # --- Isoforms ---
            for iso_acc in isoform_list:
                iso_acc = str(iso_acc).strip()
                if not iso_acc:
                    continue
                iso_entity_id, _ = self.get_or_create_entity(
                    name=iso_acc,
                    type_id=type_id,
                    data_source_id=self.data_source.id,
                    package_id=self.package.id,
                    alias_type="code",
                    xref_source="UNIPROT",
                    alias_norm=self._normalize(iso_acc),
                    is_active=True,
                    auto_commit=False,
                )
                if iso_entity_id is None:
                    continue
                iso_pe = (
                    self.session.query(ProteinEntity)
                    .filter_by(entity_id=iso_entity_id, protein_id=pm.id)
                    .one_or_none()
                )
                if iso_pe is None:
                    self.session.add(ProteinEntity(
                        entity_id=iso_entity_id,
                        protein_id=pm.id,
                        is_isoform=True,
                        isoform_accession=iso_acc,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    ))
                    isoforms_created += 1

            # --- ProteinPfamLink ---
            for pfam_acc in pfam_id_list:
                pfam_acc = str(pfam_acc).strip()
                pfam_id_val = pfam_cache.get(pfam_acc)
                if pfam_id_val is None:
                    continue
                exists = (
                    self.session.query(ProteinPfamLink)
                    .filter_by(protein_id=pm.id, pfam_pk_id=pfam_id_val)
                    .one_or_none()
                )
                if exists is None:
                    self.session.add(ProteinPfamLink(
                        protein_id=pm.id,
                        pfam_pk_id=pfam_id_val,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    ))
                    pfam_links_created += 1

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
            extras={
                "isoforms_created":   isoforms_created,
                "pfam_links_created": pfam_links_created,
            },
        )
        msg = (
            f"[{self.DTP_NAME}] Load complete: "
            f"total={total} created={created} updated={updated} "
            f"warnings={warnings} isoforms={isoforms_created} "
            f"pfam_links={pfam_links_created}"
        )
        self.logger.log(msg, "INFO")
        return True, msg, stats

    # -------------------------------------------------------------------------
    # XML helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _comment_text(entry, type_: str, ns: dict) -> Optional[str]:
        comment = entry.find(f"up:comment[@type='{type_}']", ns)
        if comment is None:
            return None
        text = comment.find("up:text", ns)
        return text.text.strip() if text is not None and text.text else None

    @staticmethod
    def _subcellular_locations(entry, ns: dict) -> Optional[str]:
        locs = entry.findall(
            "up:comment[@type='subcellular location']"
            "/up:subcellularLocation/up:location",
            ns,
        )
        parts = [loc.text.strip() for loc in locs if loc is not None and loc.text]
        return "; ".join(parts) if parts else None

    @staticmethod
    def _isoform_ids(entry, ns: dict) -> list[str]:
        alt = entry.find("up:comment[@type='alternative products']", ns)
        if alt is None:
            return []
        ids: list[str] = []
        for iso in alt.findall("up:isoform", ns):
            iso_id = iso.find("up:id", ns)
            if iso_id is not None and iso_id.text:
                ids.append(iso_id.text.strip())
        return ids

    @staticmethod
    def _db_ids(entry, db_type: str, ns: dict) -> list[str]:
        return [
            ref.attrib["id"]
            for ref in entry.findall(
                f"up:dbReference[@type='{db_type}']", ns
            )
            if "id" in ref.attrib
        ]

    @staticmethod
    def _db_id(entry, db_type: str, ns: dict) -> Optional[str]:
        ref = entry.find(f"up:dbReference[@type='{db_type}']", ns)
        return ref.attrib.get("id") if ref is not None else None

    @staticmethod
    def _as_list(value) -> list[str]:
        """Decode a JSON-encoded list column from parquet back to a list."""
        if not value:
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v]
        s = str(value).strip()
        if not s or s in ("nan", "None", "null"):
            return []
        try:
            decoded = json.loads(s)
            return [str(v) for v in decoded if v]
        except (json.JSONDecodeError, TypeError):
            return [s]

    @staticmethod
    def _decode_list(value) -> list[str]:
        """Decode a JSON-encoded list or return empty list for None/NaN."""
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v]
        try:
            import math
            if isinstance(value, float) and math.isnan(value):
                return []
        except Exception:
            pass
        s = str(value).strip()
        if not s or s in ("nan", "None", "null"):
            return []
        try:
            decoded = json.loads(s)
            return [str(v) for v in decoded if v]
        except (json.JSONDecodeError, TypeError):
            return [s] if s else []
