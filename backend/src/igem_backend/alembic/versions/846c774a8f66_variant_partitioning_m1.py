"""variant_partitioning_m1

Revision ID: 846c774a8f66
Revises: 7a8b9c0d1e2f
Create Date: 2026-05-10 17:13:15.422041

Replaces the declarative `variant_masters` (from baseline 0.1.0)
with a chromosome-partitioned parent + 25 child partitions, and
adds four new chromosome-partitioned tables for variant annotations
to match the BF4 schema:

- variant_molecular_effects
- variant_effect_predictions
- variant_regulatory_elements
- variant_gene_regulatory_evidence

PostgreSQL: PARTITION BY LIST (chromosome). Composite PK
(chromosome, variant_id). 25 child partitions covering chr 1-22 +
X(23) + Y(24) + MT(25). DDL is centralised in
`igem_backend.modules.db.core_ddl`.

SQLite (dev/test only): plain tables with the same column shape
but `variant_id INTEGER PRIMARY KEY`. Created by SQLAlchemy's
`create_all` after the migration drops the old shape.

Safety: PROD had not yet ingested variants data at the time this
migration was authored (2026-05-10), so the drop of the existing
`variant_masters` is lossless. If you are running this against an
environment where variants have been loaded, you MUST backup first
and migrate the data manually — `op.execute("DROP TABLE …")`
discards rows unconditionally.

Downgrade is unsupported: re-creating the old declarative
`variant_masters` would risk schema drift and would not restore
data anyway. Restore from `pg_dump` if you need to revert.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect

from igem_backend.modules.db.core_ddl import (
    CORE_PARTITIONED,
    ddl_list_partitions,
    ddl_variant_effect_predictions,
    ddl_variant_gene_regulatory_evidence,
    ddl_variant_masters,
    ddl_variant_molecular_effects,
    ddl_variant_regulatory_elements,
)

# revision identifiers, used by Alembic.
revision: str = "846c774a8f66"
down_revision: Union[str, Sequence[str], None] = "7a8b9c0d1e2f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Replace declarative variant_masters with partitioned parents + children."""
    bind = op.get_bind()
    dialect = bind.dialect.name

    if dialect == "postgresql":
        # 1. Drop the old declarative variant_masters.
        #    Assumes PROD has not yet ingested variants — see module docstring.
        op.execute("DROP TABLE IF EXISTS variant_masters CASCADE")

        # 2. Create the five partitioned parents.
        for ddl in (
            ddl_variant_masters(),
            ddl_variant_molecular_effects(),
            ddl_variant_effect_predictions(),
            ddl_variant_regulatory_elements(),
            ddl_variant_gene_regulatory_evidence(),
        ):
            op.execute(ddl)

        # 3. Create 25 child partitions for each parent (chr 1..25).
        for parent in CORE_PARTITIONED:
            for ddl in ddl_list_partitions(
                parent_table=parent,
                part_prefix=parent,
                chrom_min=1,
                chrom_max=25,
            ):
                op.execute(ddl)
        return

    # SQLite (dev/test). Drop the old shape; the new shape will be
    # (re-)created by Base.metadata.create_all on the next bootstrap
    # — including when this migration is followed by db.upgrade_db
    # or by a fresh db.create_db.
    op.execute("DROP TABLE IF EXISTS variant_masters")

    # In offline (dry-run) mode, `bind` is a MockConnection that does
    # not support introspection or table.create(). Skip the Python-side
    # re-creation in that case — the SQL emitted is enough for review.
    if op.get_context().as_sql:
        return

    from igem_backend.modules.db.base import Base
    from igem_backend.utils.db_loader import bootstrap_models

    bootstrap_models(bind.engine)

    # Force creation of the new-shape Tables now so the DB is fully
    # operational at the end of this migration.
    existing = set(inspect(bind).get_table_names())
    for tbl_name in CORE_PARTITIONED:
        tbl = Base.metadata.tables.get(tbl_name)
        if tbl is not None and tbl_name not in existing:
            tbl.create(bind, checkfirst=True)


def downgrade() -> None:
    """Downgrade is unsupported — restore from pg_dump backup if needed."""
    raise NotImplementedError(
        "Downgrade of variant_partitioning_m1 is not supported. "
        "Restore the database from a pg_dump backup taken before the "
        "upgrade. Re-creating the old declarative variant_masters would "
        "not restore data and would risk subtle schema drift."
    )
