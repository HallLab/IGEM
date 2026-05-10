"""baseline_0_1_0

Revision ID: 7a8b9c0d1e2f
Revises:
Create Date: 2026-05-10 00:00:00.000000

This is the **baseline** migration for IGEM-Server 0.1.0.

It is intentionally a no-op (`pass` in upgrade/downgrade). Its purpose
is to give existing databases a single, well-known revision they can
be **stamped** to with `igem-server db stamp-head`.

How to use:

  - Brand-new database
      `igem-server db create` — creates schema via
      `Base.metadata.create_all(...)` AND stamps to this revision.
      The DB starts versioned.

  - Existing database (production with omics already loaded)
      `igem-server db stamp-head` — inserts a single row into
      `alembic_version` recording this revision. **No DDL runs.**

After the baseline, all schema changes go through new Alembic
migrations chained from `7a8b9c0d1e2f`.

See `docs/caderno/2026-05-10__001_*` for the full plan.
"""
from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "7a8b9c0d1e2f"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema — baseline is a no-op."""
    pass


def downgrade() -> None:
    """Downgrade schema — baseline cannot be downgraded."""
    pass
