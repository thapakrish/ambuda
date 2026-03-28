"""Add site_config table

Revision ID: 86b67aeb9dce
Revises: cdd2fd862702
Create Date: 2026-03-27 19:06:36.693781

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "86b67aeb9dce"
down_revision = "cdd2fd862702"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "site_config",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("data", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("site_config")
