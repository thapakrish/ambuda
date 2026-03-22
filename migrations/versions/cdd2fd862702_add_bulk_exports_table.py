"""add bulk_exports table

Revision ID: cdd2fd862702
Revises: d1e2f3a4b5c6
Create Date: 2026-03-14 13:03:19.578985

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "cdd2fd862702"
down_revision = "d1e2f3a4b5c6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bulk_exports",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("slug", sa.String(), nullable=False),
        sa.Column("export_type", sa.String(), nullable=False),
        sa.Column("s3_path", sa.String(), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("sha256_checksum", sa.String(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )


def downgrade() -> None:
    op.drop_table("bulk_exports")
