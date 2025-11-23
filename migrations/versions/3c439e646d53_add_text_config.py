"""Add Text.meta

Revision ID: 3c439e646d53
Revises: ec1a2265066c
Create Date: 2025-11-23 12:24:30.133622

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "3c439e646d53"
down_revision = "ec1a2265066c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("texts", sa.Column("config", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("texts", "config")
