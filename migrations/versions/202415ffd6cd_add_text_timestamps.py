"""add_text_timestamps

Revision ID: 202415ffd6cd
Revises: ef5bf0538d73
Create Date: 2025-12-06 22:54:55.404001

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "202415ffd6cd"
down_revision = "ef5bf0538d73"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("texts", sa.Column("created_at", sa.DateTime(), nullable=True))
    op.add_column("texts", sa.Column("published_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("texts", "published_at")
    op.drop_column("texts", "created_at")
