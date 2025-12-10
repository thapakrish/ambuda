"""add_project_config

Revision ID: 3f4bacd1d993
Revises: 202415ffd6cd
Create Date: 2025-12-06 23:15:28.952241

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "3f4bacd1d993"
down_revision = "202415ffd6cd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("proof_projects", sa.Column("config", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("proof_projects", "config")
