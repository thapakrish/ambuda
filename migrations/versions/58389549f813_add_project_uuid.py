"""Add project.uuid

Revision ID: 58389549f813
Revises: f208e1844a36
Create Date: 2025-11-22 10:19:22.745516

"""

import uuid

import sqlalchemy as sa
from alembic import op
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base

# revision identifiers, used by Alembic.
revision = "58389549f813"
down_revision = "f208e1844a36"
branch_labels = None
depends_on = None


Base = declarative_base()


class Project(Base):
    __tablename__ = "proof_projects"
    id = sa.Column(sa.Integer, primary_key=True)
    uuid = sa.Column(sa.String)


def upgrade() -> None:
    # Create as nullable column to prevent errors while we populate.
    op.add_column("proof_projects", sa.Column("uuid", sa.String(), nullable=True))

    bind = op.get_bind()
    session = orm.Session(bind=bind)
    for project in session.query(Project).all():
        project.uuid = str(uuid.uuid4())
    session.commit()

    with op.batch_alter_table("proof_projects") as batch_op:
        batch_op.alter_column("uuid", existing_type=sa.String(), nullable=False)
        batch_op.create_unique_constraint("u_idx_proof_projects", ["uuid"])


def downgrade() -> None:
    with op.batch_alter_table("proof_projects") as batch_op:
        batch_op.drop_constraint(None, type_="unique")
    op.drop_column("proof_projects", "uuid")
