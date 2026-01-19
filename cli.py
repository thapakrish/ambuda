#!/usr/bin/env python3

import getpass
import os
from pathlib import Path

import click
from dotenv import load_dotenv
from slugify import slugify
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

import ambuda
from ambuda import database as db
from ambuda import queries as q
from ambuda.seed.utils.data_utils import create_db
from ambuda.tasks.projects import (
    create_project_from_local_pdf_inner,
)
from ambuda.tasks.text_exports import create_text_export_inner
from ambuda.utils import text_exports
from ambuda.utils.text_exports import ExportType
from ambuda.tasks.utils import LocalTaskStatus
from ambuda.s3_utils import S3Path

# Load environment variables from .env file
load_dotenv()

engine = create_db()


@click.group()
def cli():
    pass


@cli.command()
def create_user():
    """Create a new user.

    This command is best used in development to quickly create new users.
    """
    username = input("Username: ")
    raw_password = getpass.getpass("Password: ")
    email = input("Email: ")

    with Session(engine) as session:
        stmt = select(db.User).where(
            or_(db.User.username == username, db.User.email == email)
        )
        u = session.scalars(stmt).first()
        if u is not None:
            if u.username == username:
                raise click.ClickException(f'User "{username}" already exists.')
            else:
                raise click.ClickException(f'Email "{email}" already exists.')

        user = db.User(username=username, email=email)
        user.set_password(raw_password)
        session.add(user)
        session.commit()


@cli.command()
@click.option("--username", help="the user to modify")
@click.option("--role", help="the role to add")
def add_role(username, role):
    """Add the given role to the given user.

    In particular, `add-role <user> admin` will give a user administrator
    privileges and grant them full access to Ambuda's data and content.
    """
    with Session(engine) as session:
        stmt = select(db.User).where(db.User.username == username)
        u = session.scalars(stmt).first()
        if u is None:
            raise click.ClickException(f'User "{username}" does not exist.')
        stmt = select(db.Role).where(db.Role.name == role)
        r = session.scalars(stmt).first()
        if r is None:
            raise click.ClickException(f'Role "{role}" does not exist.')
        if r in u.roles:
            raise click.ClickException(f'User "{username}" already has role "{role}".')

        u.roles.append(r)
        session.add(u)
        session.commit()
    print(f'Added role "{role}" to user "{username}".')


@cli.command()
@click.option("--title", help="title of the new project")
@click.option("--pdf-path", help="path to the source PDF")
def create_project(title, pdf_path):
    """Create a proofing project from a PDF."""
    current_app = ambuda.create_app("development")
    with current_app.app_context():
        session = q.get_session()
        stmt = select(db.User)
        arbitrary_user = session.scalars(stmt).first()
        if not arbitrary_user:
            raise click.ClickException(
                "Every project must have a user that created it. "
                "But, no users were found in the database.\n"
                "Please create a user first with `create-user`."
            )

        create_project_from_local_pdf_inner(
            pdf_path=pdf_path,
            display_title=title,
            app_environment=current_app.config["AMBUDA_ENVIRONMENT"],
            creator_id=arbitrary_user.id,
            task_status=LocalTaskStatus(),
        )


@cli.command()
@click.option("--text-slug", help="slug of the text to export")
def export_text(text_slug):
    """Create all exports for a text."""
    with Session(engine) as session:
        stmt = select(db.Text).where(db.Text.slug == text_slug)
        text = session.scalars(stmt).first()
        if text is None:
            raise click.ClickException(f'Text with slug "{text_slug}" does not exist.')

        text_id = text.id

    app_environment = os.getenv("FLASK_ENV")
    if not app_environment:
        raise click.ClickException("FLASK_ENV not found in .env file")

    click.echo(
        f'Creating all exports for text "{text_slug}" (id={text_id}) in {app_environment} environment...'
    )

    xml_exports = [e for e in text_exports.EXPORTS if e.type == ExportType.XML]
    other_exports = [e for e in text_exports.EXPORTS if e.type != ExportType.XML]

    for export_config in xml_exports:
        click.echo(f"Creating {export_config.label} export...")
        create_text_export_inner(
            text_id, export_config.slug_pattern, app_environment, engine=engine
        )

    for export_config in other_exports:
        click.echo(f"Creating {export_config.label} export...")
        create_text_export_inner(
            text_id, export_config.slug_pattern, app_environment, engine=engine
        )

    click.echo("All exports completed successfully.")


if __name__ == "__main__":
    cli()
