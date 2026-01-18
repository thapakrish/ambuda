#!/usr/bin/env python3

import asyncio
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
    create_project_inner,
    move_project_pdf_to_s3_inner,
)
from ambuda.tasks.text_exports import create_text_export_inner
from ambuda.utils import text_exports
from ambuda.utils.text_exports import ExportType
from ambuda.tasks.utils import LocalTaskStatus
from ambuda.utils.assets import get_page_image_filepath
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

        slug = slugify(title)
        page_image_dir = (
            Path(current_app.config["UPLOAD_FOLDER"]) / "projects" / slug / "pages"
        )
        page_image_dir.mkdir(parents=True, exist_ok=True)
        create_project_inner(
            title=title,
            pdf_path=pdf_path,
            output_dir=str(page_image_dir),
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


async def _upload_page_async(
    idx, page, project, s3_bucket, upload_folder, total_pages, dry_run, semaphore
):
    """Upload a single page image to S3 (async)."""
    async with semaphore:
        # Run blocking I/O operations in thread pool
        local_path = await asyncio.to_thread(
            get_page_image_filepath,
            project_slug=project.slug,
            page_slug=page.slug,
            upload_folder=upload_folder,
        )

        # Check if file exists
        exists = await asyncio.to_thread(local_path.exists)
        if not exists:
            return (
                "skipped_missing",
                f"[{idx}/{total_pages}] Skipping {project.slug}/{page.slug} - file not found locally",
            )

        # Build S3 path
        s3_key = f"assets/pages/{page.uuid}.jpg"
        s3_path = S3Path(bucket=s3_bucket, key=s3_key)

        # Check if already exists in S3
        s3_exists = await asyncio.to_thread(s3_path.exists)
        if s3_exists:
            return (
                "skipped_exists",
                f"[{idx}/{total_pages}] Skipping {project.slug}/{page.slug} - already exists in S3",
            )

        if dry_run:
            return (
                "uploaded",
                f"[{idx}/{total_pages}] Would upload {local_path} -> {s3_path}",
            )
        else:
            # Upload to S3
            try:
                await asyncio.to_thread(s3_path.upload_file, local_path)
                return (
                    "uploaded",
                    f"[{idx}/{total_pages}] Uploaded {project.slug}/{page.slug} -> {s3_path}",
                )
            except Exception as e:
                return (
                    "error",
                    f"[{idx}/{total_pages}] ERROR uploading {project.slug}/{page.slug}: {e}",
                )


async def _upload_all_pages_async(results, s3_bucket, upload_folder, dry_run, workers):
    """Process all page uploads concurrently."""
    total_pages = len(results)

    # Counters (no locks needed - asyncio is single-threaded)
    uploaded = 0
    skipped_missing = 0
    skipped_exists = 0
    errors = 0

    # Semaphore to limit concurrency
    semaphore = asyncio.Semaphore(workers)

    # Create all tasks
    tasks = [
        _upload_page_async(
            idx,
            page,
            project,
            s3_bucket,
            upload_folder,
            total_pages,
            dry_run,
            semaphore,
        )
        for idx, (page, project) in enumerate(results, 1)
    ]

    # Process tasks as they complete
    completed = 0
    for coro in asyncio.as_completed(tasks):
        status, message = await coro
        completed += 1

        # Update counters
        if status == "uploaded":
            uploaded += 1
        elif status == "skipped_missing":
            skipped_missing += 1
        elif status == "skipped_exists":
            skipped_exists += 1
        elif status == "error":
            errors += 1

        # Show progress periodically or for important messages
        if (
            completed % 100 == 0
            or completed == total_pages
            or "ERROR" in message
            or "Would upload" in message
        ):
            click.echo(message)

    return uploaded, skipped_missing, skipped_exists, errors


@cli.command()
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be uploaded without actually uploading",
)
@click.option(
    "--workers",
    type=int,
    default=10,
    help="Number of concurrent uploads (default: 10)",
)
def upload_page_images(dry_run, workers):
    """Upload page images from local disk to S3.

    Uploads each page image to s3://$S3_BUCKET/assets/pages/{page.uuid}
    Uses async I/O for efficient concurrent uploads.
    """
    app_environment = os.getenv("FLASK_ENV")
    if not app_environment:
        raise click.ClickException("FLASK_ENV not found in .env file")

    current_app = ambuda.create_app(app_environment)
    with current_app.app_context():
        s3_bucket = current_app.config.get("S3_BUCKET")
        if not s3_bucket:
            raise click.ClickException("S3_BUCKET not configured in environment")

        upload_folder = current_app.config.get("UPLOAD_FOLDER")
        if not upload_folder:
            raise click.ClickException("UPLOAD_FOLDER not configured in environment")

        with Session(engine) as session:
            # Get all pages along with their project information
            stmt = select(db.Page, db.Project).join(
                db.Project, db.Page.project_id == db.Project.id
            )
            results = session.execute(stmt).all()

            if not results:
                click.echo("No pages found in the database.")
                return

            total_pages = len(results)

            click.echo(f"Found {total_pages} pages to process.")
            if dry_run:
                click.echo("DRY RUN MODE - No files will be uploaded")
            click.echo(f"Using {workers} concurrent uploads")
            click.echo()

            # Run async upload process
            uploaded, skipped_missing, skipped_exists, errors = asyncio.run(
                _upload_all_pages_async(
                    results, s3_bucket, upload_folder, dry_run, workers
                )
            )

            # Summary
            click.echo()
            click.echo("=" * 50)
            click.echo("Summary:")
            click.echo(f"  Total pages: {total_pages}")
            if dry_run:
                click.echo(f"  Would upload: {uploaded}")
            else:
                click.echo(f"  Uploaded: {uploaded}")
            click.echo(f"  Skipped (missing locally): {skipped_missing}")
            click.echo(f"  Skipped (already in S3): {skipped_exists}")
            if errors > 0:
                click.echo(f"  Errors: {errors}")
            click.echo("=" * 50)


if __name__ == "__main__":
    cli()
