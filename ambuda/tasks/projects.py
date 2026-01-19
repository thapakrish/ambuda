"""Background tasks for proofing projects."""

import logging
import uuid
import os
import json
import re
import hashlib
import tempfile
from datetime import datetime
from pathlib import Path

import requests

# NOTE: `fitz` is the internal package name for PyMuPDF. PyPI hosts another
# package called `fitz` (https://pypi.org/project/fitz/) that is completely
# unrelated to PDF parsing.
import fitz
from slugify import slugify
from sqlalchemy import select

from ambuda import database as db
from ambuda.s3_utils import S3Path
from ambuda.tasks import app
from ambuda.tasks.utils import CeleryTaskStatus, TaskStatus, get_db_session


def _split_pdf_into_pages(
    pdf_path: Path, output_dir: Path, task_status: TaskStatus
) -> list[str]:
    """Split the given PDF into N .jpg images, one image per page.

    :param pdf_path: filesystem path to the PDF we should process.
    :param output_dir: the directory to which we'll write these images.
    :return: a list of UUIDs for each page, in order.
    """
    doc = fitz.open(pdf_path)
    task_status.progress(0, doc.page_count)
    page_uuids = []

    for page in doc:
        n = page.number + 1
        # Generate a unique UUID for this page
        page_uuid = str(uuid.uuid4())
        page_uuids.append(page_uuid)

        pix = page.get_pixmap(dpi=200)
        output_path = output_dir / f"{page_uuid}.jpg"
        pix.pil_save(output_path, optimize=True)
        task_status.progress(n, doc.page_count)

    return page_uuids


def _add_project_to_database(
    session, display_title: str, slug: str, page_uuids: list[str], creator_id: int
):
    """Create a project on the database.

    :param session: database session
    :param display_title: the project title
    :param slug: the project slug
    :param page_uuids: list of UUIDs for each page, in order
    :param creator_id: the user ID of the creator
    """

    logging.info(f"Creating project (slug = {slug}) ...")
    board = db.Board(title=f"{slug} discussion board")
    session.add(board)
    session.flush()

    project = db.Project(slug=slug, display_title=display_title, creator_id=creator_id)
    project.board_id = board.id
    session.add(project)
    session.flush()

    logging.info(f"Fetching project and status (slug = {slug}) ...")
    stmt = select(db.PageStatus).filter_by(name="reviewed-0")
    unreviewed = session.scalars(stmt).one()

    num_pages = len(page_uuids)
    logging.info(f"Creating {num_pages} Page entries (slug = {slug}) ...")
    for n, page_uuid in enumerate(page_uuids, start=1):
        session.add(
            db.Page(
                project_id=project.id,
                slug=str(n),
                uuid=page_uuid,
                order=n,
                status_id=unreviewed.id,
            )
        )
    session.commit()


def create_project_from_local_pdf_inner(
    *,
    pdf_path: str,
    display_title: str | None = None,
    app_environment: str,
    creator_id: int,
    task_status: TaskStatus,
    engine=None,
):
    """Split a local PDF into pages and register the project on the database.

    We separate this function from `create_project_from_local_pdf` so that we can run this
    function in a non-Celery context (for example, in `cli.py`).

    :param pdf_path: local path to the source PDF.
    :param display_title: optional custom display title for the project.
    :param app_environment: the app environment, e.g. `"development"`.
    :param creator_id: the user that created this project.
    :param task_status: tracks progress on the task.
    :param engine: optional SQLAlchemy engine. Tests should pass this to share
                   the same :memory: database.
    """

    if not display_title:
        with open(pdf_path, "rb") as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        hash_prefix = file_hash[:12]
        display_title = f"Project {hash_prefix} ({timestamp})"

    with get_db_session(app_environment, engine=engine) as (session, query, config_obj):
        slug = slugify(display_title)
        stmt = select(db.Project).filter_by(slug=slug)
        project = session.scalars(stmt).first()

        if project:
            raise ValueError(
                f'Project "{display_title}" already exists. Please choose a different title.'
            )

        upload_folder = getattr(config_obj, "UPLOAD_FOLDER", None)
        if upload_folder:
            pages_dir = Path(upload_folder) / "pages"
            pages_dir.mkdir(parents=True, exist_ok=True)
        else:
            pages_dir = Path(tempfile.mkdtemp(prefix=f"ambuda_pages_{slug}_"))

        page_uuids = _split_pdf_into_pages(Path(pdf_path), pages_dir, task_status)

        _add_project_to_database(
            session=session,
            display_title=display_title,
            slug=slug,
            page_uuids=page_uuids,
            creator_id=creator_id,
        )

        # Move assets to s3.
        project = session.scalars(stmt).first()
        if not project:
            raise ValueError(f"Could not create project {display_title}")

        s3_bucket = config_obj.S3_BUCKET
        if s3_bucket:
            s3_dest = project.s3_path(s3_bucket)
            if s3_dest.exists():
                logging.info(f"S3 path {s3_dest} already exists.")
            else:
                s3_dest.upload_file(pdf_path)
                logging.info(f"Uploaded {project.id} PDF path to {s3_dest}.")

            Path(pdf_path).unlink()
            logging.info(f"Removed local file {pdf_path}.")

            pages_dir = Path(pages_dir)
            logging.info(f"Uploading {len(page_uuids)} page images to S3...")

            stmt = select(db.Page).filter_by(project_id=project.id)
            pages = session.scalars(stmt).all()
            page_map = {page.uuid: page for page in pages}

            for page_uuid in page_uuids:
                local_page_path = pages_dir / f"{page_uuid}.jpg"
                if not local_page_path.exists():
                    logging.warning(f"Page image not found: {local_page_path}")
                    continue

                page = page_map.get(page_uuid)
                if not page:
                    logging.warning(f"Page with UUID {page_uuid} not found in database")
                    continue

                s3_page_path = page.s3_path(s3_bucket)
                if s3_page_path.exists():
                    logging.info(f"Page {page_uuid} already exists in S3, skipping.")
                else:
                    s3_page_path.upload_file(str(local_page_path))
                    logging.info(f"Uploaded page {page_uuid} to {s3_page_path}.")

                local_page_path.unlink()

            logging.info(f"Finished uploading page images to S3.")
        else:
            logging.info(f"No s3 bucket found")

    task_status.success(len(page_uuids), slug)


@app.task(bind=True)
def create_project_from_local_pdf(
    self,
    *,
    pdf_path: str,
    display_title: str | None = None,
    creator_id: int,
    app_environment: str,
):
    """Split a local PDF into pages and register the project on the database.

    For argument details, see `create_project_from_local_pdf_inner`.
    """
    task_status = CeleryTaskStatus(self)
    create_project_from_local_pdf_inner(
        pdf_path=pdf_path,
        display_title=display_title,
        creator_id=creator_id,
        task_status=task_status,
        app_environment=app_environment,
    )


def create_project_from_url_inner(
    *,
    pdf_url: str,
    display_title: str | None = None,
    app_environment: str,
    creator_id: int,
    task_status: TaskStatus,
    engine=None,
):
    """Download a PDF from URL and create a project.

    :param pdf_url: URL to download PDF from.
    :param display_title: optional custom display title for the project.
    :param app_environment: the app environment, e.g. `"development"`.
    :param creator_id: the user that created this project.
    :param task_status: tracks progress on the task.
    :param engine: optional SQLAlchemy engine. Tests should pass this to share
                   the same :memory: database.
    """

    temp_dir = Path(tempfile.gettempdir())
    temp_pdf_path = temp_dir / f"ambuda_pdf_{uuid.uuid4()}.pdf"
    try:
        logging.info(f"Downloading PDF from {pdf_url}...")
        response = requests.get(pdf_url)
        response.raise_for_status()
        temp_pdf_path.write_bytes(response.content)

        create_project_from_local_pdf_inner(
            pdf_path=str(temp_pdf_path),
            display_title=display_title,
            app_environment=app_environment,
            creator_id=creator_id,
            task_status=task_status,
            engine=engine,
        )
    except Exception as e:
        if temp_pdf_path.exists():
            temp_pdf_path.unlink()
        raise ValueError(f"Failed to download PDF from URL: {e}")


@app.task(bind=True)
def create_project_from_url(
    self,
    *,
    pdf_url: str,
    display_title: str | None = None,
    creator_id: int,
    app_environment: str,
):
    """Download a PDF from URL and create a project.

    For argument details, see `create_project_from_url_inner`.
    """
    task_status = CeleryTaskStatus(self)
    create_project_from_url_inner(
        pdf_url=pdf_url,
        display_title=display_title,
        creator_id=creator_id,
        task_status=task_status,
        app_environment=app_environment,
    )


def _extract_gdrive_folder_id(folder_url: str) -> str:
    """Extract the folder ID from a Google Drive folder URL.

    Supports formats like:
    - https://drive.google.com/drive/folders/FOLDER_ID
    - https://drive.google.com/drive/folders/FOLDER_ID?usp=sharing
    """
    # Try to extract folder ID using regex
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", folder_url)
    if match:
        return match.group(1)
    raise ValueError(f"Could not extract folder ID from URL: {folder_url}")


def _list_gdrive_folder_pdfs(folder_id: str, api_key: str = None):
    """List all PDF files in a public Google Drive folder.

    :param folder_id: The Google Drive folder ID
    :param api_key: Optional Google Drive API key
    :return: List of dicts with 'id' and 'name' keys
    """
    if api_key:
        url = "https://www.googleapis.com/drive/v3/files"
        params = {
            "q": f"'{folder_id}' in parents and mimeType='application/pdf'",
            "key": api_key,
            "fields": "files(id,name)",
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("files", [])
        except Exception as e:
            logging.error(f"Failed to list files using API: {e}")
            raise ValueError(
                f"Failed to access Google Drive folder. Make sure it's publicly accessible. Error: {e}"
            )
    else:
        raise ValueError(
            "Google Drive API key not configured. Please set GOOGLE_DRIVE_API_KEY in your configuration."
        )


def _get_gdrive_file_download_url(file_id: str) -> str:
    """Get the download URL for a Google Drive file.

    :param file_id: The Google Drive file ID
    :return: Direct download URL
    """
    return f"https://drive.google.com/uc?export=download&id={file_id}"


def create_projects_from_gdrive_folder_inner(
    *,
    folder_url: str,
    upload_folder: str,
    app_environment: str,
    creator_id: int,
    task_status: TaskStatus,
    engine=None,
):
    """Create multiple projects from PDFs in a Google Drive folder.

    :param folder_url: Google Drive folder URL
    :param upload_folder: Base upload folder
    :param app_environment: The app environment
    :param creator_id: The user that created these projects
    :param task_status: Tracks progress on the task
    :param engine: Optional SQLAlchemy engine
    """
    logging.info(f"Processing Google Drive folder: {folder_url}")

    # Extract folder ID
    try:
        folder_id = _extract_gdrive_folder_id(folder_url)
        logging.info(f"Extracted folder ID: {folder_id}")
    except ValueError as e:
        raise ValueError(f"Invalid Google Drive folder URL: {e}")

    # Get API key from config
    with get_db_session(app_environment, engine=engine) as (session, query, config_obj):
        api_key = getattr(config_obj, "GOOGLE_DRIVE_API_KEY", None)

        # List all PDFs in the folder
        try:
            pdf_files = _list_gdrive_folder_pdfs(folder_id, api_key)
            logging.info(f"Found {len(pdf_files)} PDF files in folder")
        except ValueError as e:
            raise

        if not pdf_files:
            raise ValueError("No PDF files found in the Google Drive folder.")

        # Update progress
        task_status.progress(0, len(pdf_files))

        # Create a project for each PDF
        created_projects = []
        for idx, pdf_file in enumerate(pdf_files):
            file_id = pdf_file["id"]
            file_name = pdf_file["name"]

            # Use filename (without extension) as the title
            title = Path(file_name).stem
            logging.info(f"Creating project for: {title} ({file_name})")

            # Get download URL
            download_url = _get_gdrive_file_download_url(file_id)

            # Create project using the existing logic
            try:
                create_project_from_url_inner(
                    pdf_url=download_url,
                    display_title=title,
                    app_environment=app_environment,
                    creator_id=creator_id,
                    task_status=TaskStatus(),
                    engine=engine,
                )
                created_projects.append(title)
                logging.info(f"Successfully created project: {title}")
            except Exception as e:
                logging.error(f"Failed to create project for {title}: {e}")
                # Continue with other files even if one fails

            # Update progress
            task_status.progress(idx + 1, len(pdf_files))

        # Return summary
        success_msg = (
            f"Created {len(created_projects)} projects from {len(pdf_files)} PDFs"
        )
        logging.info(success_msg)
        task_status.success(len(created_projects), success_msg)


@app.task(bind=True)
def create_projects_from_gdrive_folder(
    self,
    *,
    folder_url: str,
    upload_folder: str,
    app_environment: str,
    creator_id: int,
):
    """Create multiple projects from PDFs in a Google Drive folder.

    For argument details, see `create_projects_from_gdrive_folder_inner`.
    """
    task_status = CeleryTaskStatus(self)
    create_projects_from_gdrive_folder_inner(
        folder_url=folder_url,
        upload_folder=upload_folder,
        app_environment=app_environment,
        creator_id=creator_id,
        task_status=task_status,
    )
