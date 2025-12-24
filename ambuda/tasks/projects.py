"""Background tasks for proofing projects."""

import logging
import uuid
import os
from pathlib import Path

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
) -> int:
    """Split the given PDF into N .jpg images, one image per page.

    :param pdf_path: filesystem path to the PDF we should process.
    :param output_dir: the directory to which we'll write these images.
    :return: the page count, which we use downstream.
    """
    doc = fitz.open(pdf_path)
    task_status.progress(0, doc.page_count)
    for page in doc:
        n = page.number + 1
        pix = page.get_pixmap(dpi=200)
        output_path = output_dir / f"{n}.jpg"
        pix.pil_save(output_path, optimize=True)
        task_status.progress(n, doc.page_count)
    return doc.page_count


def _add_project_to_database(
    session, display_title: str, slug: str, num_pages: int, creator_id: int
):
    """Create a project on the database.

    :param session: database session
    :param display_title: the project title
    :param slug: the project slug
    :param num_pages: the number of pages in the project
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

    logging.info(f"Creating {num_pages} Page entries (slug = {slug}) ...")
    for n in range(1, num_pages + 1):
        session.add(
            db.Page(
                project_id=project.id,
                slug=str(n),
                order=n,
                status_id=unreviewed.id,
            )
        )
    session.commit()


def create_project_inner(
    *,
    display_title: str,
    pdf_path: str,
    output_dir: str,
    app_environment: str,
    creator_id: int,
    task_status: TaskStatus,
    engine=None,
):
    """Split the given PDF into pages and register the project on the database.

    We separate this function from `create_project` so that we can run this
    function in a non-Celery context (for example, in `cli.py`).

    :param display_title: the project's title.
    :param pdf_path: local path to the source PDF.
    :param output_dir: local path where page images will be stored.
    :param app_environment: the app environment, e.g. `"development"`.
    :param creator_id: the user that created this project.
    :param task_status: tracks progress on the task.
    :param engine: optional SQLAlchemy engine. Tests should pass this to share
                   the same :memory: database.
    """
    logging.info(f'Received upload task "{display_title}" for path {pdf_path}.')

    # Tasks must be idempotent. Exit if the project already exists.
    with get_db_session(app_environment, engine=engine) as (session, query, config_obj):
        slug = slugify(display_title)
        stmt = select(db.Project).filter_by(slug=slug)
        project = session.scalars(stmt).first()

        if project:
            raise ValueError(
                f'Project "{display_title}" already exists. Please choose a different title.'
            )

        pdf_path = Path(pdf_path)
        pages_dir = Path(output_dir)

        num_pages = _split_pdf_into_pages(Path(pdf_path), Path(pages_dir), task_status)

        _add_project_to_database(
            session=session,
            display_title=display_title,
            slug=slug,
            num_pages=num_pages,
            creator_id=creator_id,
        )

        move_project_pdf_to_s3_inner(
            session=session,
            config_obj=config_obj,
            project_slug=slug,
            pdf_path=str(pdf_path),
        )

    task_status.success(num_pages, slug)


@app.task(bind=True)
def create_project(
    self,
    *,
    display_title: str,
    pdf_path: str,
    output_dir: str,
    app_environment: str,
    creator_id: int,
):
    """Split the given PDF into pages and register the project on the database.

    For argument details, see `create_project_inner`.
    """
    task_status = CeleryTaskStatus(self)
    create_project_inner(
        display_title=display_title,
        pdf_path=pdf_path,
        output_dir=output_dir,
        app_environment=app_environment,
        creator_id=creator_id,
        task_status=task_status,
    )


def move_project_pdf_to_s3_inner(*, session, config_obj, project_slug, pdf_path):
    """Temporary task to move project PDFs to S3.

    :param session: database session
    :param config_obj: config object
    :param project_slug: the project slug
    :param pdf_path: path to the PDF file
    """

    stmt = select(db.Project).filter_by(slug=project_slug)
    project = session.scalars(stmt).first()

    s3_bucket = config_obj.S3_BUCKET
    if not s3_bucket:
        logging.info(f"No s3 bucket found")
        return

    s3_dest = S3Path(bucket=s3_bucket, key=f"proofing/{project.uuid}/pdf/source.pdf")
    if s3_dest.exists():
        logging.info(f"S3 path {s3_dest} already exists.")
        return

    s3_dest.upload_file(pdf_path)
    logging.info(f"Uploaded {project.id} PDF path to {s3_dest}.")

    Path(pdf_path).unlink()
    logging.info(f"Removed local file {pdf_path}.")


@app.task(bind=True)
def move_project_pdf_to_s3(self, *, project_slug, pdf_path, app_environment):
    """Temporary task to move project PDFs to S3."""
    with get_db_session(app_environment) as (session, query, config_obj):
        move_project_pdf_to_s3_inner(
            session=session,
            config_obj=config_obj,
            project_slug=project_slug,
            pdf_path=pdf_path,
        )
