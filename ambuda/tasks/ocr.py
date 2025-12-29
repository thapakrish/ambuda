"""Background tasks for proofing projects."""

from celery import group
from celery.result import GroupResult

from ambuda import consts
from ambuda import database as db
from ambuda.enums import SitePageStatus
from ambuda.tasks import app
from ambuda.tasks.utils import get_db_session
from ambuda.utils import google_ocr
from ambuda.utils.assets import get_page_image_filepath
from ambuda.utils.revisions import add_revision


def _run_ocr_for_page_inner(
    app_env: str,
    project_slug: str,
    page_slug: str,
) -> int:
    """Run OCR for a single page without Flask dependency."""

    with get_db_session(app_env) as (session, query, cfg):
        bot_user = query.user(consts.BOT_USERNAME)
        if bot_user is None:
            raise ValueError(f'User "{consts.BOT_USERNAME}" is not defined.')

        # The actual API call.
        image_path = get_page_image_filepath(project_slug, page_slug, cfg.UPLOAD_FOLDER)
        ocr_response = google_ocr.run(image_path)

        project = query.project(project_slug)
        page = query.page(project.id, page_slug)

        page.ocr_bounding_boxes = google_ocr.serialize_bounding_boxes(
            ocr_response.bounding_boxes
        )
        session.add(page)
        session.commit()

        summary = "Run OCR"
        try:
            return add_revision(
                page=page,
                summary=summary,
                content=ocr_response.text_content,
                version=0,
                author_id=bot_user.id,
                status=SitePageStatus.R0,
                session=session,
                query=query,
            )
        except Exception as e:
            raise ValueError(
                f'OCR failed for page "{project.slug}/{page.slug}".'
            ) from e


@app.task(bind=True)
def run_ocr_for_page(
    self,
    *,
    app_env: str,
    project_slug: str,
    page_slug: str,
):
    _run_ocr_for_page_inner(
        app_env,
        project_slug,
        page_slug,
    )


def run_ocr_for_project(
    app_env: str,
    project: db.Project,
) -> GroupResult | None:
    """Create a `group` task to run OCR on a project.

    Usage:

    >>> r = run_ocr_for_project(...)
    >>> progress = r.completed_count() / len(r.results)

    :return: the Celery result, or ``None`` if no tasks were run.
    """
    unedited_pages = [p for p in project.pages if p.version == 0]

    if unedited_pages:
        tasks = group(
            run_ocr_for_page.s(
                app_env=app_env,
                project_slug=project.slug,
                page_slug=p.slug,
            )
            for p in unedited_pages
        )
        ret = tasks.apply_async()
        # Save the result so that we can poll for it later. If we don't do
        # this, the result won't be available at all.
        ret.save()
        return ret
    else:
        return None
