"""Background tasks for structuring proofing pages with LLMs."""

from celery import group
from celery.result import GroupResult

from ambuda import consts
from ambuda import database as db
from ambuda.enums import SitePageStatus
from ambuda.tasks import app
from ambuda.tasks.utils import get_db_session
from ambuda.utils import llm_structuring
from ambuda.utils.revisions import add_revision


def _run_structuring_for_page_inner(
    app_env: str,
    project_slug: str,
    page_slug: str,
    prompt_template: str = llm_structuring.DEFAULT_STRUCTURING_PROMPT,
) -> int:
    with get_db_session(app_env) as (session, query, config_obj):
        bot_user = query.user(consts.BOT_USERNAME)
        if not bot_user:
            raise ValueError(f'User "{consts.BOT_USERNAME}" is not defined.')

        project = query.project(project_slug)
        page = query.page(project.id, page_slug)

        latest_revision = (
            session.query(db.Revision)
            .filter(db.Revision.page_id == page.id)
            .order_by(db.Revision.created_at.desc())
            .first()
        )

        api_key = config_obj.GEMINI_API_KEY
        if not api_key:
            raise ValueError("GEMINI_API_KEY not configured")

        if not latest_revision or not latest_revision.content:
            raise ValueError(f"No content found for page {project_slug}/{page_slug}")
        current_content = latest_revision.content

        structured_content = llm_structuring.run(
            current_content, api_key, prompt_template
        )

        summary = "Apply LLM structuring"
        try:
            return add_revision(
                page=page,
                summary=summary,
                content=structured_content,
                version=page.version,
                author_id=bot_user.id,
                # keep the same status as before
                status_id=page.status_id,
            )
        except Exception as e:
            raise ValueError(
                f'Structuring failed for page "{project.slug}/{page.slug}".'
            ) from e


@app.task(bind=True)
def run_structuring_for_page(
    self,
    *,
    app_env: str,
    project_slug: str,
    page_slug: str,
    prompt_template: str = llm_structuring.DEFAULT_STRUCTURING_PROMPT,
):
    _run_structuring_for_page_inner(
        app_env,
        project_slug,
        page_slug,
        prompt_template,
    )


def run_structuring_for_project(
    app_env: str,
    project: db.Project,
    prompt_template: str = llm_structuring.DEFAULT_STRUCTURING_PROMPT,
) -> GroupResult | None:
    # version == 0 means the page is brand new (= zero content).
    edited_pages = [p for p in project.pages if p.version > 0]

    if edited_pages:
        tasks = group(
            run_structuring_for_page.s(
                app_env=app_env,
                project_slug=project.slug,
                page_slug=p.slug,
                prompt_template=prompt_template,
            )
            for p in edited_pages
        )
        ret = tasks.apply_async()
        ret.save()
        return ret
    else:
        return None
