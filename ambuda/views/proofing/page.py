"""Routes related to project pages.

The main route here is `edit`, which defines the page editor and the edit flow.
"""

from dataclasses import dataclass

from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    render_template,
    request,
    send_file,
    url_for,
)
from flask_babel import lazy_gettext as _l
from flask_login import current_user, login_required
from flask_wtf import FlaskForm
from werkzeug.exceptions import abort
from wtforms import HiddenField, RadioField, StringField
from wtforms.validators import DataRequired, ValidationError
from wtforms.widgets import TextArea

from ambuda import database as db
from ambuda import queries as q
from ambuda.enums import SitePageStatus
from ambuda.utils import google_ocr, llm_structuring, project_utils, structuring
from ambuda.utils.assets import get_page_image_filepath
from ambuda.utils.diff import revision_diff
from ambuda.utils.revisions import EditError, add_revision
from ambuda.utils.structuring import ProofPage, validate_page_xml
from ambuda.views.api import bp as api
from ambuda.views.site import bp as site

bp = Blueprint("page", __name__)


def page_xml_validator(form, field):
    errors = validate_page_xml(field.data)
    if errors:
        messages = [error.message for error in errors]
        raise ValidationError("; ".join(messages))


@dataclass
class PageContext:
    """A page, its project, and some navigation data."""

    #: The current project.
    project: db.Project
    #: The current page.
    cur: db.Page
    #: The page before `cur`, if it exists.
    prev: db.Page | None
    #: The page after `cur`, if it exists.
    next: db.Page | None
    #: The number of pages in this project.
    num_pages: int


class EditPageForm(FlaskForm):
    #: An optional summary that describes the revision.
    summary = StringField(_l("Edit summary (optional)"))
    #: The page version. Versions are monotonically increasing: if A < B, then
    #: A is older than B.
    version = HiddenField(_l("Page version"))
    #: The page content.
    content = StringField(
        _l("Page content"),
        widget=TextArea(),
        validators=[DataRequired(), page_xml_validator],
    )
    #: The page status.
    status = RadioField(
        _l("Status"),
        choices=[
            (SitePageStatus.R0.value, _l("Needs work")),
            (SitePageStatus.R1.value, _l("Proofed once")),
            (SitePageStatus.R2.value, _l("Proofed twice")),
            (SitePageStatus.SKIP.value, _l("Not relevant")),
        ],
    )


def _get_page_context(project_slug: str, page_slug: str) -> PageContext | None:
    """Get the previous, current, and next pages for the given project.

    :param project_slug: slug for the current project
    :param page_slug: slug for a page within the current project.
    :return: a `PageContext` if the project and page can be found, else ``None``.
    """
    project_ = q.project(project_slug)
    if project_ is None:
        return None

    pages = project_.pages
    found = False
    i = 0
    for i, s in enumerate(pages):
        if s.slug == page_slug:
            found = True
            break

    if not found:
        return None

    prev = pages[i - 1] if i > 0 else None
    cur = pages[i]
    next = pages[i + 1] if i < len(pages) - 1 else None
    return PageContext(
        project=project_, cur=cur, prev=prev, next=next, num_pages=len(pages)
    )


def _get_page_number(project_: db.Project, page_: db.Page) -> str:
    """Get the page number for the given page.

    We define page numbers through a page spec. For now, just interpret the
    full page spec. In the future, we might store this in its own column.
    """
    if not project_.page_numbers:
        return page_.slug

    page_rules = project_utils.parse_page_number_spec(project_.page_numbers)
    page_titles = project_utils.apply_rules(len(project_.pages), page_rules)
    for title, cur in zip(page_titles, project_.pages):
        if cur.id == page_.id:
            return title

    # We shouldn't reach this case, but if we do, reuse the page's slug.
    return page_.slug


def _get_image_url(project: db.Project, page: db.Page) -> str:
    """Handler for getting the image URL (S3 migration in progress.)"""
    if current_app.debug:
        return url_for(
            "site.page_image", project_slug=project.slug, page_slug=page.slug
        )

    CLOUDFRONT_BASE_URL = current_app.config.get("CLOUDFRONT_BASE_URL")
    page_uuid = page.uuid
    s3_url = f"{CLOUDFRONT_BASE_URL}/pages/{page_uuid}.jpg"
    return s3_url


@bp.route("/<project_slug>/<page_slug>/")
def edit(project_slug, page_slug):
    """Display the page editor."""
    ctx = _get_page_context(project_slug, page_slug)
    if ctx is None:
        abort(404)
    assert ctx

    cur = ctx.cur
    form = EditPageForm()
    form.version.data = cur.version

    # FIXME: less hacky approach?
    status_names = {s.id: s.name for s in q.page_statuses()}
    form.status.data = status_names[cur.status_id]

    has_edits = bool(cur.revisions)
    if has_edits:
        latest_revision = cur.revisions[-1]
        form.content.data = ProofPage.from_content_and_page_id(
            latest_revision.content, ctx.cur.id
        ).to_xml_string()

    is_r0 = cur.status.name == SitePageStatus.R0
    image_number = cur.slug
    page_number = _get_page_number(ctx.project, cur)
    image_url = _get_image_url(ctx.project, cur)

    return render_template(
        "proofing/pages/edit.html",
        conflict=None,
        cur=ctx.cur,
        form=form,
        has_edits=has_edits,
        image_number=image_number,
        is_r0=is_r0,
        page_context=ctx,
        page_number=page_number,
        project=ctx.project,
        image_url=image_url,
    )


@bp.route("/<project_slug>/<page_slug>/", methods=["POST"])
@login_required
def edit_post(project_slug, page_slug):
    """Submit changes through the page editor.

    Since `edit` is public on GET and needs auth on `POST`, it's cleaner to
    separate the logic here into two views.
    """
    ctx = _get_page_context(project_slug, page_slug)
    if ctx is None:
        abort(404)
    assert ctx

    cur = ctx.cur
    form = EditPageForm()
    conflict = None

    if form.validate_on_submit():
        # `new_content` is already validated through EditPageForm.
        new_content = form.content.data

        cur_page = ctx.cur
        cur_content = cur_page.revisions[-1].content
        content_has_changed = cur_content != new_content

        status_has_changed = cur_page.status.name != form.status.data
        has_changed = content_has_changed or status_has_changed
        try:
            if has_changed:
                new_version = add_revision(
                    cur,
                    summary=form.summary.data,
                    content=form.content.data,
                    status=form.status.data,
                    version=int(form.version.data),
                    author_id=current_user.id,
                )
                form.version.data = new_version
                flash("Saved changes.", "success")
            else:
                flash("Skipped save. (No changes made.)", "success")
        except EditError:
            # FIXME: in the future, use a proper edit conflict view.
            flash("Edit conflict. Please incorporate the changes below:")
            conflict = cur.revisions[-1]
            form.version.data = cur.version
    else:
        flash("Sorry, your changes have one or more errors.", "error")

    is_r0 = cur.status.name == SitePageStatus.R0
    image_number = cur.slug
    page_number = _get_page_number(ctx.project, cur)

    # Keep args in sync with `edit`. (We can't unify these functions easily
    # because one function requires login but the other doesn't. Helper
    # functions don't have any obvious cutting points.
    return render_template(
        "proofing/pages/edit.html",
        conflict=conflict,
        cur=ctx.cur,
        form=form,
        has_edits=True,
        image_number=image_number,
        is_r0=is_r0,
        page_context=ctx,
        page_number=page_number,
        project=ctx.project,
    )


@site.route("/static/uploads/<project_slug>/pages/<page_slug>.jpg")
def page_image(project_slug, page_slug):
    """(Debug only) Serve an image from the filesystem.

    In production, we serve images directly from Cloudfront.
    """
    assert current_app.debug

    project = q.project(project_slug)
    if not project:
        return None

    page = q.page(project.id, page_slug)
    if not page:
        return None

    s3_path = page.s3_path(current_app.config["S3_BUCKET"])
    local_path = s3_path._debug_local_path()
    if not local_path:
        return None

    return send_file(local_path)


@bp.route("/<project_slug>/<page_slug>/history")
def history(project_slug, page_slug):
    """View the full revision history for the given page."""
    ctx = _get_page_context(project_slug, page_slug)
    if ctx is None:
        abort(404)

    assert ctx
    return render_template(
        "proofing/pages/history.html",
        project=ctx.project,
        cur=ctx.cur,
        prev=ctx.prev,
        next=ctx.next,
    )


@bp.route("/<project_slug>/<page_slug>/revision/<revision_id>")
def revision(project_slug, page_slug, revision_id):
    """View a specific revision for some page."""
    ctx = _get_page_context(project_slug, page_slug)
    if ctx is None:
        abort(404)

    assert ctx
    cur = ctx.cur
    prev_revision = None
    cur_revision = None
    for r in cur.revisions:
        if str(r.id) == revision_id:
            cur_revision = r
            break
        else:
            prev_revision = r

    if not cur_revision:
        abort(404)

    if prev_revision:
        diff = revision_diff(prev_revision.content, cur_revision.content)
    else:
        diff = revision_diff("", cur_revision.content)

    return render_template(
        "proofing/pages/revision.html",
        project=ctx.project,
        cur=cur,
        prev=ctx.prev,
        next=ctx.next,
        revision=cur_revision,
        diff=diff,
    )


# FIXME: added trailing slash as a quick hack to support OCR routes on
# frontend, which just concatenate the window URL onto "/api/ocr".
@api.route("/ocr/<project_slug>/<page_slug>/")
@login_required
def ocr_api(project_slug, page_slug):
    """Apply Google OCR to the given page."""
    project_ = q.project(project_slug)
    if project_ is None:
        abort(404)
    assert project_

    page_ = q.page(project_.id, page_slug)
    if not page_:
        abort(404)
    assert page_

    image_path = get_page_image_filepath(project_slug, page_slug)
    ocr_response = google_ocr.run(image_path)
    ocr_text = ocr_response.text_content

    structured_data = ProofPage.from_content_and_page_id(ocr_text, page_.id)
    ret = structured_data.to_xml_string()
    return ret


@api.route("/llm-structuring/<project_slug>/<page_slug>/")
@login_required
def llm_structuring_api(project_slug, page_slug):
    project_ = q.project(project_slug)
    if project_ is None:
        abort(404)
    assert project_

    page_ = q.page(project_.id, page_slug)
    if not page_:
        abort(404)
    assert page_

    content = request.json.get("content", "")
    if not content:
        return "Error: No content provided", 400

    try:
        api_key = current_app.config.get("GEMINI_API_KEY")
        if not api_key:
            return "Error: GEMINI_API_KEY not configured", 500

        structured_content = llm_structuring.run(content, api_key)
        return structured_content
    except Exception as e:
        current_app.logger.error(f"LLM structuring failed: {e}")
        return f"Error: {str(e)}", 500


@api.route("/proofing/<project_slug>/<page_slug>/history")
def page_history_api(project_slug, page_slug):
    ctx = _get_page_context(project_slug, page_slug)
    if ctx is None:
        abort(404)

    assert ctx
    revisions = []
    for r in reversed(ctx.cur.revisions):
        revisions.append(
            {
                "id": r.id,
                "created": r.created.strftime("%Y-%m-%d %H:%M"),
                "author": r.author.username,
                "summary": r.summary or "",
                "status": r.status.name,
                "revision_url": url_for(
                    "proofing.page.revision",
                    project_slug=project_slug,
                    page_slug=page_slug,
                    revision_id=r.id,
                    _external=True,
                ),
                "author_url": url_for(
                    "user.summary", username=r.author.username, _external=True
                ),
            }
        )

    return jsonify({"revisions": revisions})
