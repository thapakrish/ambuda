"""Views for basic site pages."""

from pathlib import Path

from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    render_template,
    redirect,
    request,
    send_file,
    url_for,
)
from flask_wtf import FlaskForm
from flask_login import current_user, login_required
from slugify import slugify
from sqlalchemy import update
from wtforms import StringField, HiddenField
from wtforms.validators import DataRequired
from wtforms.widgets import TextArea
from werkzeug.utils import secure_filename

import ambuda.queries as q
from ambuda import database as db
from ambuda.utils import google_ocr
from ambuda.tasks import pdf
from ambuda.views.site import bp as site
from ambuda.views.api import bp as api


bp = Blueprint("proofing", __name__)


class EditException(Exception):
    pass


class PageForm(FlaskForm):
    message = StringField("Summary of changes made:")
    version = HiddenField("Page version")
    content = StringField("Content", widget=TextArea(), validators=[DataRequired()])


def _is_allowed_document_file(filename: str) -> bool:
    return Path(filename).suffix == ".pdf"


def _is_allowed_image_file(filename: str) -> bool:
    return Path(filename).suffix == ".jpg"


def _get_image_filesystem_path(project_slug: str, page_slug: str) -> Path:
    """Get the location of the given image on disk."""
    image_dir = Path(current_app.config["UPLOAD_FOLDER"]) / project_slug
    return image_dir / (page_slug + ".jpg")


def _prev_cur_next(pages: list[db.Page], slug: str):
    """Get the previous, current, and next pages.

    :param pages: all of the pages in this project.
    :param slug: the slug for the current page.
    """
    found = False
    i = 0
    for i, s in enumerate(pages):
        if s.slug == slug:
            found = True
            break

    if not found:
        raise ValueError(f"Unknown slug {slug}")

    prev = pages[i - 1] if i > 0 else None
    cur = pages[i]
    next = pages[i + 1] if i < len(pages) - 1 else None
    return prev, cur, next


def add_revision(
    page: db.Page, message: str, content: str, version: int, author_id: int
) -> int:
    # If this doesn't update any rows, there's an edit conflict.
    # Details: https://gist.github.com/shreevatsa/237bd6592771caadecc68c9515403bc3
    # FIXME: rather than do this on the application side, do an `exists` query
    # FIXME: instead? Not sure if this is a clear win, but worth thinking about.
    session = q.get_session()
    new_version = version + 1
    result = session.execute(
        update(db.Page)
        .where((db.Page.id == page.id) & (db.Page.version == version))
        .values(version=new_version)
    )
    session.commit()

    num_rows_changed = result.rowcount
    if num_rows_changed == 0:
        raise EditException("Edit conflict")

    # Must be 1 since there's exactly one page with the given page ID.
    # If this fails, the application data is in a weird state.
    assert num_rows_changed == 1

    revision = db.Revision(
        project_id=page.project_id,
        page_id=page.id,
        content=content,
        author_id=author_id,
    )
    session.add(revision)
    session.commit()
    return new_version


@bp.route("/")
def index():
    """List all available proofreading projects."""
    projects = q.projects()
    return render_template("proofing/index.html", projects=projects)


@bp.route("/upload")
@login_required
def upload():
    return render_template("proofing/upload-images.html")


@bp.route("/upload", methods=["POST"])
@login_required
def upload_post_image_only():
    if "file" not in request.files:
        # Request has no file data
        flash("Sorry, there's a server error.")
        return redirect(request.url)

    title = request.form.get("title", None)
    if not title:
        # Missing title.
        flash("Please submit a title.")
        return redirect(request.url)

    # Check that we have a valid slug.
    # `secure_filename` might be redundant given what `slugify` already does,
    # but let's call it anyway so that we're not coupled to the internals of
    # `slugify` here.
    slug = slugify(title)
    slug = secure_filename(slug)
    if not slug:
        # Slug is empty -- bad title.
        flash("Please submit a valid title.")
        return redirect(request.url)

    q.create_project(title=title, slug=slug)
    # FIXME: Need to fetch again, otherwise DetachedInstanceError?
    # https://sqlalche.me/e/14/bhk3
    _project = q.project(slug)

    image_dir = _get_image_filesystem_path(_project.slug, "1").parent
    image_dir.mkdir(exist_ok=True, parents=True)

    session = q.get_session()
    for i, file in enumerate(request.files.getlist("file")):
        if file.filename == "":
            # Empty file submitted.
            flash("Please submit valid files.")
            session.rollback()
            return redirect(request.url)

        if file and _is_allowed_image_file(file.filename):
            n = i + 1
            image_path = _get_image_filesystem_path(_project.slug, str(n))
            file.save(image_path)

            session.add(
                db.Page(
                    project_id=_project.id,
                    slug=str(n),
                    order=n,
                )
            )

        else:
            flash("Please submit .jpg files only.")
            session.rollback()
            return redirect(request.url)

    session.commit()
    return redirect(url_for("proofing.index"))


# Unused in prod -- needs task queue support (celery/dramatiq)
@login_required
def upload_post_pdf_only():
    if "file" not in request.files:
        # Request has no file data
        flash("Sorry, there's a server error.")
        return redirect(request.url)

    file = request.files["file"]
    if file.filename == "":
        # Empty file submitted.
        flash("Please submit a file.")
        return redirect(request.url)

    title = request.form.get("title", None)
    if not title:
        # Missing title.
        flash("Please submit a title.")
        return redirect(request.url)

    # Check that we have a valid slug.
    # `secure_filename` might be redundant given what `slugify` already does,
    # but let's call it anyway so that we're not coupled to the internals of
    # `slugify` here.
    slug = slugify(title)
    slug = secure_filename(slug)
    if not slug:
        # Slug is empty -- bad title.
        flash("Please submit a valid title.")
        return redirect(request.url)

    if file and _is_allowed_image_file(file.filename):
        pdf_path = Path(current_app.config["UPLOAD_FOLDER"]) / slug / "original.pdf"
        pdf_path.parent.mkdir(exist_ok=True, parents=True)
        file.save(pdf_path)

        q.create_project(title=title, slug=slug)
        # FIXME: Need to fetch again, otherwise DetachedInstanceError?
        # https://sqlalche.me/e/14/bhk3
        _project = q.project(slug)

        pdf.create_pages.send(_project.id, pdf_path)
        return redirect(url_for("proofing.index"))

    flash("Please submit a PDF file.")
    return redirect(request.url)


@bp.route("/<slug>/")
def project(slug):
    _project = q.project(slug)
    return render_template("proofing/project.html", project=_project)


@bp.route("/<project_slug>/<page_slug>")
def page(project_slug, page_slug):
    _project = q.project(project_slug)
    if not _project:
        abort(404)
    try:
        prev, cur, next = _prev_cur_next(_project.pages, page_slug)
    except ValueError:
        abort(404)

    form = PageForm()
    form.version.data = cur.version
    if cur.revisions:
        latest_revision = cur.revisions[0]
        form.content.data = latest_revision.content

    return render_template(
        "proofing/page-edit.html",
        form=form,
        project=_project,
        prev=prev,
        cur=cur,
        next=next,
    )


@bp.route("/<project_slug>/<page_slug>", methods=["POST"])
@login_required
def page_post(project_slug, page_slug):
    assert current_user.is_authenticated

    _project = q.project(project_slug)
    if not _project:
        abort(404)
    try:
        prev, cur, next = _prev_cur_next(_project.pages, page_slug)
    except ValueError:
        abort(404)

    form = PageForm()
    conflict = None

    if form.validate_on_submit():
        try:
            new_version = add_revision(
                cur,
                message=form.message.data,
                content=form.content.data,
                version=int(form.version.data),
                author_id=current_user.id,
            )
            form.version.data = new_version
            flash("Saved changes.")
        except EditException:
            # FIXME: in the future, use a proper edit conflict view.
            flash("Edit conflict. Please incorporate the changes below:")
            conflict = cur.revisions[0]
            form.version.data = cur.version

    return render_template(
        "proofing/page-edit.html",
        form=form,
        project=_project,
        prev=prev,
        cur=cur,
        next=next,
        conflict=conflict,
    )


@site.route("/static/uploads/<project_slug>/<page_slug>.jpg")
def page_image(project_slug, page_slug):
    # In production, serve this directly via nginx.
    assert current_app.debug
    image_path = _get_image_filesystem_path(project_slug, page_slug)
    return send_file(image_path)


@bp.route("/<project_slug>/<page_slug>/history")
def page_history(project_slug, page_slug):
    _project = q.project(project_slug)
    if not _project:
        abort(404)
    try:
        prev, cur, next = _prev_cur_next(_project.pages, page_slug)
    except ValueError:
        abort(404)

    return render_template("proofing/page-history.html", project=_project, cur=cur)


@bp.route("/<project_slug>/<page_slug>/revision/<revision_id>")
def revision(project_slug, page_slug, revision_id):
    """View a specific revision for some page."""
    _project = q.project(project_slug)
    if not _project:
        abort(404)

    _page = q.page(_project.id, page_slug)
    if not _page:
        abort(404)

    cur = None
    for r in _page.revisions:
        if r.id == int(revision_id):
            cur = r
            break

    if not cur:
        abort(404)

    return render_template(
        "proofing/revision.html", project=_project, page=_page, revision=cur
    )


@api.route("/ocr/<project_slug>/<page_slug>")
@login_required
def ocr(project_slug, page_slug):
    """Apply Google OCR to the given page."""
    _project = q.project(project_slug)
    if _project is None:
        abort(404)

    _page = q.page(_project.id, page_slug)
    if not _page:
        abort(404)

    image_path = _get_image_filesystem_path(project_slug, page_slug)
    result = google_ocr.full_text_annotation(image_path)
    return result