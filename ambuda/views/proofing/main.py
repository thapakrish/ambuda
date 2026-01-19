"""Views for basic site pages."""

from collections import defaultdict
from datetime import datetime, timedelta, UTC
import uuid
from pathlib import Path

from flask import Blueprint, current_app, flash, render_template
from flask_login import current_user
from flask_wtf import FlaskForm
from slugify import slugify
from sqlalchemy import func, orm, select
from wtforms import FileField, RadioField, StringField
from wtforms.validators import DataRequired, ValidationError
from wtforms.widgets import TextArea

from ambuda import consts
from ambuda import database as db
from ambuda import queries as q
from ambuda.enums import SitePageStatus
from ambuda.tasks import projects as project_tasks
from ambuda.views.proofing.decorators import moderator_required, p2_required

bp = Blueprint("proofing", __name__)


def _is_allowed_document_file(filename: str) -> bool:
    """True iff we accept this type of document upload."""
    return Path(filename).suffix == ".pdf"


def _required_if_url(message: str):
    def fn(form, field):
        source = form.pdf_source.data
        if source == "url" and not field.data:
            raise ValidationError(message)

    return fn


def _required_if_gdrive(message: str):
    def fn(form, field):
        source = form.pdf_source.data
        if source == "gdrive" and not field.data:
            raise ValidationError(message)

    return fn


def _required_if_local(message: str):
    def fn(form, field):
        source = form.pdf_source.data
        if source == "local" and not field.data:
            raise ValidationError(message)

    return fn


class CreateProjectForm(FlaskForm):
    pdf_source = RadioField(
        "Source",
        choices=[
            ("url", "Upload from a URL"),
            ("local", "Upload from my computer"),
            # TODO: support this later, maybe too powerful for the average user.
            # ("gdrive", "Upload from a Google Drive folder"),
        ],
        validators=[DataRequired()],
    )
    pdf_url = StringField(
        "PDF URL",
        validators=[_required_if_url("Please provide a valid PDF URL.")],
    )
    # gdrive_folder_url = StringField(
    # "Google Drive folder URL",
    # validators=[
    # _required_if_gdrive("Please provide a valid Google Drive folder URL.")
    # ],
    # )
    local_file = FileField(
        "PDF file", validators=[_required_if_local("Please provide a PDF file.")]
    )
    display_title = StringField("Display title (optional)")


@bp.route("/dashboard")
def dashboard():
    """Show proofing dashboard with overview statistics."""
    from ambuda.models.proofing import ProjectStatus

    session = q.get_session()

    num_active_projects = session.scalar(
        select(func.count(db.Project.id)).filter(
            db.Project.status == ProjectStatus.ACTIVE
        )
    )
    num_pending_projects = session.scalar(
        select(func.count(db.Project.id)).filter(
            db.Project.status == ProjectStatus.PENDING
        )
    )
    num_texts = session.scalar(select(func.count(db.Text.id)))

    return render_template(
        "proofing/dashboard.html",
        num_active_projects=num_active_projects,
        num_pending_projects=num_pending_projects,
        num_texts=num_texts,
    )


@bp.route("/")
def index():
    """List all available proofing projects."""
    from ambuda.models.proofing import ProjectStatus

    session = q.get_session()
    status_classes = {
        SitePageStatus.R2: "bg-green-200",
        SitePageStatus.R1: "bg-yellow-200",
        SitePageStatus.R0: "bg-red-300",
        SitePageStatus.SKIP: "bg-slate-100",
    }

    # Only load the columns we need for the template
    projects = list(
        session.scalars(
            select(db.Project)
            .filter(db.Project.status == ProjectStatus.ACTIVE)
            .options(
                orm.load_only(
                    db.Project.id,
                    db.Project.display_title,
                    db.Project.slug,
                    db.Project.created_at,
                    db.Project.description,
                )
            )
        ).all()
    )

    # Only calculate stats for active projects to avoid wasting time on inactive ones
    active_project_ids = [p.id for p in projects]
    if not active_project_ids:
        # No active projects, return early
        return render_template(
            "proofing/index.html",
            projects=[],
            statuses_per_project={},
            progress_per_project={},
            pages_per_project={},
        )

    stmt = (
        select(
            db.Page.project_id,
            db.PageStatus.name,
            func.count(db.Page.id).label("count"),
        )
        .join(db.PageStatus)
        .filter(db.Page.project_id.in_(active_project_ids))
        .group_by(db.Page.project_id, db.PageStatus.name)
    )
    stats = session.execute(stmt).all()

    status_counts_by_project = defaultdict(lambda: defaultdict(int))
    for project_id, status_name, count in stats:
        status_counts_by_project[project_id][status_name] = count

    # Build display dictionaries
    statuses_per_project = {}
    progress_per_project = {}
    pages_per_project = {}

    for project in projects:
        counts = status_counts_by_project[project.id]
        num_pages = sum(counts.values())

        if num_pages == 0:
            statuses_per_project[project.id] = {}
            pages_per_project[project.id] = 0
            continue

        project_counts = {}
        for enum_value, class_ in status_classes.items():
            count = counts.get(enum_value.value, 0)
            fraction = count / num_pages
            project_counts[class_] = fraction
            if enum_value == SitePageStatus.R0:
                # The more red pages there are, the lower progress is.
                progress_per_project[project.id] = 1 - fraction

        statuses_per_project[project.id] = project_counts
        pages_per_project[project.id] = num_pages

    projects.sort(key=lambda x: x.display_title)
    return render_template(
        "proofing/index.html",
        projects=projects,
        statuses_per_project=statuses_per_project,
        progress_per_project=progress_per_project,
        pages_per_project=pages_per_project,
    )


@bp.route("/help/beginners-guide")
def beginners_guide():
    """Display our minimal proofing guidelines."""
    return render_template("proofing/beginners-guide.html")


@bp.route("/help/complete-guide")
def complete_guide():
    """Display our complete proofing guidelines."""
    return render_template("proofing/complete-guide.html")


@bp.route("/help/editor-guide")
def editor_guide():
    """Describe how to use the page editor."""
    return render_template("proofing/editor-guide.html")


@bp.route("/create-project", methods=["GET", "POST"])
@p2_required
def create_project():
    form = CreateProjectForm()
    if not form.validate_on_submit():
        return render_template("proofing/create-project.html", form=form)

    pdf_source = form.pdf_source.data

    # if pdf_source == "gdrive":
    #     gdrive_folder_url = form.gdrive_folder_url.data
    #     task = project_tasks.create_projects_from_gdrive_folder.delay(
    #         folder_url=gdrive_folder_url,
    #         app_environment=current_app.config["AMBUDA_ENVIRONMENT"],
    #         creator_id=current_user.id,
    #         upload_folder=current_app.config["UPLOAD_FOLDER"],
    #     )
    #     return render_template(
    #         "proofing/create-project-post.html",
    #         stauts=task.status,
    #         current=0,
    #         total=0,
    #         percent=0,
    #         task_id=task.id,
    #     )

    display_title = form.display_title.data or None

    if pdf_source == "url":
        pdf_url = form.pdf_url.data
        task = project_tasks.create_project_from_url.delay(
            pdf_url=pdf_url,
            display_title=display_title,
            creator_id=current_user.id,
            app_environment=current_app.config["AMBUDA_ENVIRONMENT"],
        )
    else:
        # We accept only PDFs, so validate that the user hasn't uploaded some
        # other kind of document format.
        filename = form.local_file.raw_data[0].filename
        if not _is_allowed_document_file(filename):
            flash("Please upload a PDF.")
            return render_template("proofing/create-project.html", form=form)

        # Calculate MD5 of file content
        file_data = form.local_file.data
        file_data.seek(0)

        # Create all directories for this project ahead of time.
        # FIXME(arun): push this further into the Celery task.
        upload_dir = Path(current_app.config["UPLOAD_FOLDER"]) / "pdf-upload"
        upload_dir.mkdir(parents=True, exist_ok=True)

        temp_id = str(uuid.uuid4())
        pdf_path = upload_dir / f"{temp_id}.pdf"
        form.local_file.data.save(pdf_path)

        task = project_tasks.create_project_from_local_pdf.delay(
            pdf_path=str(pdf_path),
            display_title=display_title,
            creator_id=current_user.id,
            app_environment=current_app.config["AMBUDA_ENVIRONMENT"],
        )
    return render_template(
        "proofing/create-project-post.html",
        stauts=task.status,
        current=0,
        total=0,
        percent=0,
        task_id=task.id,
    )


@bp.route("/status/<task_id>")
def create_project_status(task_id):
    """AJAX summary of the task."""
    from ambuda.tasks import app as celery_app

    r = celery_app.AsyncResult(task_id)

    info = r.info or {}
    if isinstance(info, Exception):
        current = total = percent = 0
        slug = None
    else:
        current = info.get("current", 100)
        total = info.get("total", 100)
        slug = info.get("slug", None)
        percent = 100 * current / total

    return render_template(
        "include/task-progress.html",
        status=r.status,
        current=current,
        total=total,
        percent=percent,
        slug=slug,
    )


@bp.route("/recent-changes")
def recent_changes():
    """Show recent changes across all projects."""
    num_per_page = 100

    # Exclude bot edits, which overwhelm all other edits on the site.
    bot_user = q.user(consts.BOT_USERNAME)
    assert bot_user, "Bot user not defined"

    session = q.get_session()
    stmt = (
        select(db.Revision)
        .options(
            orm.defer(db.Revision.content),
            orm.selectinload(db.Revision.author).load_only(db.User.username),
            orm.selectinload(db.Revision.page).load_only(db.Page.slug),
            orm.selectinload(db.Revision.project).load_only(
                db.Project.slug, db.Project.display_title
            ),
            orm.selectinload(db.Revision.status).load_only(db.PageStatus.name),
        )
        .filter(db.Revision.author_id != bot_user.id)
        .order_by(db.Revision.created_at.desc())
        .limit(num_per_page)
    )
    recent_revisions = list(session.scalars(stmt).all())
    recent_activity = [("revision", r.created, r) for r in recent_revisions]

    stmt = (
        select(db.Project)
        .options(orm.selectinload(db.Project.creator).load_only(db.User.username))
        .order_by(db.Project.created_at.desc())
        .limit(num_per_page)
    )
    recent_projects = list(session.scalars(stmt).all())
    recent_activity += [("project", p.created_at, p) for p in recent_projects]

    recent_activity.sort(key=lambda x: x[1], reverse=True)
    recent_activity = recent_activity[:num_per_page]
    return render_template(
        "proofing/recent-changes.html", recent_activity=recent_activity
    )


@bp.route("/talk")
def talk():
    """Show discussion across all projects."""
    projects = q.active_projects()

    all_threads = [(p, t) for p in projects for t in p.board.threads]
    all_threads.sort(key=lambda x: x[1].updated_at, reverse=True)

    return render_template("proofing/talk.html", all_threads=all_threads)


@bp.route("/documents")
def documents():
    """List all pending proofing projects."""
    from ambuda.models.proofing import ProjectStatus

    session = q.get_session()
    projects = list(
        session.scalars(
            select(db.Project).filter(db.Project.status == ProjectStatus.PENDING)
        ).all()
    )
    projects.sort(key=lambda x: x.display_title)
    return render_template("proofing/documents.html", projects=projects)


@bp.route("/texts")
def texts():
    """List all published texts."""
    session = q.get_session()
    stmt = (
        select(db.Text)
        .options(
            orm.selectinload(db.Text.project).load_only(
                db.Project.slug, db.Project.display_title
            ),
            orm.selectinload(db.Text.author).load_only(db.Author.name),
        )
        .order_by(db.Text.created_at.desc())
    )
    texts = list(session.scalars(stmt).all())

    return render_template("proofing/texts.html", texts=texts)


@bp.route("/admin/dashboard/")
@moderator_required
def admin_dashboard():
    now = datetime.now(UTC).replace(tzinfo=None)
    days_ago_30d = now - timedelta(days=30)
    days_ago_7d = now - timedelta(days=7)
    days_ago_1d = now - timedelta(days=1)

    session = q.get_session()
    stmt = select(db.User).filter_by(username=consts.BOT_USERNAME)
    bot = session.scalars(stmt).one()
    bot_id = bot.id

    stmt = (
        select(db.Revision)
        .filter(
            (db.Revision.created_at >= days_ago_30d) & (db.Revision.author_id != bot_id)
        )
        .options(orm.load_only(db.Revision.created_at, db.Revision.author_id))
        .order_by(db.Revision.created_at)
    )
    revisions_30d = list(session.scalars(stmt).all())
    revisions_7d = [x for x in revisions_30d if x.created >= days_ago_7d]
    revisions_1d = [x for x in revisions_7d if x.created >= days_ago_1d]
    num_revisions_30d = len(revisions_30d)
    num_revisions_7d = len(revisions_7d)
    num_revisions_1d = len(revisions_1d)

    num_contributors_30d = len({x.author_id for x in revisions_30d})
    num_contributors_7d = len({x.author_id for x in revisions_7d})
    num_contributors_1d = len({x.author_id for x in revisions_1d})

    return render_template(
        "proofing/dashboard.html",
        num_revisions_30d=num_revisions_30d,
        num_revisions_7d=num_revisions_7d,
        num_revisions_1d=num_revisions_1d,
        num_contributors_30d=num_contributors_30d,
        num_contributors_7d=num_contributors_7d,
        num_contributors_1d=num_contributors_1d,
    )
