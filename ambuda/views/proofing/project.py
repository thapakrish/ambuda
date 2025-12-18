import dataclasses as dc
import json
import logging
import re
from datetime import UTC, datetime
from xml.etree import ElementTree as ET

from celery.result import GroupResult
from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    make_response,
    render_template,
    request,
    url_for,
)
from flask_babel import lazy_gettext as _l
from flask_login import current_user, login_required
from flask_wtf import FlaskForm
from markupsafe import Markup, escape
from pydantic import BaseModel, TypeAdapter
import sqlalchemy as sqla
from sqlalchemy import orm, select
from werkzeug.exceptions import abort
from werkzeug.utils import redirect
from wtforms import (
    BooleanField,
    FieldList,
    Form,
    FormField,
    HiddenField,
    StringField,
    SubmitField,
)
from wtforms.validators import DataRequired, ValidationError
from wtforms.widgets import TextArea
from wtforms_sqlalchemy.fields import QuerySelectField

from ambuda import database as db
from ambuda import queries as q
from ambuda.models.proofing import PublishConfig, ProjectConfig
from ambuda.tasks import app as celery_app
from ambuda.tasks import llm_structuring as llm_structuring_tasks
from ambuda.tasks import ocr as ocr_tasks
from ambuda.utils import diff as diff_utils
from ambuda.utils import project_utils, proofing_utils, structuring
from ambuda.utils.structuring import ProofBlock, ProofPage, ProofProject, TEIDocument
from ambuda.utils.revisions import add_revision
from ambuda.views.proofing.decorators import moderator_required, p2_required
from ambuda.views.proofing.stats import calculate_stats

bp = Blueprint("project", __name__)
LOG = logging.getLogger(__name__)


@dc.dataclass
class BlockType:
    tag: str
    label: str


@dc.dataclass
class Language:
    code: str
    label: str


BLOCK_TYPES = [
    BlockType("p", "paragraph"),
    BlockType("verse", "verse"),
    BlockType("heading", "heading"),
    BlockType("title", "title"),
    BlockType("subtitle", "subtitle"),
    BlockType("footnote", "footnote"),
    BlockType("trailer", "trailer"),
    BlockType("ignore", "ignore"),
]

LANGUAGES = [
    Language(code="sa", label="Sanskrit"),
    Language(code="hi", label="Hindi"),
    Language(code="en", label="English"),
]


def _is_valid_page_number_spec(_, field):
    try:
        _ = project_utils.parse_page_number_spec(field.data)
    except Exception as e:
        raise ValidationError("The page number spec isn't valid.") from e


class EditMetadataForm(FlaskForm):
    display_title = StringField(
        _l("Display title"),
        render_kw={
            "placeholder": _l("e.g. Avantisundarīkathā"),
        },
        validators=[DataRequired()],
    )
    description = StringField(
        _l("Description (optional)"),
        widget=TextArea(),
        render_kw={
            "placeholder": _l(
                "What is this book about? Why is this project interesting?"
            ),
        },
    )
    page_numbers = StringField(
        _l("Page numbers (optional)"),
        widget=TextArea(),
        validators=[_is_valid_page_number_spec],
        render_kw={
            "placeholder": "Coming soon.",
        },
    )
    genre = QuerySelectField(
        query_factory=q.genres, allow_blank=True, blank_text=_l("(none)")
    )

    print_title = StringField(
        _l("Print title"),
        render_kw={
            "placeholder": _l(
                "e.g. Śrīdaṇḍimahākaviviracitam avantisundarīkathā nāma gadyakāvyam"
            ),
        },
    )
    author = StringField(
        _l("Author"),
        render_kw={
            "placeholder": _l("The author of the original work, e.g. Kalidasa."),
        },
    )
    editor = StringField(
        _l("Editor"),
        render_kw={
            "placeholder": _l(
                "The person or organization that created this edition, e.g. M.R. Kale."
            ),
        },
    )
    publisher = StringField(
        _l("Publisher"),
        render_kw={
            "placeholder": _l(
                "The original publisher of this book, e.g. Nirnayasagar."
            ),
        },
    )
    worldcat_link = StringField(
        _l("Worldcat link"),
        render_kw={
            "placeholder": _l("A link to this book's entry on worldcat.org."),
        },
    )
    publication_year = StringField(
        _l("Publication year"),
        render_kw={
            "placeholder": _l("The year in which this specific edition was published."),
        },
    )

    notes = StringField(
        _l("Notes (optional)"),
        widget=TextArea(),
        render_kw={
            "placeholder": _l("Internal notes for scholars and other proofreaders."),
        },
    )


class MatchForm(Form):
    selected = BooleanField()
    replace = HiddenField(validators=[DataRequired()])


class SearchForm(FlaskForm):
    class Meta:
        csrf = False

    query = StringField(_l("Query"), validators=[DataRequired()])


class DeleteProjectForm(FlaskForm):
    slug = StringField("Slug", validators=[DataRequired()])


class ReplaceForm(SearchForm):
    class Meta:
        csrf = False

    replace = StringField(_l("Replace"), validators=[DataRequired()])


def validate_matches(form, field):
    for match_form in field:
        if match_form.errors:
            raise ValidationError("Invalid match form values.")


class PreviewChangesForm(ReplaceForm):
    class Meta:
        csrf = False

    matches = FieldList(FormField(MatchForm), validators=[validate_matches])
    submit = SubmitField("Preview changes")


class ConfirmChangesForm(ReplaceForm):
    class Meta:
        csrf = False

    confirm = SubmitField("Confirm")
    cancel = SubmitField("Cancel")


@bp.route("/<slug>/")
def summary(slug):
    """Show basic information about the project."""
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    assert project_
    session = q.get_session()
    stmt = (
        sqla.select(db.Revision)
        .filter_by(project_id=project_.id)
        .order_by(db.Revision.created_at.desc())
        .limit(10)
    )
    recent_revisions = list(session.scalars(stmt).all())

    page_rules = project_utils.parse_page_number_spec(project_.page_numbers)
    page_titles = project_utils.apply_rules(len(project_.pages), page_rules)
    return render_template(
        "proofing/projects/summary.html",
        project=project_,
        pages=zip(page_titles, project_.pages),
        recent_revisions=recent_revisions,
    )


@bp.route("/<slug>/activity")
def activity(slug):
    """Show recent activity on this project."""
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    assert project_
    session = q.get_session()
    stmt = (
        sqla.select(db.Revision)
        .options(orm.defer(db.Revision.content))
        .filter_by(project_id=project_.id)
        .order_by(db.Revision.created_at.desc())
        .limit(100)
    )
    recent_revisions = list(session.scalars(stmt).all())
    recent_activity = [("revision", r.created, r) for r in recent_revisions]
    recent_activity.append(("project", project_.created_at, project_))

    return render_template(
        "proofing/projects/activity.html",
        project=project_,
        recent_activity=recent_activity,
    )


@bp.route("/<slug>/edit", methods=["GET", "POST"])
@login_required
def edit(slug):
    """Edit the project's metadata."""
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    form = EditMetadataForm(obj=project_)
    if form.validate_on_submit():
        session = q.get_session()
        form.populate_obj(project_)
        session.commit()

        flash(_l("Saved changes."), "success")
        return redirect(url_for("proofing.project.summary", slug=slug))

    return render_template(
        "proofing/projects/edit.html",
        project=project_,
        form=form,
    )


@bp.route("/<slug>/download/")
def download(slug):
    """Download the project in various output formats."""
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    return render_template("proofing/projects/download.html", project=project_)


@bp.route("/<slug>/download/text")
def download_as_text(slug):
    """Download the project as plain text."""
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    content_blobs = [
        p.revisions[-1].content if p.revisions else "" for p in project_.pages
    ]
    raw_text = proofing_utils.to_plain_text(content_blobs)

    response = make_response(raw_text, 200)
    response.mimetype = "text/plain"
    return response


@bp.route("/<slug>/download/xml")
def download_as_xml(slug):
    """Download the project as TEI XML.

    This XML will likely have various errors, but it is correct enough that it
    still saves a lot of manual work.
    """
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    assert project_
    project_meta = {
        "title": project_.display_title,
        "author": project_.author,
        "publication_year": project_.publication_year,
        "publisher": project_.publisher,
        "editor": project_.editor,
    }
    project_meta = {k: v or "TODO" for k, v in project_meta.items()}
    content_blobs = [
        p.revisions[-1].content if p.revisions else "" for p in project_.pages
    ]
    xml_blob = proofing_utils.to_tei_xml(project_meta, content_blobs)

    response = make_response(xml_blob, 200)
    response.mimetype = "text/xml"
    return response


@bp.route("/<slug>/stats")
@moderator_required
def stats(slug):
    """Show basic statistics about this project.

    Currently, these stats don't show any sensitive information. But since that
    might change in the future, limit this page to moderators only.
    """
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    assert project_
    stats_ = calculate_stats(project_)
    return render_template(
        "proofing/projects/stats.html", project=project_, stats=stats_
    )


def _select_changes(project_, selected_keys, query: str, replace: str):
    """
    Mark "query" strings
    """
    results = []
    LOG.debug(f"{__name__}: Mark changes with {query} and {replace}")
    query_pattern = re.compile(
        query, re.UNICODE
    )  # Compile the regex pattern with Unicode support
    for page_ in project_.pages:
        if not page_.revisions:
            continue

        latest = page_.revisions[-1]
        matches = []
        for line_num, line in enumerate(latest.content.splitlines()):
            form_key = f"match{page_.slug}-{line_num}"
            replace_form_key = f"match{page_.slug}-{line_num}-replace"

            if selected_keys.get(form_key) == "selected":
                LOG.debug(f"{__name__}: {form_key}: {selected_keys.get(form_key)}")
                LOG.debug(
                    f"{__name__}: {replace_form_key}: {request.form.get(replace_form_key)}"
                )
                LOG.debug(f"{__name__}: {form_key}: Appended")
                replaced_line = query_pattern.sub(replace, line)
                matches.append(
                    {
                        "query": line,
                        "replace": replaced_line,
                        "line_num": line_num,
                    }
                )

        results.append({"page": page_, "matches": matches})
        LOG.debug(f"{__name__}: Total matches appended: {len(matches)}")

    selected_count = sum(value == "selected" for value in selected_keys.values())
    LOG.debug(f"{__name__} > Number of selected changes = {selected_count}")

    return render_template(
        "proofing/projects/confirm_changes.html",
        project=project_,
        form=ConfirmChangesForm(),
        query=query,
        replace=replace,
        results=results,
    )


@bp.route("/<slug>/submit-changes", methods=["GET", "POST"])
@p2_required
def submit_changes(slug):
    """Submit selected changes across all of the project's pages.

    This is useful to replace a string across the project in one shot.
    """

    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    LOG.debug(
        f"{__name__}: SUBMIT_CHANGES --- {request.method} > {list(request.form.keys())}"
    )

    # FIXME: find a way to validate this form. Current `matches` are coming in the way of validators.
    form = PreviewChangesForm(request.form)
    # if not form.validate():
    #     # elif request.form.get("form_submitted") is None:
    #     invalid_keys = list(form.errors.keys())
    #     LOG.debug(f'{__name__}: Invalid form values - {request.method}, invalid keys: {invalid_keys}')
    #     return redirect(url_for("proofing.project.replace", slug=slug))

    render = None
    # search for "query" string and replace with "update" string
    query = form.query.data
    replace = form.replace.data

    LOG.debug(
        f"{__name__}: ({request.method})>  Got to submit method with {query}->{replace} "
    )
    LOG.debug(f"{__name__}: {request.method} > {list(request.form.keys())}")
    selected_keys = {
        key: value
        for key, value in request.form.items()
        if key.startswith("match") and not key.endswith("replace")
    }
    render = _select_changes(project_, selected_keys, query=query, replace=replace)

    return render


@bp.route("/<slug>/confirm_changes", methods=["GET", "POST"])
@p2_required
def confirm_changes(slug):
    """Confirm changes to replace a string across all of the project's pages."""
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    LOG.debug(
        f"{__name__}: confirm_changes {request.method} > Keys: {list(request.form.keys())}, Items: {list(request.form.items())}"
    )
    assert project_
    form = ConfirmChangesForm(request.form)
    if not form.validate():
        flash("Invalid input.", "danger")
        invalid_keys = list(form.errors.keys())
        LOG.error(
            f"{__name__}: Invalid form - {request.method}, invalid keys: {invalid_keys}"
        )
        return redirect(url_for("proofing.project.replace", slug=slug))

    if form.confirm.data:
        LOG.debug(f"{__name__}: {request.method} > Confirmed!")
        query = form.query.data
        replace = form.replace.data

        # Get the changes from the form and store them in a list
        pages = {}

        # Iterate over the dictionary `request.form`
        for key, value in request.form.items():
            # Check if key matches the pattern
            match = re.match(r"match(\d+)-(\d+)-replace", key)
            if match:
                # Extract page_slug and line_num from the key
                page_slug = match.group(1)
                line_num = int(match.group(2))
                if page_slug not in pages:
                    pages[page_slug] = {}
                pages[page_slug][line_num] = value

        for page_slug, changed_lines in pages.items():
            # Get the corresponding `Page` object
            LOG.debug(f"{__name__}: Project - {project_.slug}, Page : {page_slug}")

            # Page query needs id for project and slug for page
            page = q.page(project_.id, page_slug)
            if not page:
                LOG.error(
                    f"{__name__}: Page not found for project - {project_.slug}, page : {page_slug}"
                )
                return render_template(url_for("proofing.project.replace", slug=slug))

            latest = page.revisions[-1]
            current_lines = latest.content.splitlines()
            # Iterate over the `lines` dictionary
            for line_num, replace_value in changed_lines.items():
                # Check if the line_num exists in the dictionary for this page
                LOG.debug(
                    f"{__name__}: Current - {current_lines[line_num]}, Length of lines = {len(current_lines)}"
                )
                if line_num < len(current_lines):
                    # Replace the line with the replacement value
                    current_lines[line_num] = replace_value
                else:
                    LOG.error(
                        f"{__name__}: Invalid line number {line_num} in {page_slug} which has only {len(current_lines)}"
                    )
                    continue
            # Join the lines into a single string
            new_content = "\n".join(current_lines)
            # Check if the page content has changed
            if new_content != latest.content:
                # Add a new revision to the page
                new_summary = f'Replaced "{query}" with "{replace}"'
                new_revision = add_revision(
                    page=page,
                    summary=new_summary,
                    content=new_content,
                    version=page.version,
                    author_id=current_user.id,
                    status_id=page.status_id,
                )
                LOG.debug(f"{__name__}: New revision > {page_slug}: {new_revision}")

        flash("Changes applied.", "success")
        return redirect(url_for("proofing.project.activity", slug=slug))
    elif form.cancel.data:
        LOG.debug(f"{__name__}: confirm_changes Cancelled")
        return redirect(url_for("proofing.project.edit", slug=slug))

    return render_template(url_for("proofing.project.edit", slug=slug))


@bp.route("/<slug>/batch-ocr", methods=["GET", "POST"])
@p2_required
def batch_ocr(slug):
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    assert project_
    if request.method == "POST":
        task = ocr_tasks.run_ocr_for_project(
            app_env=current_app.config["AMBUDA_ENVIRONMENT"],
            project=project_,
        )
        if task:
            return render_template(
                "proofing/projects/batch-ocr-post.html",
                project=project_,
                status="PENDING",
                current=0,
                total=0,
                percent=0,
                task_id=task.id,
            )
        else:
            flash(_l("All pages in this project have at least one edit already."))

    return render_template(
        "proofing/projects/batch-ocr.html",
        project=project_,
    )


@bp.route("/batch-ocr-status/<task_id>")
def batch_ocr_status(task_id):
    r = GroupResult.restore(task_id, app=celery_app)
    assert r, task_id

    if r.results:
        current = r.completed_count()
        total = len(r.results)
        percent = current / total

        status = None
        if total:
            if current == total:
                status = "SUCCESS"
            else:
                status = "PROGRESS"
        else:
            status = "FAILURE"

        data = {
            "status": status,
            "current": current,
            "total": total,
            "percent": percent,
        }
    else:
        data = {
            "status": "PENDING",
            "current": 0,
            "total": 0,
            "percent": 0,
        }

    return render_template(
        "include/ocr-progress.html",
        **data,
    )


class BlockDiff(BaseModel):
    type: str
    content: str | None = None
    text: str | None = None
    n: str | None = None
    lang: str | None = None
    mark: str | None = None
    merge_next: bool = False
    index: int | None = None  # Original block index for existing blocks


class PageDiff(BaseModel):
    slug: str
    version: int
    blocks: list[BlockDiff]
    ignore: bool = False


class ProjectDiff(BaseModel):
    project: str
    pages: list[PageDiff]


@bp.route("/<slug>/batch-editing", methods=["GET", "POST"])
@p2_required
def batch_editing(slug):
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    assert project_
    if request.method == "POST":
        data = request.form.get("structure_data")
        if not data:
            flash("No data provided", "error")
            return redirect(url_for("proofing.project.batch_editing", slug=slug))

        try:
            project_diff = ProjectDiff.model_validate_json(data)
        except json.JSONDecodeError:
            flash("Invalid structure data format", "error")
            return redirect(url_for("proofing.project.batch_editing", slug=slug))

        # Group all batch changes with a batch ID so we can revert/dedupe later.
        session = q.get_session()
        revision_batch = db.RevisionBatch(user_id=current_user.id)
        session.add(revision_batch)
        session.flush()

        changed_pages = []
        unchanged_pages = []
        errors = []

        page_slugs = []
        for p in project_diff.pages:
            page_slugs.append(p.slug)
        pages = q.pages_with_revisions(project_.id, page_slugs)
        page_map = {p.slug: p for p in pages}

        for page_diff in project_diff.pages:
            if page_diff.ignore:
                continue

            page_slug = page_diff.slug
            if page_slug not in page_map:
                errors.append(f"Page {page_slug} not found")
                continue

            page = page_map[page_slug]
            if not page.revisions:
                errors.append(f"Page {page_slug} has no revisions.")
                continue

            latest_revision = page.revisions[-1]
            old_content = latest_revision.content
            old_structured_page = ProofPage.from_revision(latest_revision)

            new_blocks = []
            had_parse_error = False
            for i, block_data in enumerate(page_diff.blocks):
                content = block_data.content
                if content is None:
                    source_index = block_data.index
                    if source_index is not None:
                        content = old_structured_page.blocks[source_index].content
                    else:
                        content = ""

                try:
                    new_block = ProofBlock(
                        type=block_data.type,
                        content=content,
                        lang=block_data.lang,
                        text=block_data.text,
                        n=block_data.n,
                        mark=block_data.mark,
                        merge_next=block_data.merge_next,
                    )
                except KeyError as e:
                    errors.append(f"Could not parse data for {page_slug}/{i}.")
                    had_parse_error = True
                    break

                new_blocks.append(new_block)

            if had_parse_error:
                errors.append(f"Could not parse edits for {page_slug}.")
                continue

            new_structured_page = ProofPage(blocks=new_blocks, id=page.id)

            new_content = new_structured_page.to_xml_string()
            if old_content == new_content:
                unchanged_pages.append(page_slug)
                continue

            try:
                add_revision(
                    page=page,
                    summary="Batch structuring",
                    content=new_content,
                    version=page_diff.version,
                    author_id=current_user.id,
                    status_id=page.status_id,
                    batch_id=revision_batch.id,
                )
                changed_pages.append(page_slug)
            except Exception as e:
                errors.append(f"Failed to save page {page_slug}: {str(e)}")
                LOG.error(f"Failed to save batch structuring for {page_slug}: {e}")

        _plural = lambda n: "s" if n > 1 else ""

        message_parts = []
        if changed_pages:
            message_parts.append(
                f"Saved {len(changed_pages)} changed page{_plural(len(changed_pages))}"
            )
        if unchanged_pages:
            message_parts.append(f"{len(unchanged_pages)} unchanged")
        if not changed_pages and not unchanged_pages:
            message_parts.append("No pages to save")

        message = ", ".join(message_parts) + "."
        if errors:
            message += f" ({len(errors)} error{_plural(len(errors))})"
            flash(message, "warning")
        elif len(changed_pages) > 0:
            flash(message, "success")
        else:
            flash(message, "info")

        return redirect(url_for("proofing.project.summary", slug=slug))

    pages_with_content = []
    for page in project_.pages:
        if page.revisions:
            latest_revision = page.revisions[-1]
            structured_data = ProofPage.from_revision(latest_revision)

            pages_with_content.append(
                {
                    "slug": page.slug,
                    "version": page.version,
                    "blocks": structured_data.blocks,
                }
            )

    return render_template(
        "proofing/projects/batch-editing.html",
        project=project_,
        pages_with_content=pages_with_content,
        block_types=BLOCK_TYPES,
        languages=LANGUAGES,
    )


@bp.route("/<slug>/parse-content", methods=["POST"])
@login_required
def parse_content(slug):
    """Parse content and return structured blocks.

    This is a convenience API for the batch editing workflow.
    """
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    data = request.get_json()
    if not data or "content" not in data:
        return jsonify({"error": "No content provided"}), 400

    content = data["content"]
    if not content or not content.strip():
        return jsonify({"error": "Content is empty"}), 400

    try:
        # page_id is not used, so use a dummy value
        parsed_page = ProofPage.from_content_and_page_id(content, page_id=0)
        blocks = []
        for block in parsed_page.blocks:
            blocks.append(
                {
                    "type": block.type,
                    "content": block.content,
                    "lang": block.lang,
                    "text": block.text,
                    "n": block.n,
                    "mark": block.mark,
                    "merge_next": block.merge_next,
                }
            )

        return jsonify({"blocks": blocks})
    except Exception as e:
        LOG.error(f"Failed to parse content: {e}")
        return jsonify({"error": f"Failed to parse content: {str(e)}"}), 500


@bp.route("/<slug>/batch-llm-structuring", methods=["GET", "POST"])
@moderator_required
def batch_llm_structuring(slug):
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    assert project_
    if request.method == "POST":
        task = llm_structuring_tasks.run_structuring_for_project(
            app_env=current_app.config["AMBUDA_ENVIRONMENT"],
            project=project_,
        )
        if task:
            return render_template(
                "proofing/projects/batch-llm-structuring-post.html",
                project=project_,
                status="PENDING",
                current=0,
                total=0,
                percent=0,
                task_id=task.id,
            )
        else:
            flash(_l("No edited pages found in this project."))

    return render_template(
        "proofing/projects/batch-llm-structuring.html",
        project=project_,
    )


@bp.route("/batch-llm-structuring-status/<task_id>")
def batch_llm_structuring_status(task_id):
    r = GroupResult.restore(task_id, app=celery_app)
    assert r, task_id

    if r.results:
        current = r.completed_count()
        total = len(r.results)
        percent = current / total

        status = None
        if total:
            if current == total:
                status = "SUCCESS"
            else:
                status = "PROGRESS"
        else:
            status = "FAILURE"

        data = {
            "status": status,
            "current": current,
            "total": total,
            "percent": percent,
        }
    else:
        data = {
            "status": "PENDING",
            "current": 0,
            "total": 0,
            "percent": 0,
        }

    return render_template(
        "include/llm-structuring-progress.html",
        **data,
    )


@bp.route("/<slug>/publish", methods=["GET", "POST"])
@p2_required
def publish_config(slug):
    """Configure publish settings for the project."""
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    assert project_
    if request.method == "POST":
        publish_json = request.form.get("publish_config", "")
        default = lambda: render_template(
            "proofing/projects/publish.html",
            project=project_,
            publish_config=publish_json,
        )

        try:
            new_config = ProjectConfig.model_validate_json(publish_json)
        except Exception as e:
            flash(f"Validation error: {e}", "error")
            return default()

        try:
            old_config = ProjectConfig.model_validate_json(project_.config or "{}")
        except Exception as e:
            flash(f"Validation error: {e}", "error")
            return default()

        # TODO: tighten restrictions here -- should only be able to update 'publish' ?
        if new_config != old_config:
            session = q.get_session()
            project_.config = new_config.model_dump_json()
            session.commit()

        return redirect(url_for("proofing.project.publish_preview", slug=slug))

    try:
        project_config = ProjectConfig.model_validate_json(project_.config or "{}")
    except Exception:
        flash("Project config is invalid. Please contact an admin user.", "error")
        return redirect(url_for("proofing.index"))

    publish_config = project_config.model_dump_json(indent=2)
    publish_config_schema = PublishConfig.model_json_schema()

    return render_template(
        "proofing/projects/publish.html",
        project=project_,
        publish_config=publish_config,
        publish_config_schema=publish_config_schema,
    )


@bp.route("/<slug>/publish/preview", methods=["GET"])
@p2_required
def publish_preview(slug):
    """Preview the changes that will be made when publishing."""
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    assert project_
    config_page = lambda: redirect(
        url_for("proofing.project.publish_config", slug=slug)
    )
    if not project_.config:
        flash("No publish configuration found. Please configure first.", "error")
        return config_page()
    try:
        project_config = ProjectConfig.model_validate_json(project_.config or "{}")
    except Exception as e:
        flash("Could not validate project config", "error")
        return config_page()
    if not project_config.publish:
        flash("No publish configuration found. Please configure first.", "error")
        return config_page()

    session = q.get_session()
    previews = []

    text_slugs = [config.slug for config in project_config.publish]
    existing_texts = (
        session.execute(sqla.select(db.Text).where(db.Text.slug.in_(text_slugs)))
        .scalars()
        .all()
    )
    text_map = {text.slug: text for text in existing_texts}
    text_ids = [text.id for text in existing_texts]

    blocks_by_text = {}
    if text_ids:
        existing_blocks_query = (
            session.execute(
                sqla.select(db.TextBlock)
                .where(db.TextBlock.text_id.in_(text_ids))
                .order_by(db.TextBlock.text_id, db.TextBlock.n)
            )
            .scalars()
            .all()
        )
        for block in existing_blocks_query:
            blocks_by_text.setdefault(block.text_id, []).append(block)

    for config in project_config.publish:
        existing_text = text_map.get(config.slug)
        document = _create_tei_document(project_, config.target)

        new_blocks = [b.xml for section in document.sections for b in section.blocks]
        existing_blocks = []
        if existing_text:
            existing_blocks = blocks_by_text.get(existing_text.id, [])

        diffs = []
        max_len = max(len(new_blocks), len(existing_blocks))
        for i in range(max_len):
            old_xml = existing_blocks[i].xml if i < len(existing_blocks) else None
            new_xml = new_blocks[i] if i < len(new_blocks) else None

            if old_xml is None and new_xml is not None:
                diffs.append(
                    {
                        "type": "added",
                        "diff": new_xml,
                    }
                )
            elif old_xml is not None and new_xml is None:
                diffs.append(
                    {
                        "type": "removed",
                        "diff": old_xml,
                    }
                )
            elif old_xml != new_xml:
                diffs.append(
                    {
                        "type": "changed",
                        "diff": diff_utils.revision_diff(old_xml, new_xml),
                    }
                )

        parent_info = None
        if config.parent_slug:
            parent_text = session.execute(
                sqla.select(db.Text).where(db.Text.slug == config.parent_slug)
            ).scalar_one_or_none()
            if parent_text:
                parent_info = {"slug": parent_text.slug, "title": parent_text.title}

        preview = {
            "slug": config.slug,
            "title": config.title,
            "target": config.target,
            "is_new": existing_text is None,
            "parent": parent_info,
            "diffs": diffs,
        }

        previews.append(preview)

    return render_template(
        "proofing/projects/publish-preview.html",
        project=project_,
        previews=previews,
    )


def _create_tei_document(project_, target: str) -> TEIDocument:
    """Gather content blocks from all pages for a specific target."""
    revisions = []
    for page in project_.pages:
        if not page.revisions:
            continue
        latest_revision = page.revisions[-1]
        revisions.append(latest_revision)

    rules = project_utils.parse_page_number_spec(project_.page_numbers)
    page_numbers = project_utils.apply_rules(len(project_.pages), rules)
    proof_project = ProofProject.from_revisions(revisions)
    doc, _errors = proof_project.to_tei_document(
        target=target, page_numbers=page_numbers
    )
    return doc


@bp.route("/<slug>/publish/create", methods=["POST"])
@p2_required
def publish_create(slug):
    """Create or update texts based on the publish configuration."""
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    assert project_
    lambda config_page: redirect(url_for("proofing.project.publish_config", slug=slug))
    if not project_.config:
        flash("No publish configuration found. Please configure first.", "error")
        return config_page()

    try:
        project_config = ProjectConfig.model_validate_json(project_.config)
    except Exception:
        flash("Could not validate project config.", "error")
        return config_page()

    session = q.get_session()
    created_count = 0
    updated_count = 0
    texts_map = {}

    for config in project_config.publish:
        document = _create_tei_document(project_, config.target)

        text = q.text(config.slug)
        is_new_text = False
        if not text:
            text = db.Text(
                slug=config.slug,
                title=config.title,
                published_at=datetime.now(UTC),
                project_id=project_.id,
            )
            session.add(text)
            session.flush()
            is_new_text = True

        text.project_id = project_.id
        text.language = config.language
        text.title = config.title

        existing_sections = {s.slug for s in text.sections}
        doc_sections = {s.slug for s in document.sections}
        section_map = {s.slug: s for s in text.sections}

        if existing_sections != doc_sections:
            new_sections = doc_sections - existing_sections
            old_sections = existing_sections - doc_sections

            for old_slug in old_sections:
                old_section = next(
                    (s for s in text.sections if s.slug == old_slug), None
                )
                if old_section:
                    session.delete(old_section)
                    del section_map[old_slug]

            for new_slug in new_sections:
                doc_section = next(
                    (s for s in document.sections if s.slug == new_slug), None
                )
                if doc_section:
                    new_section = db.TextSection(
                        text_id=text.id,
                        slug=new_slug,
                        title=new_slug,
                    )
                    session.add(new_section)
                    section_map[new_slug] = new_section

            session.flush()

        existing_blocks = {
            b.slug
            for b in session.execute(
                sqla.select(db.TextBlock).where(db.TextBlock.text_id == text.id)
            )
            .scalars()
            .all()
        }
        doc_blocks = {b.slug for s in document.sections for b in s.blocks}

        if existing_blocks != doc_blocks:
            old_blocks = existing_blocks - doc_blocks
            new_blocks = doc_blocks - existing_blocks

            if old_blocks:
                session.execute(
                    sqla.delete(db.TextBlock).where(
                        db.TextBlock.text_id == text.id,
                        db.TextBlock.slug.in_(old_blocks),
                    )
                )

            existing_blocks = existing_blocks - old_blocks
        else:
            new_blocks = set()

        existing_blocks_map = {}
        if existing_blocks:
            existing_blocks_list = (
                session.execute(
                    sqla.select(db.TextBlock).where(
                        db.TextBlock.text_id == text.id,
                        db.TextBlock.slug.in_(existing_blocks),
                    )
                )
                .scalars()
                .all()
            )
            existing_blocks_map = {b.slug: b for b in existing_blocks_list}

        block_index = 0
        for doc_section in document.sections:
            section = section_map[doc_section.slug]

            for block in doc_section.blocks:
                block_index += 1

                if block.slug in existing_blocks_map:
                    existing_block = existing_blocks_map[block.slug]
                    existing_block.xml = block.xml
                    existing_block.n = block_index
                    existing_block.section_id = section.id
                    existing_block.page_id = block.page_id
                elif block.slug in new_blocks:
                    new_block = db.TextBlock(
                        text_id=text.id,
                        section_id=section.id,
                        slug=block.slug,
                        xml=block.xml,
                        n=block_index,
                        page_id=block.page_id,
                    )
                    session.add(new_block)

        texts_map[config.slug] = text
        if is_new_text:
            created_count += 1
        else:
            updated_count += 1

    session.flush()

    for config in project_config.publish:
        if config.parent_slug:
            text = texts_map[config.slug]
            parent_text = texts_map.get(config.parent_slug) or q.text(
                config.parent_slug
            )
            if parent_text:
                text.parent_id = parent_text.id

    session.flush()

    for config in project_config.publish:
        if config.parent_slug:
            text = texts_map[config.slug]
            parent_text = texts_map.get(config.parent_slug) or q.text(
                config.parent_slug
            )
            if not parent_text:
                continue

            parent_blocks = (
                session.execute(
                    sqla.select(db.TextBlock)
                    .where(db.TextBlock.text_id == parent_text.id)
                    .order_by(db.TextBlock.n)
                )
                .scalars()
                .all()
            )

            child_blocks = (
                session.execute(
                    sqla.select(db.TextBlock)
                    .where(db.TextBlock.text_id == text.id)
                    .order_by(db.TextBlock.n)
                )
                .scalars()
                .all()
            )

            child_block_ids = [b.id for b in child_blocks]
            if child_block_ids:
                session.execute(
                    sqla.delete(db.text_block_associations).where(
                        db.text_block_associations.c.child_id.in_(child_block_ids)
                    )
                )

            parent_blocks_by_slug = {b.slug: b for b in parent_blocks}
            for child_block in child_blocks:
                parent_block = parent_blocks_by_slug.get(child_block.slug)
                if parent_block:
                    session.execute(
                        sqla.insert(db.text_block_associations).values(
                            parent_id=parent_block.id,
                            child_id=child_block.id,
                        )
                    )

    session.commit()

    if created_count > 0:
        flash(f"Created {created_count} text(s)", "success")
    if updated_count > 0:
        flash(f"Updated {updated_count} text(s)", "success")

    return redirect(url_for("proofing.project.publish_config", slug=slug))


@bp.route("/<slug>/admin", methods=["GET", "POST"])
@moderator_required
def admin(slug):
    """View admin controls for the project.

    We restrict these operations to admins because they are destructive in the
    wrong hands. Current list of admin operations:

    - delete project
    """
    project_ = q.project(slug)
    if project_ is None:
        abort(404)

    form = DeleteProjectForm()
    if form.validate_on_submit():
        if form.slug.data == slug:
            session = q.get_session()
            session.delete(project_)
            session.commit()

            flash(f"Deleted project {slug}")
            return redirect(url_for("proofing.index"))
        else:
            form.slug.errors.append("Deletion failed (incorrect field value).")

    return render_template(
        "proofing/projects/admin.html",
        project=project_,
        form=form,
    )
