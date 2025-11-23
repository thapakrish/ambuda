from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable
import tempfile

from flask import Blueprint, render_template, abort, request, redirect, url_for, flash
from flask_login import current_user
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from sqlalchemy import inspect, Text
from sqlalchemy.exc import SQLAlchemyError
from wtforms import (
    Form,
    StringField,
    IntegerField,
    TextAreaField,
    BooleanField,
    DateTimeField,
    SelectField,
)
from wtforms.validators import Optional, DataRequired

import ambuda.database as db
import ambuda.queries as q
import ambuda.data_utils as data_utils
from ambuda.utils.tei_parser import parse_document
from sqlalchemy import select
from sqlalchemy.orm import Session

bp = Blueprint("admin", __name__)


class Category(str, Enum):
    """Model categories (for sidebar grouping)"""

    AUTH = "Auth"
    TEXTS = "Texts"
    DICTIONARIES = "Dictionaries"
    PROOFING = "Proofing"
    SITE = "Site"
    DISCUSSION = "Discussion"
    BLOG = "Blog"


@dataclass
class Task:
    """A custom task associated with some model."""

    name: str
    slug: str
    handler: Callable


def get_foreign_key_info(model_class):
    inspector = inspect(model_class)
    fk_map = {}
    for column in inspector.columns:
        if column.foreign_keys:
            fk = list(column.foreign_keys)[0]
            target_table = fk.column.table.name
            target_model = None
            for config in MODEL_CONFIG:
                if config.model.__tablename__ == target_table:
                    target_model = config.model.__name__
                    break
            if target_model:
                fk_map[column.name] = target_model
    return fk_map


def create_model_form(model_class, obj=None):
    inspector = inspect(model_class)
    fields = {}

    for column in inspector.columns:
        col_name = column.name
        col_type = column.type

        if column.primary_key or col_name in ["created_at", "updated_at"]:
            continue

        field_kwargs = {"validators": [Optional()] if column.nullable else []}

        if column.foreign_keys:
            fk = list(column.foreign_keys)[0]
            target_table = fk.column.table.name
            target_model_class = None
            for config in MODEL_CONFIG:
                if config.model.__tablename__ == target_table:
                    target_model_class = config.model
                    break

            if target_model_class:
                session = q.get_session()
                choices = [(None, "-- None --")] if column.nullable else []
                for item in session.query(target_model_class).limit(100).all():
                    label = str(
                        getattr(item, "slug", None)
                        or getattr(item, "name", None)
                        or getattr(item, "title", None)
                        or getattr(item, "id")
                    )
                    choices.append((item.id, label))
                fields[col_name] = SelectField(
                    col_name,
                    choices=choices,
                    coerce=lambda x: int(x) if x else None,
                    **field_kwargs,
                )
                continue

        python_type = col_type.python_type
        if python_type == int:
            fields[col_name] = IntegerField(col_name, **field_kwargs)
        elif python_type == bool:
            fields[col_name] = BooleanField(col_name)
        elif python_type == str:
            if isinstance(col_type, Text) or (
                hasattr(col_type, "length")
                and col_type.length
                and col_type.length > 255
            ):
                fields[col_name] = TextAreaField(col_name, **field_kwargs)
            else:
                fields[col_name] = StringField(col_name, **field_kwargs)
        else:
            fields[col_name] = StringField(col_name, **field_kwargs)

    ModelForm = type(f"{model_class.__name__}Form", (FlaskForm,), fields)
    return ModelForm(obj=obj) if obj else ModelForm()


def import_text(model_name):
    class UploadTextForm(FlaskForm):
        title = StringField("Title", validators=[DataRequired()])
        slug = StringField("Slug", validators=[DataRequired()])
        xml_file = FileField("XML File", validators=[FileRequired()])

    form = UploadTextForm()

    if form.validate_on_submit():
        slug = form.slug.data
        title = form.title.data
        xml_file = form.xml_file.data

        session = q.get_session()
        stmt = select(db.Text).filter_by(slug=slug)
        if session.scalars(stmt).first():
            flash(f"A text with slug '{slug}' already exists", "error")
            return render_template(
                "admin/upload-xml.html",
                model_name=model_name,
                form=form,
                model_configs={c.model.__name__: c for c in MODEL_CONFIG},
                models_by_category=get_models_by_category(),
            )

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".xml", delete=False
            ) as tmp_file:
                xml_file.save(tmp_file)
                tmp_path = Path(tmp_file.name)

            document = parse_document(tmp_path)
            data_utils.create_text_from_document(session, slug, title, document)

            flash(f"Successfully uploaded text '{title}' with slug '{slug}'", "success")
            return redirect(url_for("admin.list_model", model_name=model_name))

        except Exception as e:
            session.rollback()
            flash(f"Error uploading XML: {str(e)}", "error")
        finally:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()

    return render_template(
        "admin/task-import-text.html",
        model_name=model_name,
        form=form,
        model_configs={c.model.__name__: c for c in MODEL_CONFIG},
        models_by_category=get_models_by_category(),
    )


def import_parse_data(model_name):
    class UploadParseDataForm(FlaskForm):
        text_slug = StringField("Text Slug", validators=[DataRequired()])
        parse_file = FileField("Parse Data File", validators=[FileRequired()])

    form = UploadParseDataForm()

    if form.validate_on_submit():
        text_slug = form.text_slug.data
        parse_file = form.parse_file.data

        session = q.get_session()
        stmt = select(db.Text).filter_by(slug=text_slug)
        text = session.scalars(stmt).first()

        if not text:
            flash(f"Text with slug '{text_slug}' not found", "error")
            return render_template(
                "admin/upload-parse-data.html",
                model_name=model_name,
                form=form,
                model_configs={c.model.__name__: c for c in MODEL_CONFIG},
                models_by_category=get_models_by_category(),
            )

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".txt", delete=False
            ) as tmp_file:
                parse_file.save(tmp_file)
                tmp_path = Path(tmp_file.name)

            data_utils.add_parse_data(session, text_slug, tmp_path)

            flash(f"Successfully uploaded parse data for text '{text_slug}'", "success")
            return redirect(url_for("admin.list_model", model_name=model_name))

        except Exception as e:
            session.rollback()
            flash(f"Error uploading parse data: {str(e)}", "error")
        finally:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()

    return render_template(
        "admin/task-import-parse-data.html",
        model_name=model_name,
        form=form,
        model_configs={c.model.__name__: c for c in MODEL_CONFIG},
        models_by_category=get_models_by_category(),
    )


@dataclass
class ModelConfig:
    #: The model name.
    model: Any
    #: Columns that appear in list view.
    list_columns: list[str]
    #: Model category (for sidebar grouping)
    category: Category
    #: Tasks associated with the model (upload, etc.)
    tasks: list[Task] = field(default_factory=list)
    #: If set, the model can't be mutated.
    read_only: bool = False
    #: Permission required: 'admin' or 'moderator'. Defaults to 'admin'.
    permission: str = "admin"
    #: Field to display for foreign keys (e.g., 'slug', 'username'). If None, shows ID.
    display_field: str | None = None


MODEL_CONFIG = [
    ModelConfig(
        model=db.BlockParse,
        list_columns=["id", "text_id", "block_id"],
        category=Category.TEXTS,
        read_only=True,
    ),
    ModelConfig(
        model=db.BlogPost,
        list_columns=["id", "slug", "title", "author_id", "created_at"],
        category=Category.BLOG,
    ),
    ModelConfig(
        model=db.Board,
        list_columns=["id", "slug", "title"],
        category=Category.DISCUSSION,
        read_only=True,
    ),
    ModelConfig(
        model=db.ContributorInfo,
        list_columns=["id", "name", "title"],
        category=Category.SITE,
        permission="moderator",
    ),
    ModelConfig(
        model=db.Dictionary,
        list_columns=["id", "slug", "title"],
        category=Category.DICTIONARIES,
        tasks=[],
        display_field="slug",
    ),
    ModelConfig(
        model=db.DictionaryEntry,
        list_columns=["id", "dictionary_id", "key"],
        category=Category.DICTIONARIES,
        read_only=True,
    ),
    ModelConfig(
        model=db.Genre,
        list_columns=["id", "name"],
        category=Category.PROOFING,
        permission="moderator",
    ),
    ModelConfig(
        model=db.Page,
        list_columns=["id", "project_id", "slug", "order"],
        category=Category.PROOFING,
        read_only=True,
    ),
    ModelConfig(
        model=db.PageStatus,
        list_columns=["id", "name"],
        category=Category.PROOFING,
        read_only=True,
    ),
    ModelConfig(
        model=db.PasswordResetToken,
        list_columns=["id", "user_id"],
        category=Category.AUTH,
        read_only=True,
    ),
    ModelConfig(
        model=db.Post,
        list_columns=["id", "thread_id", "author_id", "created_at"],
        category=Category.DISCUSSION,
        read_only=True,
    ),
    ModelConfig(
        model=db.Project,
        list_columns=["id", "slug", "display_title", "status", "creator_id"],
        category=Category.PROOFING,
    ),
    ModelConfig(
        model=db.ProjectSponsorship,
        list_columns=["id", "sa_title", "en_title", "cost_inr"],
        category=Category.SITE,
        permission="moderator",
    ),
    ModelConfig(
        model=db.Revision,
        list_columns=["id", "page_id", "author_id", "created"],
        category=Category.PROOFING,
        read_only=True,
    ),
    ModelConfig(
        model=db.Role,
        list_columns=["id", "name"],
        category=Category.AUTH,
        read_only=True,
    ),
    ModelConfig(
        model=db.Text,
        list_columns=["id", "slug", "title"],
        category=Category.TEXTS,
        tasks=[
            Task(
                name="Import text",
                slug="import-text",
                handler=import_text,
            ),
            Task(
                name="Import parse data",
                slug="import-parse-data",
                handler=import_parse_data,
            ),
        ],
        display_field="slug",
    ),
    ModelConfig(
        model=db.TextBlock,
        list_columns=["id", "text_id", "slug", "n"],
        category=Category.TEXTS,
    ),
    ModelConfig(
        model=db.TextSection,
        list_columns=["id", "text_id", "slug", "title"],
        category=Category.TEXTS,
        read_only=True,
    ),
    ModelConfig(
        model=db.Thread,
        list_columns=["id", "board_id", "title", "created_at"],
        category=Category.DISCUSSION,
        read_only=True,
    ),
    ModelConfig(
        model=db.User,
        list_columns=["id", "username", "email", "created_at"],
        category=Category.AUTH,
        display_field="username",
    ),
]

MODELS = sorted([config.model.__name__ for config in MODEL_CONFIG])


def get_models_by_category():
    from collections import defaultdict

    by_category = defaultdict(list)
    for config in MODEL_CONFIG:
        by_category[config.category].append(config)
    for category in by_category:
        by_category[category].sort(key=lambda c: c.model.__name__)
    return dict(sorted(by_category.items(), key=lambda x: x[0].value))


def get_model_config(model_name):
    return next((c for c in MODEL_CONFIG if c.model.__name__ == model_name), None)


@bp.before_request
def check_access():
    if request.endpoint == "admin.index":
        if not current_user.is_moderator:
            abort(404)
        return

    model_name = request.view_args.get("model_name") if request.view_args else None
    if not model_name:
        abort(404)
    config = get_model_config(model_name)
    if not config:
        abort(404)

    if config.permission == "admin" and not current_user.is_admin:
        abort(404)
    if config.permission == "moderator" and not current_user.is_moderator:
        abort(404)


@bp.route("/")
def index():
    """Admin dashboard."""
    return render_template(
        "admin/index.html",
        models=MODELS,
        model_configs={c.model.__name__: c for c in MODEL_CONFIG},
        models_by_category=get_models_by_category(),
    )


@bp.route("/<model_name>/")
def list_model(model_name):
    """Model list view."""
    config = get_model_config(model_name)
    if not config:
        abort(404)

    model_class = config.model
    list_columns = config.list_columns
    tasks = config.tasks

    page = request.args.get("page", 1, type=int)
    per_page = 50

    session = q.get_session()
    query = session.query(model_class)

    total = query.count()
    items = query.limit(per_page).offset((page - 1) * per_page).all()

    total_pages = (total + per_page - 1) // per_page
    fk_map = get_foreign_key_info(model_class)

    # Build foreign key labels efficiently (single query per model type)
    fk_labels = {}
    display_fields = {
        config.model.__name__: config.display_field
        for config in MODEL_CONFIG
        if config.display_field
    }
    fk_by_model = {}
    for col, model_name in fk_map.items():
        if model_name in display_fields:
            fk_by_model.setdefault(model_name, []).append(col)
    for model_name, fk_columns in fk_by_model.items():
        display_field = display_fields[model_name]
        model_class = getattr(db, model_name)
        ids = set()
        for col in fk_columns:
            ids.update(
                [
                    getattr(item, col)
                    for item in items
                    if hasattr(item, col) and getattr(item, col) is not None
                ]
            )

        if ids:
            # One query for foreign key IDs --> label
            results = (
                session.query(model_class.id, getattr(model_class, display_field))
                .filter(model_class.id.in_(ids))
                .all()
            )

            label_map = {id_: label for id_, label in results}
            for col in fk_columns:
                fk_labels[col] = label_map

    return render_template(
        "admin/list.html",
        model_name=model_name,
        models=MODELS,
        current_model=model_name,
        list_columns=list_columns,
        items=items,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        fk_map=fk_map,
        fk_labels=fk_labels,
        tasks=tasks,
        read_only=config.read_only,
        model_configs={c.model.__name__: c for c in MODEL_CONFIG},
        models_by_category=get_models_by_category(),
    )


@bp.route("/<model_name>/create", methods=["GET", "POST"])
def create_model(model_name):
    config = get_model_config(model_name)
    if not config or config.read_only:
        abort(404)

    model_class = config.model
    form = create_model_form(model_class)

    if form.validate_on_submit():
        session = q.get_session()
        item = model_class()
        for field in form:
            if hasattr(item, field.name):
                setattr(item, field.name, field.data)

        session.add(item)
        try:
            session.commit()
            flash(f"{model_name} created successfully", "success")
            return redirect(url_for("admin.list_model", model_name=model_name))
        except (SQLAlchemyError, ValueError) as e:
            session.rollback()
            flash(f"Error creating {model_name}: {str(e)}", "error")
            # Continue to re-render the form with the error

    return render_template(
        "admin/create.html",
        model_name=model_name,
        models=MODELS,
        current_model=model_name,
        form=form,
        model_configs={c.model.__name__: c for c in MODEL_CONFIG},
        models_by_category=get_models_by_category(),
    )


@bp.route("/<model_name>/<int:item_id>/edit", methods=["GET", "POST"])
def edit_model(model_name, item_id):
    config = get_model_config(model_name)
    if not config:
        abort(404)

    model_class = config.model

    session = q.get_session()
    item = session.query(model_class).get(item_id)
    if not item:
        abort(404)

    form = create_model_form(model_class, obj=item)

    if form.validate_on_submit():
        # Don't allow POST for read-only models
        if config.read_only:
            abort(404)

        for field in form:
            if hasattr(item, field.name):
                setattr(item, field.name, field.data)

        try:
            session.commit()
            flash(f"{model_name} updated successfully", "success")
            return redirect(url_for("admin.list_model", model_name=model_name))
        except SQLAlchemyError as e:
            session.rollback()
            flash(f"Error updating {model_name}: {str(e)}", "error")
            # Continue to re-render the form with the error

    return render_template(
        "admin/edit.html",
        model_name=model_name,
        models=MODELS,
        current_model=model_name,
        form=form,
        item=item,
        item_id=item_id,
        read_only=config.read_only,
        model_configs={c.model.__name__: c for c in MODEL_CONFIG},
        models_by_category=get_models_by_category(),
    )


@bp.route("/<model_name>/<int:item_id>/delete", methods=["POST"])
def delete_model(model_name, item_id):
    config = get_model_config(model_name)
    if not config:
        abort(404)

    model_class = config.model

    session = q.get_session()
    item = session.query(model_class).get(item_id)
    if not item:
        abort(404)

    try:
        session.delete(item)
        session.commit()
        flash(f"{model_name} deleted successfully", "success")
    except SQLAlchemyError as e:
        session.rollback()
        flash(f"Error deleting {model_name}: {str(e)}", "error")

    return redirect(url_for("admin.list_model", model_name=model_name))


@bp.route("/<model_name>/task/<task_slug>", methods=["GET", "POST"])
def run_task(model_name, task_slug):
    config = get_model_config(model_name)
    if not config:
        abort(404)

    task = next((t for t in config.tasks if t.slug == task_slug), None)
    if not task:
        abort(404)

    return task.handler(model_name=model_name)
