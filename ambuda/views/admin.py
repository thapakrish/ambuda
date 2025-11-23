from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from flask import Blueprint, render_template, abort, request, redirect, url_for, flash
from flask_login import current_user
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
from wtforms.validators import Optional

import ambuda.database as db
import ambuda.queries as q

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

    ModelForm = type(f"{model_class.__name__}Form", (Form,), fields)
    return ModelForm(obj=obj) if obj else ModelForm()


@dataclass
class ModelConfig:
    #: The model name.
    model: Any
    #: Columns that appear in list view.
    list_columns: list[str]
    #: Meaningfully sortable columns. (Unused)
    searchable_columns: list[str]
    #: Model category (for sidebar grouping)
    category: Category
    #: Tasks associated with the model (upload, etc.)
    tasks: list[Task] = field(default_factory=list)
    #: If set, the model can't be mutated.
    read_only: bool = False
    #: Permission required: 'admin' or 'moderator'. Defaults to 'admin'.
    permission: str = "admin"


MODEL_CONFIG = [
    ModelConfig(
        model=db.BlockParse,
        list_columns=["id", "block_id"],
        searchable_columns=[],
        category=Category.TEXTS,
        read_only=True,
    ),
    ModelConfig(
        model=db.BlogPost,
        list_columns=["id", "slug", "title", "author_id", "created_at"],
        searchable_columns=["slug", "title"],
        category=Category.BLOG,
        read_only=True,
    ),
    ModelConfig(
        model=db.Board,
        list_columns=["id", "slug", "title"],
        searchable_columns=["slug", "title"],
        category=Category.DISCUSSION,
        read_only=True,
    ),
    ModelConfig(
        model=db.ContributorInfo,
        list_columns=["id", "sa_title", "title"],
        searchable_columns=["sa_title", "title"],
        category=Category.SITE,
        permission="moderator",
    ),
    ModelConfig(
        model=db.Dictionary,
        list_columns=["id", "slug", "title"],
        searchable_columns=["slug", "title"],
        category=Category.DICTIONARIES,
        tasks=[],
    ),
    ModelConfig(
        model=db.DictionaryEntry,
        list_columns=["id", "dictionary_id", "key"],
        searchable_columns=["key"],
        category=Category.DICTIONARIES,
        read_only=True,
    ),
    ModelConfig(
        model=db.Genre,
        list_columns=["id", "name"],
        searchable_columns=["name"],
        category=Category.PROOFING,
        permission="moderator",
    ),
    ModelConfig(
        model=db.Page,
        list_columns=["id", "project_id", "slug", "order"],
        searchable_columns=["slug"],
        category=Category.PROOFING,
        read_only=True,
    ),
    ModelConfig(
        model=db.PageStatus,
        list_columns=["id", "name"],
        searchable_columns=["name"],
        category=Category.PROOFING,
        read_only=True,
    ),
    ModelConfig(
        model=db.PasswordResetToken,
        list_columns=["id", "user_id", "created_at"],
        searchable_columns=[],
        category=Category.AUTH,
        read_only=True,
    ),
    ModelConfig(
        model=db.Post,
        list_columns=["id", "thread_id", "author_id", "created_at"],
        searchable_columns=[],
        category=Category.DISCUSSION,
        read_only=True,
    ),
    ModelConfig(
        model=db.Project,
        list_columns=["id", "slug", "display_title", "status", "creator_id"],
        searchable_columns=["slug", "display_title"],
        category=Category.PROOFING,
    ),
    ModelConfig(
        model=db.ProjectSponsorship,
        list_columns=["id", "sa_title", "en_title", "cost_inr"],
        searchable_columns=["sa_title", "en_title"],
        category=Category.SITE,
        permission="moderator",
    ),
    ModelConfig(
        model=db.Revision,
        list_columns=["id", "page_id", "author_id", "created_at"],
        searchable_columns=[],
        category=Category.PROOFING,
        read_only=True,
    ),
    ModelConfig(
        model=db.Role,
        list_columns=["id", "name", "description"],
        searchable_columns=["name"],
        category=Category.AUTH,
        read_only=True,
    ),
    ModelConfig(
        model=db.Text,
        list_columns=["id", "slug", "title"],
        searchable_columns=["slug", "title"],
        category=Category.TEXTS,
    ),
    ModelConfig(
        model=db.TextBlock,
        list_columns=["id", "text_id", "slug", "number"],
        searchable_columns=["slug"],
        category=Category.TEXTS,
    ),
    ModelConfig(
        model=db.TextSection,
        list_columns=["id", "text_id", "slug", "title"],
        searchable_columns=["slug", "title"],
        category=Category.TEXTS,
        read_only=True,
    ),
    ModelConfig(
        model=db.Thread,
        list_columns=["id", "board_id", "title", "created_at"],
        searchable_columns=["title"],
        category=Category.DISCUSSION,
        read_only=True,
    ),
    ModelConfig(
        model=db.User,
        list_columns=["id", "username", "email", "created_at"],
        searchable_columns=["username", "email"],
        category=Category.AUTH,
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

    if request.method == "POST":
        form = create_model_form(model_class)
        form.process(request.form)

        if form.validate():
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
    else:
        form = create_model_form(model_class)

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

    if request.method == "POST":
        # Don't allow POST for read-only models
        if config.read_only:
            abort(404)

        form = create_model_form(model_class)
        form.process(request.form)

        if form.validate():
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
    else:
        form = create_model_form(model_class, obj=item)

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


@bp.route("/<model_name>/task/<task_slug>", methods=["GET", "POST"])
def run_task(model_name, task_slug):
    config = get_model_config(model_name)
    if not config:
        abort(404)

    task = next((t for t in config.tasks if t.slug == task_slug), None)
    if not task:
        abort(404)

    return task.handler(model_name=model_name)
