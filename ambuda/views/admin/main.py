"""Ambuda admin interface."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from flask import (
    Blueprint,
    render_template,
    abort,
    request,
    redirect,
    url_for,
    flash,
)
from flask_login import current_user
from flask_wtf import FlaskForm
from sqlalchemy import inspect, Text, JSON
from sqlalchemy.exc import SQLAlchemyError
from wtforms import (
    Form,
    StringField,
    IntegerField,
    TextAreaField,
    BooleanField,
    DateTimeField,
    DateTimeLocalField,
    SelectField,
    SelectMultipleField,
)
from wtforms.validators import Optional

import ambuda.database as db
import ambuda.queries as q
from ambuda.views.admin import tasks


bp = Blueprint("admin", __name__)
FK_DROPDOWN_LIMIT = 500


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
    """An adhoc task associated with some model."""

    #: The display name of the task.
    name: str
    #: The URL name of the task.
    slug: str
    #: The function to call.
    handler: Callable


@dataclass
class ModelConfig:
    """Defines how to display a model in the admin UI."""

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
        model=db.Author,
        list_columns=["id", "name"],
        category=Category.TEXTS,
        display_field="name",
    ),
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
        tasks=[
            Task(
                name="Import dictionaries",
                slug="import-dictionaries",
                handler=tasks.import_dictionaries,
            ),
        ],
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
        list_columns=["id", "slug", "display_title", "creator_id"],
        category=Category.PROOFING,
        tasks=[
            Task(
                name="Import projects",
                slug="import-projects",
                handler=tasks.import_projects,
            ),
            Task(
                name="Export projects",
                slug="export-projects",
                handler=tasks.export_projects,
            ),
        ],
        display_field="slug",
    ),
    ModelConfig(
        model=db.ProjectSponsorship,
        list_columns=["id", "sa_title", "en_title", "cost_inr"],
        category=Category.SITE,
        permission="moderator",
    ),
    ModelConfig(
        model=db.Revision,
        list_columns=["id", "page_id", "author_id", "created_at"],
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
                name="Import texts",
                slug="import-text",
                handler=tasks.import_text,
            ),
            Task(
                name="Import parse data",
                slug="import-parse-data",
                handler=tasks.import_parse_data,
            ),
            Task(
                name="Import metadata",
                slug="import-metadata",
                handler=tasks.import_metadata,
            ),
            Task(
                name="Export metadata",
                slug="export-metadata",
                handler=tasks.export_metadata,
            ),
            Task(
                name="Add genre",
                slug="add-genre",
                handler=tasks.add_genre_to_texts,
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


def get_many_to_many_info(model_class):
    mapper = inspect(model_class)
    m2m_info = {}

    for relationship in mapper.relationships:
        if relationship.secondary is not None:
            target_model = relationship.mapper.class_
            m2m_info[relationship.key] = target_model

    return m2m_info


def populate_model_attributes_from_form(obj, form, model_class):
    from datetime import datetime

    m2m_info = get_many_to_many_info(model_class)

    for field in form:
        if field.name in m2m_info:
            continue
        if hasattr(obj, field.name):
            value = field.data
            if field.type in ("DateTimeField", "DateTimeLocalField") and isinstance(
                value, str
            ):
                for fmt in [
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f",
                ]:
                    try:
                        value = datetime.strptime(value, fmt)
                        break
                    except (ValueError, TypeError):
                        continue
                else:
                    value = None
            setattr(obj, field.name, value)


def populate_model_m2m_from_form(obj, form, model_class, session):
    m2m_info = get_many_to_many_info(model_class)

    for rel_name, target_model_class in m2m_info.items():
        if not hasattr(form, rel_name):
            continue

        selected_ids = form[rel_name].data
        # Filter out empty strings and convert to integers
        id_list = [int(id_str) for id_str in selected_ids if id_str]

        # Fetch all related items in a single query
        if id_list:
            related_items = (
                session.query(target_model_class)
                .filter(target_model_class.id.in_(id_list))
                .all()
            )
        else:
            related_items = []

        setattr(obj, rel_name, related_items)


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
                choices = []
                if column.nullable:
                    choices.append(("", "-- None --"))
                for item in (
                    session.query(target_model_class).limit(FK_DROPDOWN_LIMIT).all()
                ):
                    choices.append((item.id, str(item)))

                def coerce_int_or_none(x):
                    if x == "" or x is None:
                        return None
                    return int(x)

                fields[col_name] = SelectField(
                    col_name,
                    choices=choices,
                    coerce=coerce_int_or_none,
                    **field_kwargs,
                )
                continue

        python_type = col_type.python_type
        if isinstance(col_type, JSON):
            fields[col_name] = TextAreaField(
                col_name,
                render_kw={"style": "font-family: monospace;", "rows": 10},
                **field_kwargs,
            )
        elif python_type == int:
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
            from datetime import datetime, date

            if python_type in (datetime, date):
                fields[col_name] = DateTimeLocalField(
                    col_name, format="%Y-%m-%dT%H:%M:%S", **field_kwargs
                )
            else:
                fields[col_name] = StringField(col_name, **field_kwargs)

    m2m_info = get_many_to_many_info(model_class)
    session = q.get_session()

    for rel_name, target_model_class in m2m_info.items():
        choices = []
        for item in session.query(target_model_class).limit(200).all():
            choices.append((str(item.id), str(item)))

        default = []
        if obj:
            related_items = getattr(obj, rel_name, [])
            default = [str(item.id) for item in related_items]

        fields[rel_name] = SelectMultipleField(
            rel_name,
            choices=choices,
            default=default,
            render_kw={"size": "5"},
        )

    ModelForm = type(f"{model_class.__name__}Form", (FlaskForm,), fields)
    form = ModelForm(obj=obj) if obj else ModelForm()

    # Set manually for m2m fields since these aren't present as attributes on `obj`.
    # Only do this on GET requests - on POST, the form is populated from request data
    if obj and request.method == "GET":
        for rel_name in m2m_info.keys():
            if hasattr(form, rel_name):
                related_items = getattr(obj, rel_name, [])
                getattr(form, rel_name).data = [str(item.id) for item in related_items]

    return form


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
    else:
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
    for col, fk_model_name in fk_map.items():
        if fk_model_name in display_fields:
            fk_by_model.setdefault(fk_model_name, []).append(col)
    for fk_model_name, fk_columns in fk_by_model.items():
        display_field = display_fields[fk_model_name]
        fk_model_class = getattr(db, fk_model_name)
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
                session.query(fk_model_class.id, getattr(fk_model_class, display_field))
                .filter(fk_model_class.id.in_(ids))
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
    fk_map = get_foreign_key_info(model_class)

    if form.validate_on_submit():
        session = q.get_session()
        item = model_class()

        populate_model_attributes_from_form(item, form, model_class)
        session.add(item)

        # Flush to get the ID for many-to-many.
        try:
            session.flush()
        except (SQLAlchemyError, ValueError) as e:
            session.rollback()
            flash(f"Error creating {model_name}: {str(e)}", "error")
            return render_template(
                "admin/create.html",
                model_name=model_name,
                models=MODELS,
                current_model=model_name,
                form=form,
                fk_map=fk_map,
                model_configs={c.model.__name__: c for c in MODEL_CONFIG},
                models_by_category=get_models_by_category(),
            )

        populate_model_m2m_from_form(item, form, model_class, session)

        try:
            session.commit()
            flash(f"{model_name} created successfully", "success")
            return redirect(url_for("admin.list_model", model_name=model_name))
        except (SQLAlchemyError, ValueError) as e:
            session.rollback()
            flash(f"Error creating {model_name}: {str(e)}", "error")

    return render_template(
        "admin/create.html",
        model_name=model_name,
        models=MODELS,
        current_model=model_name,
        form=form,
        fk_map=fk_map,
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
    item = session.get(model_class, item_id)
    if not item:
        abort(404)

    form = create_model_form(model_class, obj=item)
    fk_map = get_foreign_key_info(model_class)

    if form.validate_on_submit():
        if config.read_only:
            abort(404)

        populate_model_attributes_from_form(item, form, model_class)
        populate_model_m2m_from_form(item, form, model_class, session)

        try:
            session.commit()
            flash(f"{model_name} updated successfully", "success")
            return redirect(url_for("admin.list_model", model_name=model_name))
        except (SQLAlchemyError, ValueError) as e:
            session.rollback()
            flash(f"Error updating {model_name}: {str(e)}", "error")

    return render_template(
        "admin/edit.html",
        model_name=model_name,
        models=MODELS,
        current_model=model_name,
        form=form,
        item=item,
        item_id=item_id,
        read_only=config.read_only,
        fk_map=fk_map,
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
    item = session.get(model_class, item_id)
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

    selected_ids = request.form.getlist("selected_ids")
    return task.handler(model_name=model_name, selected_ids=selected_ids)
