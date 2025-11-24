import json
import tempfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from flask import (
    Blueprint,
    render_template,
    abort,
    request,
    redirect,
    url_for,
    flash,
    jsonify,
    make_response,
)
from flask_login import current_user
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired, MultipleFileField
from sqlalchemy import inspect, Text, JSON
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
                choices = [("", "-- None --")] if column.nullable else []
                for item in session.query(target_model_class).limit(100).all():
                    label = str(
                        getattr(item, "slug", None)
                        or getattr(item, "name", None)
                        or getattr(item, "title", None)
                        or getattr(item, "id")
                    )
                    choices.append((item.id, label))

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

        # Check if this is a JSON field
        if isinstance(col_type, JSON):
            fields[col_name] = TextAreaField(
                col_name,
                render_kw={"style": "font-family: monospace;", "rows": 10},
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
        xml_files = MultipleFileField("XML Files", validators=[FileRequired()])

    form = UploadTextForm()

    if form.validate_on_submit():
        xml_files = form.xml_files.data
        session = q.get_session()

        success_count = 0
        error_count = 0
        errors = []

        for index, xml_file in enumerate(xml_files):
            filename = xml_file.filename
            if not filename.endswith(".xml"):
                errors.append(f"{filename}: Must be an XML file")
                error_count += 1
                continue

            # Get slug and title from form data
            slug = request.form.get(f"slug_{index}", "").strip()
            title = request.form.get(f"title_{index}", "").strip()

            # Validate that slug and title are provided
            if not slug:
                errors.append(f"{filename}: Slug is required")
                error_count += 1
                continue
            if not title:
                errors.append(f"{filename}: Title is required")
                error_count += 1
                continue

            # Check if text already exists
            stmt = select(db.Text).filter_by(slug=slug)
            if session.scalars(stmt).first():
                errors.append(f"{filename}: A text with slug '{slug}' already exists")
                error_count += 1
                continue

            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb", suffix=".xml", delete=False
                ) as tmp_file:
                    xml_file.save(tmp_file)
                    tmp_path = Path(tmp_file.name)

                document = parse_document(tmp_path)
                data_utils.create_text_from_document(session, slug, title, document)
                success_count += 1

            except Exception as e:
                session.rollback()
                errors.append(f"{filename}: {str(e)}")
                error_count += 1
            finally:
                if tmp_path and tmp_path.exists():
                    tmp_path.unlink()

        # Display summary
        if success_count > 0:
            flash(f"Successfully uploaded {success_count} text(s)", "success")
        if error_count > 0:
            flash(
                f"{error_count} error(s): {'; '.join(errors[:5])}{'...' if len(errors) > 5 else ''}",
                "error",
            )

        if success_count > 0 or error_count > 0:
            return redirect(url_for("admin.list_model", model_name=model_name))

    return render_template(
        "admin/task-import-text.html",
        model_name=model_name,
        form=form,
        model_configs={c.model.__name__: c for c in MODEL_CONFIG},
        models_by_category=get_models_by_category(),
    )


def import_parse_data(model_name):
    class UploadParseDataForm(FlaskForm):
        parse_files = MultipleFileField("Parse Data Files", validators=[FileRequired()])

    form = UploadParseDataForm()

    if form.validate_on_submit():
        parse_files = form.parse_files.data
        session = q.get_session()

        success_count = 0
        error_count = 0
        errors = []

        for parse_file in parse_files:
            # Derive text slug from filename (e.g., "bhagavad-gita.txt" -> "bhagavad-gita")
            filename = parse_file.filename
            if not filename.endswith(".txt"):
                errors.append(f"{filename}: Must be a .txt file")
                error_count += 1
                continue

            text_slug = filename[:-4]  # Remove .txt extension

            # Check if text exists
            stmt = select(db.Text).filter_by(slug=text_slug)
            text = session.scalars(stmt).first()

            if not text:
                errors.append(f"{filename}: Text with slug '{text_slug}' not found")
                error_count += 1
                continue

            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb", suffix=".txt", delete=False
                ) as tmp_file:
                    parse_file.save(tmp_file)
                    tmp_path = Path(tmp_file.name)

                data_utils.add_parse_data(session, text_slug, tmp_path)
                success_count += 1

            except Exception as e:
                session.rollback()
                errors.append(f"{filename}: {str(e)}")
                error_count += 1
            finally:
                if tmp_path and tmp_path.exists():
                    tmp_path.unlink()

        # Display summary
        if success_count > 0:
            flash(
                f"Successfully uploaded parse data for {success_count} text(s)",
                "success",
            )
        if error_count > 0:
            flash(
                f"{error_count} error(s): {'; '.join(errors[:5])}{'...' if len(errors) > 5 else ''}",
                "error",
            )

        if success_count > 0 or error_count > 0:
            return redirect(url_for("admin.list_model", model_name=model_name))

    return render_template(
        "admin/task-import-parse-data.html",
        model_name=model_name,
        form=form,
        model_configs={c.model.__name__: c for c in MODEL_CONFIG},
        models_by_category=get_models_by_category(),
    )


def add_genre_to_texts(model_name):
    """Batch action to add a genre to multiple texts."""

    class AddGenreForm(FlaskForm):
        genre_id = SelectField("Genre", coerce=int, validators=[DataRequired()])

    session = q.get_session()
    genres = session.query(db.Genre).order_by(db.Genre.name).all()

    form = AddGenreForm()
    form.genre_id.choices = [(g.id, g.name) for g in genres]
    selected_ids = request.form.getlist("selected_ids")

    if not selected_ids:
        flash("No texts selected", "error")
        return redirect(url_for("admin.list_model", model_name=model_name))

    if form.validate_on_submit():
        genre_id = form.genre_id.data

        try:
            updated_count = 0
            for text_id in selected_ids:
                text = session.query(db.Text).get(int(text_id))
                if text:
                    text.genre_id = genre_id
                    updated_count += 1

            session.commit()
            genre_name = session.query(db.Genre).get(genre_id).name
            flash(
                f"Successfully added genre '{genre_name}' to {updated_count} text(s)",
                "success",
            )
            return redirect(url_for("admin.list_model", model_name=model_name))

        except Exception as e:
            session.rollback()
            flash(f"Error adding genre: {str(e)}", "error")

    texts = []
    for text_id in selected_ids:
        text = session.query(db.Text).get(int(text_id))
        if text:
            texts.append(text)

    return render_template(
        "admin/task-add-genre.html",
        model_name=model_name,
        form=form,
        texts=texts,
        selected_ids=selected_ids,
        model_configs={c.model.__name__: c for c in MODEL_CONFIG},
        models_by_category=get_models_by_category(),
    )


def import_metadata(model_name):
    """Import text metadata from a JSON file."""

    class UploadMetadataForm(FlaskForm):
        json_file = FileField("JSON File", validators=[FileRequired()])

    form = UploadMetadataForm()

    if form.validate_on_submit():
        json_file = form.json_file.data

        session = q.get_session()
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".json", delete=False
            ) as tmp_file:
                json_file.save(tmp_file)
                tmp_path = Path(tmp_file.name)

            updated_count, not_found_slugs = data_utils.import_text_metadata(
                session, tmp_path
            )

            if not_found_slugs:
                flash(
                    f"Updated {updated_count} text(s). Warning: {len(not_found_slugs)} slug(s) not found: {', '.join(not_found_slugs[:5])}{'...' if len(not_found_slugs) > 5 else ''}",
                    "warning",
                )
            else:
                flash(f"Successfully updated {updated_count} text(s)", "success")

            return redirect(url_for("admin.list_model", model_name=model_name))

        except Exception as e:
            session.rollback()
            flash(f"Error importing metadata: {str(e)}", "error")
        finally:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()

    return render_template(
        "admin/task-import-metadata.html",
        model_name=model_name,
        form=form,
        model_configs={c.model.__name__: c for c in MODEL_CONFIG},
        models_by_category=get_models_by_category(),
    )


def export_metadata(model_name):
    """Export Text metadata as JSON."""
    session = q.get_session()

    texts = session.query(db.Text).all()
    export_data = []
    for text in texts:
        text_dict = {
            "slug": text.slug,
            "title": text.title,
            "header": text.header,
            "config": json.loads(text.config) if text.config else None,
            "genre": text.genre.name if text.genre else None,
        }
        export_data.append(text_dict)

    response = make_response(jsonify(export_data))
    response.headers["Content-Disposition"] = "attachment; filename=texts_metadata.json"
    response.headers["Content-Type"] = "application/json"

    return response


def import_dictionaries(model_name):
    """Import dictionaries from XML files."""

    class UploadDictionaryForm(FlaskForm):
        xml_files = MultipleFileField("XML Files", validators=[FileRequired()])

    form = UploadDictionaryForm()

    if form.validate_on_submit():
        xml_files = form.xml_files.data
        session = q.get_session()

        success_count = 0
        error_count = 0
        errors = []
        total_entries = 0

        for index, xml_file in enumerate(xml_files):
            filename = xml_file.filename
            if not filename.endswith(".xml"):
                errors.append(f"{filename}: Must be an XML file")
                error_count += 1
                continue

            slug = request.form.get(f"slug_{index}", "").strip()
            title = request.form.get(f"title_{index}", "").strip()

            if not slug:
                errors.append(f"{filename}: Slug is required")
                error_count += 1
                continue
            if not title:
                errors.append(f"{filename}: Title is required")
                error_count += 1
                continue

            session = q.get_session()
            stmt = select(db.Dictionary).filter_by(slug=slug)
            dictionary = session.scalars(stmt).first()
            if dictionary:
                errors.append(
                    f"{filename}: A dictionary with slug '{slug}' already exists"
                )
                error_count += 1
                continue

            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb", suffix=".xml", delete=False
                ) as tmp_file:
                    xml_file.save(tmp_file)
                    tmp_path = Path(tmp_file.name)

                entry_count = data_utils.import_dictionary_from_xml(
                    slug=slug, title=title, path=tmp_path
                )
                total_entries += entry_count
                success_count += 1

            except Exception as e:
                session.rollback()
                errors.append(f"{filename}: {str(e)}")
                error_count += 1
            finally:
                if tmp_path and tmp_path.exists():
                    tmp_path.unlink()

        # Display summary
        if success_count > 0:
            flash(
                f"Successfully imported {success_count} dictionar{'ies' if success_count > 1 else 'y'} ({total_entries} entries)",
                "success",
            )
        if error_count > 0:
            flash(
                f"{error_count} error(s): {'; '.join(errors[:5])}{'...' if len(errors) > 5 else ''}",
                "error",
            )

        if success_count > 0 or error_count > 0:
            return redirect(url_for("admin.list_model", model_name=model_name))

    return render_template(
        "admin/task-import-dictionary.html",
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
        tasks=[
            Task(
                name="Import dictionaries",
                slug="import-dictionaries",
                handler=import_dictionaries,
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
                name="Import texts",
                slug="import-text",
                handler=import_text,
            ),
            Task(
                name="Import parse data",
                slug="import-parse-data",
                handler=import_parse_data,
            ),
            Task(
                name="Add genre",
                slug="add-genre",
                handler=add_genre_to_texts,
            ),
            Task(
                name="Export metadata",
                slug="export-metadata",
                handler=export_metadata,
            ),
            Task(
                name="Import metadata",
                slug="import-metadata",
                handler=import_metadata,
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
        except (SQLAlchemyError, ValueError) as e:
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
