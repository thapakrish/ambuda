"""Views related to texts: title pages, sections, verses, etc."""

import json
import os

from flask import (
    Blueprint,
    Response,
    abort,
    jsonify,
    request,
    render_template,
    url_for,
    send_file,
)
from flask_login import current_user, login_required
from vidyut.lipi import transliterate, Scheme

import ambuda.database as db
import ambuda.queries as q
from ambuda.consts import TEXT_CATEGORIES
from ambuda.models.texts import TextConfig
from ambuda.utils import text_utils
from ambuda.utils import text_exports
from ambuda.utils.text_exports import ExportType, EXPORTS
from ambuda.utils import xml
from ambuda.utils.json_serde import AmbudaJSONEncoder
from ambuda.utils.text_quality import validate
from ambuda.views.api import bp as api
from ambuda.views.reader.schema import Block, Section
from ambuda.s3_utils import S3Path
from flask import current_app
from sqlalchemy import select

bp = Blueprint("texts", __name__)

# A hacky list that decides which texts have parse data.
HAS_NO_PARSE = {
    "raghuvamsham",
    "bhattikavyam",
    "shatakatrayam",
    "shishupalavadham",
    "shivopanishat",
    "catuhshloki",
}

#: A special slug for single-section texts.
#:
#: Some texts are small enough that they don't have any divisions (sargas,
#: kandas). For simplicity, we represent such texts as having one section that
#: we just call "all." All such texts are called *single-section texts.*
SINGLE_SECTION_SLUG = "all"


def _prev_cur_next(sections: list[db.TextSection], slug: str):
    """Get the previous, current, and next esctions.

    :param sections: all of the sections in this text.
    :param slug: the slug for the current section.
    """
    found = False
    i = 0
    for i, s in enumerate(sections):
        if s.slug == slug:
            found = True
            break

    if not found:
        raise ValueError(f"Unknown slug {slug}")

    prev = sections[i - 1] if i > 0 else None
    cur = sections[i]
    next = sections[i + 1] if i < len(sections) - 1 else None
    return prev, cur, next


def _make_section_url(text: db.Text, section: db.TextSection | None) -> str | None:
    if section:
        return url_for("texts.section", text_slug=text.slug, section_slug=section.slug)
    else:
        return None


def _hk_to_dev(s: str) -> str:
    return transliterate(s, Scheme.HarvardKyoto, Scheme.Devanagari)


@bp.route("/")
def index():
    """Show all texts."""

    text_entries = text_utils.create_text_entries()
    return render_template("texts/index.html", text_entries=text_entries)


@bp.route("/<slug>/")
def text(slug):
    """Show a text's title page and contents."""
    text = q.text(slug)
    if text is None:
        abort(404)
    assert text

    try:
        config = TextConfig.model_validate_json(text.config)
    except Exception:
        config = TextConfig()

    prefix_titles = config.titles.fixed

    section_groups = {}
    for section in text.sections:
        key, _, _ = section.slug.rpartition(".")
        if key not in section_groups:
            section_groups[key] = []

        name = section.slug
        if section.slug.count(".") == 1:
            x, y = section.slug.split(".")
            # NOTE: experimental -- metadata paths may move at any time.
            try:
                pattern = config.titles.patterns["x.y"]
            except Exception:
                pattern = None
            if pattern:
                name = pattern.format(x=x, y=y)
        section_groups[key].append((section.slug, name))

    return render_template(
        "texts/text.html",
        text=text,
        section_groups=section_groups,
        prefix_titles=prefix_titles,
    )


@bp.route("/<slug>/about")
def text_about(slug):
    """Show a text's metadata."""
    text = q.text(slug)
    if text is None:
        abort(404)
    assert text

    header_data = xml.parse_tei_header(text.header)
    return render_template(
        "texts/text-about.html",
        text=text,
        header=header_data,
    )


@bp.route("/<slug>/resources")
def text_resources(slug):
    """Show a text's downloadable resources."""
    text = q.text(slug)
    if text is None:
        abort(404)
    assert text

    def _key_fn(x: db.TextExport) -> tuple:
        if x.slug.endswith("txt"):
            return (0, x.slug)
        if x.slug.endswith("xml"):
            return (1, x.slug)
        if x.slug.endswith("pdf"):
            return (2, x.slug)
        if x.slug.endswith("csv"):
            return (3, x.slug)
        return (4, x.slug)

    exports = sorted(text.exports, key=_key_fn)
    return render_template("texts/text-resources.html", text=text, exports=exports)


@bp.route("/<slug>/validate")
def validate_text(slug):
    text = q.text(slug)
    if text is None or not text.supports_text_export:
        abort(404)
    assert text

    report = validate(text)
    return render_template("texts/text-validate.html", text=text, report=report)


@bp.route("/downloads/")
def downloads():
    """Show all available downloads."""
    with q.get_session() as session:
        stmt = select(db.TextExport).order_by(db.TextExport.slug)
        exports = list(session.execute(stmt).scalars())

    return render_template("texts/downloads.html", exports=exports)


@bp.route("/downloads/<filename>")
def download_file(filename):
    text_export = q.text_export(filename)
    if not text_export:
        abort(404)
    assert text_export

    export_config = text_export.export_config
    if export_config is None:
        abort(404)
    assert export_config

    # Check cache first
    cache = current_app.cache
    cache_key = f"text_export:{filename}"
    cached_path = cache.get(cache_key)

    if cached_path and os.path.exists(cached_path):
        file_path = cached_path
    else:
        s3_path = S3Path.from_path(text_export.s3_path)
        cache_dir = current_app.config.get("CACHE_DIR", "/tmp/ambuda-cache")
        os.makedirs(cache_dir, exist_ok=True)

        file_path = os.path.join(cache_dir, filename)
        s3_path.download_file(file_path)
        cache.set(cache_key, file_path, timeout=0)

    return send_file(
        file_path,
        download_name=filename,
        mimetype=export_config.mime_type,
    )


@bp.route("/<text_slug>/<section_slug>")
def section(text_slug, section_slug):
    """Show a specific section of a text."""
    text_ = q.text(text_slug)
    if text_ is None:
        abort(404)
    assert text_

    try:
        prev, cur, next_ = _prev_cur_next(text_.sections, section_slug)
    except ValueError:
        abort(404)

    is_single_section_text = not prev and not next_
    if is_single_section_text:
        # Single-section texts have exactly one section whose slug should be
        # `SINGLE_SECTION_SLUG`. If the slug is anything else, abort.
        if section_slug != SINGLE_SECTION_SLUG:
            abort(404)

    has_no_parse = text_.slug in HAS_NO_PARSE

    # Fetch with content blocks
    cur = q.text_section(text_.id, section_slug)

    # TODO: this sucks
    with q.get_session() as _:
        _ = cur.blocks
        for block in cur.blocks:
            _ = block.page
            if block.page:
                _ = block.page.project
            # Eagerly load parent relationships if this is a child text
            if text_.parent_id:
                _ = block.parents
                for parent_block in block.parents:
                    _ = parent_block.page
                    if parent_block.page:
                        _ = parent_block.page.project

    blocks = []
    for block in cur.blocks:
        page = block.page
        page_url = None
        if page:
            page_url = url_for(
                "proofing.page.edit",
                project_slug=page.project.slug,
                page_slug=page.slug,
            )

        # Fetch parent blocks if this text has a parent
        parent_blocks = None
        if text_.parent_id and block.parents:
            parent_blocks = []
            for parent_block in block.parents:
                parent_page = parent_block.page
                parent_page_url = None
                if parent_page:
                    parent_page_url = url_for(
                        "proofing.page.edit",
                        project_slug=parent_page.project.slug,
                        page_slug=parent_page.slug,
                    )
                parent_blocks.append(
                    Block(
                        slug=parent_block.slug,
                        mula=xml.transform_text_block(parent_block.xml),
                        page_url=parent_page_url,
                    )
                )

        # HACK: skip these for now.
        if block.xml.startswith("<title") or block.xml.startswith("<subtitle"):
            continue

        blocks.append(
            Block(
                slug=block.slug,
                mula=xml.transform_text_block(block.xml),
                page_url=page_url,
                parent_blocks=parent_blocks,
            )
        )

    data = Section(
        text_title=_hk_to_dev(text_.title),
        section_title=_hk_to_dev(cur.title),
        blocks=blocks,
        prev_url=_make_section_url(text_, prev),
        next_url=_make_section_url(text_, next_),
    )
    json_payload = json.dumps(data, cls=AmbudaJSONEncoder)

    return render_template(
        "texts/section.html",
        text=text_,
        prev=prev,
        section=cur,
        next=next_,
        json_payload=json_payload,
        html_blocks=data.blocks,
        has_no_parse=has_no_parse,
        is_single_section_text=is_single_section_text,
    )


@api.route("/texts/<text_slug>/blocks/<block_slug>")
def block_htmx(text_slug, block_slug):
    text = q.text(text_slug)
    if text is None:
        abort(404)
    assert text

    block = q.block(text.id, block_slug)
    if not block:
        abort(404)
    assert block

    html_block = xml.transform_text_block(block.xml)
    return render_template(
        "htmx/text-block.html",
        slug=block.slug,
        html=html_block,
    )


@api.route("/texts/<text_slug>/<section_slug>")
def reader_json(text_slug, section_slug):
    # NOTE: currently unused, since we bootstrap from a JSON blob in the
    # original request.
    text_ = q.text(text_slug)
    if text_ is None:
        abort(404)
    assert text_

    try:
        prev, cur, next_ = _prev_cur_next(text_.sections, section_slug)
    except ValueError:
        abort(404)

    with q.get_session() as _:
        html_blocks = [xml.transform_text_block(b.xml) for b in cur.blocks]

    data = Section(
        text_title=_hk_to_dev(text_.title),
        section_title=_hk_to_dev(cur.title),
        blocks=html_blocks,
        prev_url=_make_section_url(text, prev),
        next_url=_make_section_url(text, next_),
    )
    return jsonify(data)


@api.route("/bookmarks/toggle", methods=["POST"])
def toggle_bookmark():
    """Toggle a bookmark on a text block."""

    if not current_user.is_authenticated:
        return jsonify({"error": "Authentication required"}), 401

    data = request.get_json()
    block_slug = data.get("block_slug")

    if not block_slug:
        return jsonify({"error": "block_slug is required"}), 400

    session = q.get_session()

    block = session.scalar(select(db.TextBlock).where(db.TextBlock.slug == block_slug))
    if not block:
        return jsonify({"error": "Block not found"}), 404

    existing_bookmark = session.scalar(
        select(db.TextBlockBookmark).where(
            db.TextBlockBookmark.user_id == current_user.id,
            db.TextBlockBookmark.block_id == block.id,
        )
    )

    if existing_bookmark:
        session.delete(existing_bookmark)
        session.commit()
        return jsonify({"bookmarked": False, "block_slug": block_slug})
    else:
        bookmark = db.TextBlockBookmark(
            user_id=current_user.id,
            block_id=block.id,
        )
        session.add(bookmark)
        session.commit()
        return jsonify({"bookmarked": True, "block_slug": block_slug})
