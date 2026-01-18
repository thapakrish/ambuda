import os

import sqlalchemy as sqla
from flask import (
    Blueprint,
    abort,
    current_app,
    render_template,
    request,
    flash,
    redirect,
    url_for,
)
from flask_login import login_required, current_user
from pydantic import BaseModel
from vidyut.lipi import transliterate, Scheme

import ambuda.queries as q
from ambuda import database as db
from ambuda.utils import xml, revisions
from ambuda.tasks import tagging as tagging_tasks
from ambuda.views.proofing.decorators import p2_required


bp = Blueprint("tagging", __name__)


class TokenData(BaseModel):
    form: str
    base: str
    parse: str


class BlockData(BaseModel):
    block_id: int
    block_slug: str
    token_block_id: int
    version: int
    tokens: list[TokenData]

    def to_tsv_string(self) -> str:
        tsv_lines = []
        for token in self.tokens:
            row = (token.form.strip(), token.base.strip(), token.parse.strip())
            if all(row):
                tsv_lines.append("\t".join(row))
        return "\n".join(tsv_lines)


class ParseDataRequest(BaseModel):
    text: str
    blocks: list[BlockData]


@bp.route("/")
def index():
    """All published texts - redirects to /proofing/texts."""
    return redirect(url_for("proofing.texts"))


@bp.route("/<text_slug>/tagging")
def text(text_slug):
    """A text and its parse status."""
    text_ = q.text(text_slug)
    if text_ is None:
        abort(404)

    assert text_
    session = q.get_session()
    num_blocks = session.scalar(
        sqla.select(sqla.func.count(db.TextBlock.id)).filter_by(text_id=text_.id)
    )
    num_parsed_blocks = session.scalar(
        sqla.select(sqla.func.count(db.TokenBlock.id)).filter_by(text_id=text_.id)
    )

    return render_template(
        "proofing/tagging/text.html",
        text=text_,
        num_blocks=num_blocks,
        num_parsed_blocks=num_parsed_blocks,
    )


@bp.route("/<text_slug>/tagging/<section>", methods=["GET", "POST"])
@p2_required
def section(text_slug, section):
    db_text = q.text(text_slug)
    if db_text is None:
        abort(404)

    assert db_text
    session = q.get_session()
    db_section = next((s for s in db_text.sections if s.slug == section), None)
    if db_section is None:
        abort(404)

    if request.method == "POST":
        default = lambda: redirect(
            url_for("proofing.tagging.section", text_slug=text_slug, section=section)
        )
        data = request.form.get("parse_data")
        if not data:
            flash("No data provided", "error")
            return default()

        try:
            parse_data = ParseDataRequest.model_validate_json(data)
        except Exception as e:
            flash(f"Invalid data format: {e}", "error")
            return default()

        updated_count = 0
        errors = []

        token_block_ids = [x.token_block_id for x in parse_data.blocks]
        token_blocks = (
            session.execute(
                sqla.select(db.TokenBlock).where(db.TokenBlock.id.in_(token_block_ids))
            )
            .scalars()
            .all()
        )
        token_blocks_by_id = {x.id: x for x in token_blocks}

        for block_data in parse_data.blocks:
            token_block = token_blocks_by_id.get(block_data.token_block_id)
            if not token_block:
                errors.append(f"TokenBlock not found for block {block_data.block_slug}")
                continue

            new_parse_data = block_data.to_tsv_string()

            latest_revision = session.execute(
                sqla.select(db.TokenRevision)
                .filter_by(token_block_id=token_block.id)
                .order_by(db.TokenRevision.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()

            if latest_revision and latest_revision.data == new_parse_data:
                continue

            try:
                revisions.add_token_revision(
                    token_block=token_block,
                    data=new_parse_data,
                    version=block_data.version,
                    author_id=current_user.id,
                    block_id=block_data.block_id,
                )
                updated_count += 1
            except revisions.EditError as e:
                errors.append(
                    f"Conflict: Block {block_data.block_slug} was modified by someone else. "
                    f"Please reload and try again. ({e})"
                )

        for error in errors:
            flash(error, "warning")
        if updated_count > 0:
            flash(f"Updated {updated_count} block(s)", "success")
        else:
            flash("No changes were made", "info")

        return redirect(
            url_for("proofing.tagging.section", text_slug=text_slug, section=section)
        )

    blocks_in_section = []
    for block in db_section.blocks:
        token_block = session.execute(
            sqla.select(db.TokenBlock).filter_by(block_id=block.id)
        ).scalar_one_or_none()

        if token_block:
            latest_revision = session.execute(
                sqla.select(db.TokenRevision)
                .filter_by(token_block_id=token_block.id)
                .order_by(db.TokenRevision.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()

            if latest_revision:
                tokens = []
                for line in latest_revision.data.strip().split("\n"):
                    if line.strip():
                        parts = line.split("\t")
                        if len(parts) == 3:
                            tokens.append(
                                TokenData(
                                    form=parts[0],
                                    base=parts[1],
                                    parse=parts[2],
                                )
                            )

                # Convert XML to string for JSON serialization
                xml_content = xml.transform_text_block(block.xml)
                if hasattr(xml_content, "__html__"):
                    xml_content = xml_content.__html__()
                else:
                    xml_content = str(xml_content)

                block_data = BlockData(
                    block_id=block.id,
                    block_slug=block.slug,
                    token_block_id=token_block.id,
                    version=token_block.version,
                    tokens=tokens,
                )

                blocks_in_section.append(
                    {
                        "block_data": block_data.model_dump(),
                        "xml_content": xml_content,
                    }
                )

    sections_with_parse = []
    for section in db_text.sections:
        has_parse = False
        for block in section.blocks:
            token_block = session.execute(
                sqla.select(db.TokenBlock).filter_by(block_id=block.id)
            ).scalar_one_or_none()
            if token_block:
                has_parse = True
                break
        if has_parse:
            sections_with_parse.append(section)

    current_idx = None
    for idx, section in enumerate(sections_with_parse):
        if section.id == db_section.id:
            current_idx = idx
            break

    prev_section_url = None
    next_section_url = None
    if current_idx is not None:
        if current_idx > 0:
            prev_section = sections_with_parse[current_idx - 1]
            prev_section_url = url_for(
                "proofing.tagging.section",
                text_slug=db_text.slug,
                section=prev_section.slug,
            )
        if current_idx < len(sections_with_parse) - 1:
            next_section = sections_with_parse[current_idx + 1]
            next_section_url = url_for(
                "proofing.tagging.section",
                text_slug=db_text.slug,
                section=next_section.slug,
            )

    return render_template(
        "proofing/tagging/section.html",
        text=db_text,
        section=db_section,
        blocks=blocks_in_section,
        current_section_num=current_idx + 1 if current_idx is not None else 1,
        total_sections=len(sections_with_parse),
        prev_section_url=prev_section_url,
        next_section_url=next_section_url,
    )


@bp.route("/<text_slug>/batch-tag", methods=["POST"])
@p2_required
def batch_tag(text_slug):
    """Trigger Dharmamitra batch tagging for all blocks in a text."""
    text_ = q.text(text_slug)
    if text_ is None:
        abort(404)

    app_env = current_app.config["AMBUDA_ENVIRONMENT"]

    try:
        result = tagging_tasks.tag_text.delay(app_env, text_.slug)
        flash(f"Batch tagging started. Task ID: {result.id}", "success")
    except Exception as e:
        flash(f"Error starting batch tagging: {e}", "error")

    return redirect(url_for("proofing.tagging.text", text_slug=text_slug))
