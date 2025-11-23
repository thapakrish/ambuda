"""Utilities for ingesting data assets into Ambuda."""

from pathlib import Path
from typing import Iterator

from sqlalchemy import select
from sqlalchemy.orm import Session, load_only

import ambuda.database as db


def create_text_from_document(session: Session, slug: str, title: str, document):
    text = db.Text(slug=slug, title=title, header=document.header)
    session.add(text)
    session.flush()

    n = 1
    for section in document.sections:
        db_section = db.TextSection(
            text_id=text.id, slug=section.slug, title=section.slug
        )
        session.add(db_section)
        session.flush()

        for block in section.blocks:
            db_block = db.TextBlock(
                text_id=text.id,
                section_id=db_section.id,
                slug=block.slug,
                xml=block.blob,
                n=n,
            )
            session.add(db_block)
            n += 1

    session.commit()
    return text


def drop_existing_parse_data(session: Session, text_id: int):
    stmt = select(db.BlockParse).filter_by(text_id=text_id)
    for parse in session.scalars(stmt).all():
        session.delete(parse)


def get_slug_id_map(session: Session, text_id: int) -> dict[str, int]:
    stmt = (
        select(db.TextBlock)
        .filter_by(text_id=text_id)
        .options(
            load_only(
                db.TextBlock.id,
                db.TextBlock.slug,
            )
        )
    )
    blocks = list(session.scalars(stmt).all())
    return {b.slug: b.id for b in blocks}


def iter_parse_data(path: Path) -> Iterator[tuple[str, str]]:
    block_slug = None
    buf = []
    with open(path) as f:
        for line in f:
            line = line.strip()

            if line.startswith("#"):
                comm, key, eq, value = line.split()
                if key == "id":
                    xml_id = value
                    _, _, block_slug = xml_id.partition(".")
            elif line:
                if line.count("\t") != 2:
                    raise ValueError(f'Line "{line}" must have exactly two tabs.')
                buf.append(line)
            else:
                yield block_slug, "\n".join(buf)
                buf = []
    if buf:
        yield block_slug, "\n".join(buf)


def add_parse_data(session: Session, text_slug: str, path: Path):
    stmt = select(db.Text).filter_by(slug=text_slug)
    text = session.scalars(stmt).first()
    if not text:
        raise ValueError(f"Text with slug '{text_slug}' not found")

    drop_existing_parse_data(session, text.id)

    slug_id_map = get_slug_id_map(session, text.id)
    for slug, blob in iter_parse_data(path):
        if slug not in slug_id_map:
            raise ValueError(f"Block slug '{slug}' not found in text '{text_slug}'")
        session.add(
            db.BlockParse(text_id=text.id, block_id=slug_id_map[slug], data=blob)
        )
    session.commit()
