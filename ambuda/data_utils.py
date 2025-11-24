"""Utilities for ingesting data assets into Ambuda."""

import itertools
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, load_only

import ambuda.database as db
import ambuda.queries as q

#: The maximum number of entries to add to the dictionary at one time.
#: Batching is more efficient than adding entries one at a time. But large
#: batches also take up a lot of memory.
BATCH_SIZE = 10000


def _batches(generator, n):
    """Yield successive n-sized batches from a generator."""
    while True:
        batch = list(itertools.islice(generator, n))
        if batch:
            yield batch
        else:
            return


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


def import_dictionary_from_xml(slug: str, title: str, path: Path) -> int:
    """Import dictionary entries from an XML file using batch inserts."""

    # Create the dictionary.
    session = q.get_session()
    dictionary = db.Dictionary(slug=slug, title=title)
    session.add(dictionary)
    try:
        session.commit()
        session.close()  # New session in case upload fails
    except SQLAlchemyError as e:
        raise ValueError(f"Failed to create dictionary with slug '{slug}': {e}")

    def _iter_entries():
        """Streaming iterator that yields (key, value) tuples."""
        for event, elem in ET.iterparse(str(path), events=["end"]):
            if elem.tag != "entry":
                continue

            key_elem = elem.find("key")
            value_elem = elem.find("value")

            if key_elem is None:
                raise ValueError("Entry missing <key> element")
            if value_elem is None:
                raise ValueError("Entry missing <value> element")

            num_children = len(value_elem)
            if num_children != 1:
                raise ValueError(
                    f"<value> should have exactly one child, got {num_children}"
                )

            key = (key_elem.text or "").strip()
            value = ET.tostring(value_elem[0])

            if not key:
                raise ValueError("Entry has empty <key>")
            if not value:
                raise ValueError("Entry has empty <value>")

            yield key, value

            # Clear to free memory
            elem.clear()

    engine = q.get_engine()
    entries_table = db.DictionaryEntry.__table__
    ins = entries_table.insert()

    entry_count = 0
    with engine.begin() as conn:
        for batch in _batches(_iter_entries(), BATCH_SIZE):
            items = [
                {"dictionary_id": dictionary_id, "key": key, "value": value}
                for key, value in batch
            ]
            conn.execute(ins, items)
            entry_count += len(items)

    return entry_count


def import_text_metadata(session: Session, json_path: Path) -> tuple[int, list[str]]:
    try:
        with open(json_path, "r") as f:
            metadata_list = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON file: {e}")

    if not isinstance(metadata_list, list):
        raise ValueError("JSON file must contain a list of objects")

    genre_map = {}
    stmt = select(db.Genre)
    for genre in session.scalars(stmt).all():
        genre_map[genre.name] = genre.id

    updated_count = 0
    unmatched_slugs = []

    for item in metadata_list:
        if not isinstance(item, dict):
            raise ValueError("Each item in the JSON must be an object")

        slug = item.get("slug")
        if not slug:
            raise ValueError("Each item must have a 'slug' field")

        stmt = select(db.Text).filter_by(slug=slug)
        text = session.scalars(stmt).first()

        if not text:
            unmatched_slugs.append(slug)
            continue

        if "title" in item:
            text.title = item["title"]
        if "header" in item:
            text.header = item["header"]
        if "config" in item:
            text.config = json.dumps(item["config"]) if item["config"] else ""
        if "genre" in item:
            genre_name = item["genre"]
            if genre_name:
                if genre_name not in genre_map:
                    new_genre = db.Genre(name=genre_name)
                    session.add(new_genre)
                    session.flush()
                    genre_map[genre_name] = new_genre.id
                text.genre_id = genre_map[genre_name]
            else:
                text.genre_id = None

        updated_count += 1

    session.commit()
    return updated_count, unmatched_slugs
