import dataclasses as dc
from collections import OrderedDict
from datetime import datetime, timedelta

from vidyut.lipi import transliterate, Scheme

import ambuda.database as db
from ambuda import queries as q


@dc.dataclass
class TextEntry:
    text: db.Text
    children: list["TextEntry"]

    author: db.Author | None


def create_text_entries() -> list[TextEntry]:
    texts = q.texts()
    mula_texts = []
    child_texts = []
    for text in texts:
        is_mula = text.parent_id is None
        (child_texts, mula_texts)[is_mula].append(text)

    sorted_mula_texts = sorted(
        mula_texts,
        key=lambda x: transliterate(x.title, Scheme.HarvardKyoto, Scheme.Devanagari),
    )
    sorted_child_texts = sorted(
        child_texts,
        key=lambda x: transliterate(x.title, Scheme.HarvardKyoto, Scheme.Devanagari),
    )
    author_map = {x.id: x for x in q.authors()}

    text_entries = []
    text_entry_map = {}
    for text in sorted_mula_texts:
        assert text.parent_id is None

        author = author_map.get(text.author_id)
        entry = TextEntry(
            text=text,
            children=[],
            author=author,
        )
        text_entries.append(entry)
        text_entry_map[text.id] = entry

    for text in sorted_child_texts:
        assert text.parent_id is not None

        entry = TextEntry(text=text, children=[], author=None)
        try:
            parent = text_entry_map[text.parent_id]
            parent.children.append(entry)
        except KeyError:
            pass

    return text_entries


def create_recent_text_entries() -> list[TextEntry]:
    one_week_ago = datetime.utcnow() - timedelta(weeks=1)
    all_entries = create_text_entries()
    recent = [
        e
        for e in all_entries
        if e.text.published_at is not None and e.text.published_at >= one_week_ago
    ]
    recent.sort(key=lambda e: e.text.published_at, reverse=True)
    return recent[:10]


def create_grouped_text_entries() -> OrderedDict[str, list[TextEntry]]:
    """Group text entries by their collections, ordered by collection order."""
    all_colls = q.Query(q.get_session()).all_collections()
    by_parent = q.group_collections_by_parent(all_colls)
    top_collections = by_parent.get(None, [])

    # Map every collection id to its top-level ancestor's title
    coll_id_to_heading: dict[int, str] = {}
    heading_order: list[str] = []
    for coll in top_collections:
        heading_order.append(coll.title)
        for cid in q.all_descendant_ids(coll.id, all_colls):
            coll_id_to_heading[cid] = coll.title

    fallback_heading = "\u0905\u0928\u094d\u092f\u0947 \u0917\u094d\u0930\u0928\u094d\u0925\u093e\u0903"  # अन्ये ग्रन्थाः

    grouped: OrderedDict[str, list[TextEntry]] = OrderedDict()
    for heading in heading_order:
        grouped[heading] = []
    grouped[fallback_heading] = []

    for entry in create_text_entries():
        heading = None
        for coll in entry.text.collections:
            h = coll_id_to_heading.get(coll.id)
            if h:
                heading = h
                break
        if heading is None:
            heading = fallback_heading
        grouped[heading].append(entry)

    return grouped
