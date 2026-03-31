"""Views for the text catalog with filtering and search."""

from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections import Counter

from flask import Blueprint, render_template, request, jsonify

import ambuda.queries as q
from ambuda.utils import text_utils
from ambuda.utils.xml import parse_tei_header

bp = Blueprint("catalog", __name__)

PER_PAGE = 25

# Status badge config: (value, label, css_color, css_dot)
STATUS_BADGES = [
    ("p0", "Unproofed", "text-red-400", "bg-red-300"),
    ("p1", "Proofed once", "text-yellow-600", "bg-yellow-400"),
    ("p2", "Proofed", "text-green-600", "bg-green-400"),
]


def _text_type(text) -> str:
    """Classify a text as mula, commentary, or translation."""
    if text.parent_id is None:
        return "mula"
    if text.language != text.parent.language:
        return "translation"
    return "commentary"


def _text_source(text) -> str:
    return "ambuda" if text.project_id else "external"


def _flatten_entries(grouped_entries):
    """Flatten grouped text entries, promoting children to top-level."""
    entries = []
    for group in grouped_entries:
        for sub in group.subgroups:
            for entry in sub.entries:
                entries.append(entry)
                entries.extend(entry.children)
    return entries


def _apply_filters(entries, search, collection_ids, text_types, statuses, sources):
    """Apply all sidebar filters and return the filtered list."""
    if search:
        q_lower = search.lower()
        entries = [
            e
            for e in entries
            if q_lower in e.text.title.lower() or q_lower in e.text.slug.lower()
        ]

    if collection_ids:
        all_colls = q.Query(q.get_session()).all_collections()
        expanded = set()
        for cid in collection_ids:
            expanded.update(q.all_descendant_ids(cid, all_colls))
        entries = [
            e for e in entries if any(c.id in expanded for c in e.text.collections)
        ]

    if text_types:
        allowed = set(text_types)
        entries = [e for e in entries if _text_type(e.text) in allowed]

    if statuses:
        allowed = set(statuses)
        entries = [e for e in entries if (e.text.status or "none") in allowed]

    if sources:
        allowed = set(sources)
        entries = [e for e in entries if _text_source(e.text) in allowed]

    return entries


def _sort_entries(entries, field, direction):
    """Sort entries in place."""
    reverse = direction == "desc"
    if field == "created":
        entries.sort(
            key=lambda e: e.text.created_at.isoformat() if e.text.created_at else "",
            reverse=reverse,
        )
    else:
        entries.sort(key=lambda e: e.text.title.lower(), reverse=reverse)


def _compute_counts(entries, collections=None):
    """Compute sidebar facet counts from a list of entries.

    Collection counts are rolled up: a parent's count is the number of unique
    texts tagged with it *or any of its children*, so the parent total is never
    less than the sum of its children.
    """
    # Build per-collection sets of text ids for deduplication
    coll_text_ids: dict[int, set[int]] = {}
    for e in entries:
        for c in e.text.collections:
            coll_text_ids.setdefault(c.id, set()).add(e.text.id)

    # Roll up: grandchildren into children, then children into parents
    if collections:
        for parent in collections:
            for child in parent.children or []:
                child_set = coll_text_ids.setdefault(child.id, set())
                for grandchild in child.children or []:
                    child_set |= coll_text_ids.get(grandchild.id, set())
            parent_set = coll_text_ids.setdefault(parent.id, set())
            for child in parent.children or []:
                parent_set |= coll_text_ids.get(child.id, set())

    coll_counts = Counter({cid: len(ids) for cid, ids in coll_text_ids.items()})

    return {
        "type": Counter(_text_type(e.text) for e in entries),
        "collection": coll_counts,
        "status": Counter(e.text.status or "none" for e in entries),
        "source": Counter(_text_source(e.text) for e in entries),
    }


def _decode_cursor(cursor):
    """Decode an opaque cursor string to an integer offset. Returns 0 on failure."""
    if not cursor:
        return 0
    try:
        return max(0, int(urlsafe_b64decode(cursor.encode()).decode()))
    except Exception:
        return 0


def _encode_cursor(offset):
    """Encode an integer offset as an opaque cursor string."""
    return urlsafe_b64encode(str(offset).encode()).decode()


def _paginate(items, cursor, per_page):
    """Return (page_items, total, prev_cursor, next_cursor, offset)."""
    total = len(items)
    offset = _decode_cursor(cursor)
    offset = min(offset, max(0, total - 1))  # clamp to valid range
    page_items = items[offset : offset + per_page]
    prev_cursor = _encode_cursor(offset - per_page) if offset > 0 else None
    next_cursor = (
        _encode_cursor(offset + per_page) if offset + per_page < total else None
    )
    return page_items, total, prev_cursor, next_cursor, offset


def _parse_headers(entries):
    """Build a dict mapping text.id -> ParsedTEIHeader."""
    headers = {}
    for e in entries:
        if e.text.header:
            try:
                headers[e.text.id] = parse_tei_header(e.text.header)
            except Exception:
                pass
    return headers


def _serialize_counts(counts):
    """Serialize counts dict for JSON response."""
    return {
        "type": dict(counts["type"]),
        "collection": {str(k): v for k, v in counts["collection"].items()},
        "status": dict(counts["status"]),
        "source": dict(counts["source"]),
    }


@bp.route("/")
def index():
    """Show all texts with filtering sidebar."""
    search = request.args.get("q", "").strip()
    collection_ids = request.args.getlist("collection", type=int)
    text_types = request.args.getlist("text_type")
    statuses = request.args.getlist("status")
    sources = request.args.getlist("source")
    sort_field = request.args.get("sort", "title")
    sort_dir = request.args.get("sort_dir", "asc")
    cursor = request.args.get("cursor", "")

    collections = q.collections()
    all_entries = _flatten_entries(text_utils.create_grouped_text_entries())
    filtered = _apply_filters(
        all_entries, search, collection_ids, text_types, statuses, sources
    )
    counts = _compute_counts(filtered, collections)
    _sort_entries(filtered, sort_field, sort_dir)
    page_entries, total, prev_cursor, next_cursor, offset = _paginate(
        filtered, cursor, PER_PAGE
    )
    headers = _parse_headers(page_entries)

    pagination = dict(
        total=total,
        per_page=PER_PAGE,
        prev_cursor=prev_cursor,
        next_cursor=next_cursor,
        offset=offset,
    )

    if request.args.get("partial") == "1":
        return jsonify(
            html=render_template(
                "catalog/results.html",
                entries=page_entries,
                headers=headers,
                status_badges=STATUS_BADGES,
                **pagination,
            ),
            bar_html=render_template("catalog/results_bar.html", **pagination),
            count=total,
            counts=_serialize_counts(counts),
        )

    return render_template(
        "catalog/index.html",
        entries=page_entries,
        headers=headers,
        collections=collections,
        search=search,
        collection_ids=collection_ids,
        text_types=text_types,
        statuses=statuses,
        sources=sources,
        counts=counts,
        status_badges=STATUS_BADGES,
        sort_field=sort_field,
        sort_dir=sort_dir,
        **pagination,
    )
