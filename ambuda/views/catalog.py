"""Views for the text catalog with filtering and search."""

from collections import Counter

from flask import Blueprint, render_template, request, jsonify

import ambuda.queries as q
from ambuda.utils import text_utils
from ambuda.utils.xml import parse_tei_header

bp = Blueprint("catalog", __name__)

PER_PAGE = 100

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


def _compute_counts(entries):
    """Compute sidebar facet counts from a list of entries."""
    coll_counts: Counter = Counter()
    for e in entries:
        for c in e.text.collections:
            coll_counts[c.id] += 1
    return {
        "type": Counter(_text_type(e.text) for e in entries),
        "collection": coll_counts,
        "status": Counter(e.text.status or "none" for e in entries),
        "source": Counter(_text_source(e.text) for e in entries),
    }


def _paginate(items, page, per_page):
    """Return (page_items, total, total_pages, clamped_page)."""
    total = len(items)
    total_pages = max(1, -(-total // per_page))  # ceil division
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    return items[start : start + per_page], total, total_pages, page


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
    page = request.args.get("page", 1, type=int)

    all_entries = _flatten_entries(text_utils.create_grouped_text_entries())
    filtered = _apply_filters(
        all_entries, search, collection_ids, text_types, statuses, sources
    )
    counts = _compute_counts(filtered)
    _sort_entries(filtered, sort_field, sort_dir)
    page_entries, total, total_pages, page = _paginate(filtered, page, PER_PAGE)
    headers = _parse_headers(page_entries)

    pagination = dict(
        page=page, total=total, total_pages=total_pages, per_page=PER_PAGE
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
        collections=q.collections(),
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
