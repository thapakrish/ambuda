"""Views for text collections."""

from flask import Blueprint, abort, render_template

import ambuda.queries as q

bp = Blueprint("collections", __name__)


@bp.route("/")
def index():
    """List all top-level collections."""
    top_collections = q.collections()
    return render_template(
        "texts/collections_index.html",
        collections=top_collections,
    )


@bp.route("/<slug>")
def collection(slug):
    """List all texts in a collection and its descendants."""
    coll = q.collection(slug)
    if coll is None:
        abort(404)

    texts_list = q.collection_texts(coll)
    child_collections = sorted(coll.children, key=lambda c: c.order)

    return render_template(
        "texts/collection.html",
        collection=coll,
        texts=texts_list,
        child_collections=child_collections,
    )
