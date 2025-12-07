from sqlalchemy import update

from ambuda import database as db
from ambuda import queries as q


class EditError(Exception):
    """Raised if a user's attempt to edit a page fails."""

    pass


# TODO(akp): refactor to use just status_id everywhere.
def add_revision(
    page: db.Page,
    summary: str,
    content: str,
    version: int,
    author_id: int,
    status: str | None = None,
    status_id: int | None = None,
) -> int:
    """Add a new revision for a page."""
    # If this doesn't update any rows, there's an edit conflict.
    # Details: https://gist.github.com/shreevatsa/237bd6592771caadecc68c9515403bc3
    # FIXME: rather than do this on the application side, do an `exists` query
    # FIXME: instead? Not sure if this is a clear win, but worth thinking about.

    # FIXME: Check for `proofreading` user permission before allowing changes
    session = q.get_session()

    if status_id is None:
        if status is None:
            raise ValueError("Either status or status_id must be provided")
        status_ids = {s.name: s.id for s in q.page_statuses()}
        status_id = status_ids[status]

    new_version = version + 1
    result = session.execute(
        update(db.Page)
        .where((db.Page.id == page.id) & (db.Page.version == version))
        .values(version=new_version, status_id=status_id)
    )
    session.commit()

    num_rows_changed = result.rowcount
    if num_rows_changed == 0:
        raise EditError(f"Edit conflict {page.slug}, {version}")

    # Must be 1 since there's exactly one page with the given page ID.
    # If this fails, the application data is in a weird state.
    assert num_rows_changed == 1

    revision_ = db.Revision(
        project_id=page.project_id,
        page_id=page.id,
        summary=summary,
        content=content,
        author_id=author_id,
        status_id=status_id,
    )
    session.add(revision_)
    session.commit()
    return new_version
