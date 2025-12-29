from sqlalchemy import update
import sqlalchemy as sqla
from sqlalchemy.orm import Session

from ambuda import database as db
from ambuda import queries as q
from ambuda.queries import Query


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
    batch_id: int | None = None,
    session: Session | None = None,
    query: Query | None = None,
) -> int:
    """Add a new revision for a page."""
    # If this doesn't update any rows, there's an edit conflict.
    # Details: https://gist.github.com/shreevatsa/237bd6592771caadecc68c9515403bc3
    # FIXME: rather than do this on the application side, do an `exists` query
    # FIXME: instead? Not sure if this is a clear win, but worth thinking about.

    # FIXME: Check for `proofreading` user permission before allowing changes
    query = query or Query()
    session = session or q.get_session()

    if status_id is None:
        if status is None:
            raise ValueError("Either status or status_id must be provided")
        status_ids = {s.name: s.id for s in query.page_statuses()}
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
        batch_id=batch_id,
    )
    session.add(revision_)
    session.commit()
    return new_version


def add_token_revision(
    token_block: db.TokenBlock,
    data: str,
    version: int,
    author_id: int,
    block_id: int,
) -> int:
    session = q.get_session()

    # Update with optimistic locking
    new_version = version + 1
    result = session.execute(
        update(db.TokenBlock)
        .where(
            (db.TokenBlock.id == token_block.id) & (db.TokenBlock.version == version)
        )
        .values(version=new_version)
    )

    num_rows_changed = result.rowcount
    if num_rows_changed == 0:
        raise EditError(
            f"Edit conflict for TokenBlock {token_block.id}, version {version}"
        )

    assert num_rows_changed == 1

    token_revision = db.TokenRevision(
        token_block_id=token_block.id,
        author_id=author_id,
        data=data,
    )
    session.add(token_revision)
    session.flush()

    session.execute(sqla.delete(db.Token).where(db.Token.block_id == block_id))
    for order, line in enumerate(data.strip().split("\n")):
        if line.strip():
            parts = line.split("\t")
            if len(parts) == 3:
                form, lemma, parse = parts
                if form.strip() and lemma.strip() and parse.strip():
                    new_token = db.Token(
                        form=form.strip(),
                        base=lemma.strip(),
                        parse=parse.strip(),
                        block_id=block_id,
                        order=order,
                    )
                    session.add(new_token)

    session.commit()
    return new_version
