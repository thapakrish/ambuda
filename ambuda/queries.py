"""Common queries.

We use this module to organize repetitive query logic and keep our views readable.
For simple or adhoc queries, you can just write them in their corresponding view.
"""

import dataclasses as dc
import functools

from flask import current_app
from sqlalchemy import create_engine, select
from sqlalchemy.orm import (
    load_only,
    scoped_session,
    selectinload,
    sessionmaker,
    Session,
)

import ambuda.database as db

# NOTE: this logic is copied from Flask-SQLAlchemy. We avoid Flask-SQLAlchemy
# because we also need to access the database from a non-Flask context when
# we run database seed scripts.
# ~~~
# Scope the session to the current greenlet if greenlet is available,
# otherwise fall back to the current thread.
try:
    from greenlet import getcurrent as _ident_func
except ImportError:
    from threading import get_ident as _ident_func


# functools.cache makes this return value a singleton.
@functools.cache
def get_engine():
    database_uri = current_app.config["SQLALCHEMY_DATABASE_URI"]
    # For debugging, add echo=True to the constructor.
    return create_engine(database_uri)


# functools.cache makes this return value a singleton.
@functools.cache
def get_session_class():
    # Scoped sessions remove various kinds of errors, e.g. when using database
    # objects created on different threads.
    #
    # For details, see:
    # - https://stackoverflow.com/questions/12223335
    # - https://flask.palletsprojects.com/en/2.1.x/patterns/sqlalchemy/
    session_factory = sessionmaker(bind=get_engine(), autoflush=False, autocommit=False)
    return scoped_session(session_factory, scopefunc=_ident_func)


def get_session():
    """Instantiate a scoped session.

    If we implemented this right, there should be exactly one unique session
    per request.
    """
    Session = get_session_class()
    return Session()


class Query:
    def __init__(self, session=None):
        self.session = session or get_session()

    def texts(self) -> list[db.Text]:
        return list(self.session.scalars(select(db.Text)).all())

    def page_statuses(self) -> list[db.PageStatus]:
        return list(self.session.scalars(select(db.PageStatus)).all())

    def text(self, slug: str) -> db.Text | None:
        stmt = (
            select(db.Text)
            .filter_by(slug=slug)
            .options(
                selectinload(db.Text.sections).load_only(
                    db.TextSection.slug,
                    db.TextSection.title,
                )
            )
        )
        return self.session.scalars(stmt).first()

    def text_meta(self, slug: str) -> db.Text | None:
        # TODO: is this method even useful? Is there a performance penalty for
        # using just `text`?
        stmt = (
            select(db.Text)
            .filter_by(slug=slug)
            .options(
                load_only(
                    db.Text.id,
                    db.Text.slug,
                )
            )
        )
        return self.session.scalars(stmt).first()

    def text_section(self, text_id: int, slug: str) -> db.TextSection | None:
        stmt = select(db.TextSection).filter_by(text_id=text_id, slug=slug)
        return self.session.scalars(stmt).first()

    def text_export(self, slug: str) -> db.TextExport | None:
        stmt = select(db.TextExport).filter_by(slug=slug)
        return self.session.scalars(stmt).first()

    def block(self, text_id: int, slug: str) -> db.TextBlock | None:
        stmt = select(db.TextBlock).filter_by(text_id=text_id, slug=slug)
        return self.session.scalars(stmt).first()

    def block_parse(self, block_id: int) -> db.BlockParse | None:
        stmt = select(db.BlockParse).filter_by(block_id=block_id)
        return self.session.scalars(stmt).first()

    def dictionaries(self) -> list[db.Dictionary]:
        return list(self.session.scalars(select(db.Dictionary)).all())

    def dict_entries(
        self, sources: list[str], keys: list[str]
    ) -> dict[str, list[db.DictionaryEntry]]:
        dicts = self.dictionaries()
        source_ids = [d.id for d in dicts if d.slug in sources]

        stmt = select(db.DictionaryEntry).filter(
            (db.DictionaryEntry.dictionary_id.in_(source_ids))
            & (db.DictionaryEntry.key.in_(keys))
        )
        rows = list(self.session.scalars(stmt).all())

        dict_id_to_slug = {d.id: d.slug for d in dicts}
        mapping = {s: [] for s in sources}
        for row in rows:
            dict_slug = dict_id_to_slug[row.dictionary_id]
            mapping[dict_slug].append(row)
        return mapping

    def active_projects(self) -> list[db.Project]:
        from ambuda.models.proofing import ProjectStatus

        stmt = select(db.Project).filter(db.Project.status == ProjectStatus.ACTIVE)
        return list(self.session.scalars(stmt).all())

    def pending_projects(self) -> list[db.Project]:
        from ambuda.models.proofing import ProjectStatus

        stmt = select(db.Project).filter(db.Project.status == ProjectStatus.PENDING)
        return list(self.session.scalars(stmt).all())

    def project(self, slug: str) -> db.Project | None:
        stmt = select(db.Project).filter(db.Project.slug == slug)
        return self.session.scalars(stmt).first()

    def thread(self, *, id: int) -> db.Thread | None:
        stmt = select(db.Thread).filter_by(id=id)
        return self.session.scalars(stmt).first()

    def post(self, *, id: int) -> db.Post | None:
        stmt = select(db.Post).filter_by(id=id)
        return self.session.scalars(stmt).first()

    def create_thread(self, *, board_id: int, user_id: int, title: str, content: str):
        assert board_id
        thread = db.Thread(board_id=board_id, author_id=user_id, title=title)
        self.session.add(thread)
        self.session.flush()

        post = db.Post(
            board_id=board_id, author_id=user_id, thread_id=thread.id, content=content
        )
        self.session.add(post)
        self.session.commit()

    def create_post(
        self, *, board_id: int, thread: db.Thread, user_id: int, content: str
    ):
        post = db.Post(
            board_id=board_id, author_id=user_id, thread_id=thread.id, content=content
        )
        self.session.add(post)
        self.session.flush()

        assert post.created_at
        thread.updated_at = post.created_at
        self.session.add(thread)
        self.session.commit()

    def pages_with_revisions(self, project_id, page_slugs: list[str]) -> list[db.Page]:
        stmt = (
            select(db.Page)
            .filter((db.Page.project_id == project_id) & (db.Page.slug.in_(page_slugs)))
            .options(selectinload(db.Page.revisions))
        )
        return list(self.session.scalars(stmt).all())

    def page(self, project_id, page_slug: str) -> db.Page | None:
        stmt = select(db.Page).filter(
            (db.Page.project_id == project_id) & (db.Page.slug == page_slug)
        )
        return self.session.scalars(stmt).first()

    def revision(self, revision_id) -> db.Revision | None:
        stmt = select(db.Revision).filter((db.Revision.id == revision_id))
        return self.session.scalars(stmt).first()

    def user(self, username: str) -> db.User | None:
        stmt = select(db.User).filter_by(
            username=username, is_deleted=False, is_banned=False
        )
        return self.session.scalars(stmt).first()

    def create_user(self, *, username: str, email: str, raw_password: str) -> db.User:
        user = db.User(username=username, email=email)
        user.set_password(raw_password)
        self.session.add(user)
        self.session.flush()

        # Allow all users to be proofreaders
        stmt = select(db.Role).filter_by(name=db.SiteRole.P1.value)
        proofreader_role = self.session.scalars(stmt).first()
        user_role = db.UserRoles(user_id=user.id, role_id=proofreader_role.id)
        self.session.add(user_role)

        self.session.commit()
        return user

    def blog_post(self, slug: str) -> db.BlogPost | None:
        stmt = select(db.BlogPost).filter_by(slug=slug)
        return self.session.scalars(stmt).first()

    def blog_posts(self) -> list[db.BlogPost]:
        stmt = select(db.BlogPost).order_by(db.BlogPost.created_at.desc())
        return list(self.session.scalars(stmt).all())

    def project_sponsorships(self) -> list[db.ProjectSponsorship]:
        results = list(self.session.scalars(select(db.ProjectSponsorship)).all())
        return sorted(results, key=lambda s: s.sa_title or s.en_title)

    def contributor_info(self) -> list[db.ContributorInfo]:
        stmt = select(db.ContributorInfo).order_by(db.ContributorInfo.name)
        return list(self.session.scalars(stmt).all())

    def genres(self) -> list[db.Genre]:
        return list(self.session.scalars(select(db.Genre)).all())

    def authors(self) -> list[db.Author]:
        return list(self.session.scalars(select(db.Author)).all())


def texts() -> list[db.Text]:
    """Return a list of all texts in no particular older."""
    query = Query(get_session())
    return query.texts()


def page_statuses() -> list[db.PageStatus]:
    query = Query(get_session())
    return query.page_statuses()


def text(slug: str) -> db.Text | None:
    query = Query(get_session())
    return query.text(slug)


def text_meta(slug: str) -> db.Text | None:
    """Return only specific fields from the given text."""
    # TODO: is this method even useful? Is there a performance penalty for
    # using just `text`?
    query = Query(get_session())
    return query.text_meta(slug)


def text_section(text_id: int, slug: str) -> db.TextSection | None:
    query = Query(get_session())
    return query.text_section(text_id, slug)


def text_export(slug: str) -> db.TextExport | None:
    query = Query(get_session())
    return query.text_export(slug)


def block(text_id: int, slug: str) -> db.TextBlock | None:
    query = Query(get_session())
    return query.block(text_id, slug)


def block_parse(block_id: int) -> db.BlockParse | None:
    query = Query(get_session())
    return query.block_parse(block_id)


def dictionaries() -> list[db.Dictionary]:
    query = Query(get_session())
    return query.dictionaries()


def dict_entries(
    sources: list[str], keys: list[str]
) -> dict[str, list[db.DictionaryEntry]]:
    """
    :param sources: slugs of the dictionaries to query
    :param keys: the keys (dictionary entries) to query
    """
    query = Query(get_session())
    return query.dict_entries(sources, keys)


def active_projects() -> list[db.Project]:
    """Return all active projects in no particular order."""
    query = Query(get_session())
    return query.active_projects()


def pending_projects() -> list[db.Project]:
    """Return all pending projects in no particular order."""
    query = Query(get_session())
    return query.pending_projects()


def project(slug: str) -> db.Project | None:
    query = Query(get_session())
    return query.project(slug)


def thread(*, id: int) -> db.Thread | None:
    query = Query(get_session())
    return query.thread(id=id)


def post(*, id: int) -> db.Post | None:
    query = Query(get_session())
    return query.post(id=id)


def create_thread(*, board_id: int, user_id: int, title: str, content: str):
    query = Query(get_session())
    return query.create_thread(
        board_id=board_id, user_id=user_id, title=title, content=content
    )


def create_post(*, board_id: int, thread: db.Thread, user_id: int, content: str):
    query = Query(get_session())
    return query.create_post(
        board_id=board_id, thread=thread, user_id=user_id, content=content
    )


def pages_with_revisions(project_id, page_slugs: list[str]) -> list[db.Page]:
    query = Query(get_session())
    return query.pages_with_revisions(project_id, page_slugs)


def page(project_id, page_slug: str) -> db.Page | None:
    query = Query(get_session())
    return query.page(project_id, page_slug)


def revision(revision_id) -> db.Revision | None:
    query = Query(get_session())
    return query.revision(revision_id)


def user(username: str) -> db.User | None:
    query = Query(get_session())
    return query.user(username)


def create_user(*, username: str, email: str, raw_password: str) -> db.User:
    query = Query(get_session())
    return query.create_user(username=username, email=email, raw_password=raw_password)


def blog_post(slug: str) -> db.BlogPost | None:
    """Fetch the given blog post."""
    query = Query(get_session())
    return query.blog_post(slug)


def blog_posts() -> list[db.BlogPost]:
    """Fetch all blog posts."""
    query = Query(get_session())
    return query.blog_posts()


def project_sponsorships() -> list[db.ProjectSponsorship]:
    query = Query(get_session())
    return query.project_sponsorships()


def contributor_info() -> list[db.ContributorInfo]:
    query = Query(get_session())
    return query.contributor_info()


def genres() -> list[db.Genre]:
    query = Query(get_session())
    return query.genres()


def authors() -> list[db.Author]:
    query = Query(get_session())
    return query.authors()
