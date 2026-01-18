"""Models for text documents in our library.

We define texts with three different tables:

- `Text` defines the text as a whole.
- `TextSection` defines ordered sections of a `Text`.
- `TextBlock` is typically a verse or paragraph within a `TextSection`.
"""

import json
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, JSON, Table, event
from sqlalchemy import Text as _Text
from sqlalchemy import select, exists
from sqlalchemy.orm import relationship, Mapped, mapped_column, object_session

from ambuda.models.base import Base, foreign_key, pk


class TitleConfig(BaseModel):
    fixed: dict[str, str] = Field(default_factory=dict)
    patterns: dict[str, str] = Field(default_factory=dict)


class TextConfig(BaseModel):
    titles: TitleConfig = Field(default_factory=TitleConfig)


class TextStatus(StrEnum):
    P0 = "p0"
    P1 = "p1"
    P2 = "p2"


class Text(Base):
    """A text with its metadata."""

    __tablename__ = "texts"

    #: Primary key.
    id = pk()
    #: Human-readable ID, which we display in the URL.
    slug: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    #: The title of this text.
    title: Mapped[str] = mapped_column(String, nullable=False)
    #: Metadata for this text, as a <teiHeader> element.
    #: This is public-facing metadata as part of a TEI document.
    header: Mapped[str | None] = mapped_column(_Text)
    #: Additional metadata for this text as a JSON document.
    #: This is mainly for ambuda-internal notes on heading names, etc.
    # NOTE: `meta` is reserved by WTForms and `metadata` has other meanings in sqlalchemy,
    # NOTE: so just call this `config`.
    #: The schema is defined in `TextConfig`.
    config = Column(JSON, nullable=True)
    language: Mapped[str] = mapped_column(String, nullable=False, default="sa")

    #: Timestamp at which this text was created.
    #: Nullable for legacy reasons.
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=True)
    #: Timestamp at which this text was published.
    published_at = Column(DateTime, nullable=True)
    #: Timestamp at which this text was updated.
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    genre_id = foreign_key("genres.id", nullable=True)
    #: The project that created this text.
    project_id = foreign_key("proof_projects.id", nullable=True)
    # The text's author.
    author_id = foreign_key("authors.id", nullable=True)
    # The parent text that this text corresponds to.
    # (Non-null for translations, commentaries, etc.)
    parent_id = foreign_key("texts.id", nullable=True)

    #: An ordered list of the sections contained within this text.
    sections = relationship("TextSection", backref="text", cascade="delete")
    #: The genre this text belongs to.
    genre = relationship("Genre", backref="texts")
    #: The project that created this text.
    project = relationship("Project", backref="texts")
    #: The author that created this text.
    author = relationship("Author", backref="texts")
    # The parent text that this text corresponds to.
    parent = relationship("Text", remote_side=[id], backref="children")
    # The exports associated with this text.
    exports = relationship("TextExport", backref="text")

    # DEPRECATED parse data
    block_parses = relationship("BlockParse", backref="text")

    def __str__(self) -> str:
        return self.slug

    @property
    def supports_text_export(self) -> bool:
        """Temporary prop while we support Celery-based export."""
        return len(self.sections) < 20

    @property
    def has_parse_data(self) -> bool:
        from ambuda.models.parse import BlockParse, TokenBlock

        session = object_session(self)
        if not session:
            return False

        return (
            session.scalar(
                select(
                    exists().where(BlockParse.text_id == self.id)
                    | exists().where(TokenBlock.text_id == self.id)
                )
            )
            or False
        )


@event.listens_for(Text, "before_insert")
@event.listens_for(Text, "before_update")
def validate_text(mapper, connection, text):
    if text.config:
        try:
            json.loads(text.config)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Text.config must be a valid JSON document: {e}")


class TextSection(Base):
    """Ordered divisions of text content. This represent divisions like kāṇḍas,
    sargas, etc.

    A TextSection is the "unit of viewing." By default, Ambuda will display a
    text one section at a time.

    NOTE: sections are not nested.
    """

    __tablename__ = "text_sections"

    #: Primary key.
    id = pk()
    #: The text that contains this section.
    text_id = foreign_key("texts.id")
    #: Human-readable ID, which we display in the URL.
    #:
    #: Slugs are hierarchical, with different levels of the hierarchy separated
    #: by "." characters. At serving time, we rely on this property to properly
    #: organize a text into different sections.
    slug: Mapped[str] = mapped_column(String, index=True, nullable=False)
    #: The title of this section.
    title: Mapped[str] = mapped_column(String, nullable=False)
    #: An ordered list of the blocks contained within this section.
    blocks = relationship(
        "TextBlock", backref="section", order_by=lambda: TextBlock.n, cascade="delete"
    )

    def __str__(self) -> str:
        return self.slug


text_block_associations = Table(
    "text_block_associations",
    Base.metadata,
    Column("parent_id", Integer, ForeignKey("text_blocks.id"), primary_key=True),
    Column("child_id", Integer, ForeignKey("text_blocks.id"), primary_key=True),
)


class TextBlock(Base):
    """A verse or paragraph.

    A TextBlock is the "unit of reuse." When we make cross-references between
    texts, we do so at the TextBlock level.
    """

    __tablename__ = "text_blocks"

    #: Primary key.
    id = pk()
    #: The text this block belongs to.
    text_id = foreign_key("texts.id")
    #: The section this block belongs to.
    section_id = foreign_key("text_sections.id")
    #: The proofing page this block came from.
    page_id = foreign_key("proof_pages.id", nullable=True)
    #: Human-readable ID, which we display in the URL.
    slug: Mapped[str] = mapped_column(String, index=True, nullable=False)
    #: Raw XML content, which we translate into HTML at serving time.
    xml = Column(_Text, nullable=False)
    #: (internal-only) Block A comes before block B iff A.n < B.n.
    n = Column(Integer, nullable=False)

    text = relationship("Text")
    page = relationship("Page", backref="text_blocks")

    children = relationship(
        "TextBlock",
        secondary=text_block_associations,
        primaryjoin="TextBlock.id==text_block_associations.c.parent_id",
        secondaryjoin="TextBlock.id==text_block_associations.c.child_id",
        order_by="TextBlock.n",
        backref="parents_backref",
    )

    # Parents: blocks that are parents of this block (e.g., verses that this commentary references)
    parents = relationship(
        "TextBlock",
        secondary=text_block_associations,
        primaryjoin="TextBlock.id==text_block_associations.c.child_id",
        secondaryjoin="TextBlock.id==text_block_associations.c.parent_id",
        order_by="TextBlock.n",
        viewonly=True,
    )

    def __str__(self) -> str:
        return self.slug


class Author(Base):
    """The author of some text."""

    __tablename__ = "authors"

    # Primary key.
    id = pk()
    # The author's name.
    name = Column(String, nullable=False)
    slug = Column(String, unique=True, nullable=False)

    def __str__(self):
        # Include slug because author names are not unique.
        return f"{self.name} ({self.slug})"


class TextExport(Base):
    """A catalog of text exports."""

    __tablename__ = "text_exports"

    id = pk()
    #: The text this export belongs to.
    text_id = foreign_key("texts.id")
    #: A unique identifier for this export.
    slug: Mapped[str] = mapped_column(String, unique=True)
    #: The type of export (plain_text, xml, pdf, tokens).
    export_type: Mapped[str] = mapped_column(String, nullable=False)
    #: The path to this resource on S3.
    s3_path: Mapped[str] = mapped_column(String)
    #: Size in bytes
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    #: SHA256 checksum of the file contents
    sha256_checksum: Mapped[str | None] = mapped_column(String, nullable=True)
    #: When this export was last updated.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(UTC), nullable=False
    )

    @property
    def export_config(self) -> "ExportConfig | None":
        from ambuda.utils.text_exports import EXPORTS

        try:
            return next(x for x in EXPORTS if x.matches(self.slug))
        except StopIteration:
            return None


class TextBlockBookmark(Base):
    """Bookmarks on a text."""

    __tablename__ = "text_block_bookmarks"

    #: The user that created this bookmark.
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), primary_key=True)
    #: The block that the user has bookmarked.
    block_id: Mapped[int] = mapped_column(
        ForeignKey("text_blocks.id"), primary_key=True
    )
    #: When the bookmark was created.
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(UTC)
    )

    #: Relationships
    block = relationship("TextBlock", backref="bookmarks")
    user = relationship("User", backref="bookmarks")
