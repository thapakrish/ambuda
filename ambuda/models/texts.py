"""Models for text documents in our library.

We define texts with three different tables:

- `Text` defines the text as a whole.
- `TextSection` defines ordered sections of a `Text`.
- `TextBlock` is typically a verse or paragraph within a `TextSection`.
"""

import json
from datetime import UTC, datetime

from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, JSON, Table, event
from sqlalchemy import Text as _Text
from sqlalchemy.orm import relationship

from ambuda.models.base import Base, foreign_key, pk


class TitleConfig(BaseModel):
    fixed: dict[str, str] = Field(default_factory=dict)
    patterns: dict[str, str] = Field(default_factory=dict)


class TextConfig(BaseModel):
    titles: TitleConfig = Field(default_factory=TitleConfig)


class Text(Base):
    """A text with its metadata."""

    __tablename__ = "texts"

    #: Primary key.
    id = pk()
    #: Human-readable ID, which we display in the URL.
    slug = Column(String, unique=True, nullable=False)
    #: The title of this text.
    title = Column(String, nullable=False)
    #: Metadata for this text, as a <teiHeader> element.
    #: This is public-facing metadata as part of a TEI document.
    header = Column(_Text)
    #: Additional metadata for this text as a JSON document.
    #: This is mainly for ambuda-internal notes on heading names, etc.
    # NOTE: `meta` is reserved by WTForms and `metadata` has other meanings in sqlalchemy,
    # NOTE: so just call this `config`.
    #: The schema is defined in `TextConfig`.
    config = Column(JSON, nullable=True)
    language = Column(String, nullable=False, default="sa")

    #: Timestamp at which this text was created.
    #: Nullable for legacy reasons.
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=True)
    #: Timestamp at which this text was published.
    published_at = Column(DateTime, nullable=True)

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

    def __str__(self):
        return self.slug


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
    slug = Column(String, index=True, nullable=False)
    #: The title of this section.
    title = Column(String, nullable=False)
    #: An ordered list of the blocks contained within this section.
    blocks = relationship(
        "TextBlock", backref="section", order_by=lambda: TextBlock.n, cascade="delete"
    )


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
    slug = Column(String, index=True, nullable=False)
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
