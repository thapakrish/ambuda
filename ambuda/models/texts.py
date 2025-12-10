"""Models for text documents in our library.

We define texts with three different tables:

- `Text` defines the text as a whole.
- `TextSection` defines ordered sections of a `Text`.
- `TextBlock` is typically a verse or paragraph within a `TextSection`.
"""

import json
from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, Integer, String, JSON, event
from sqlalchemy import Text as _Text
from sqlalchemy.orm import relationship

from ambuda.models.base import Base, foreign_key, pk


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
    # NOE: so just call this `config`.
    config = Column(JSON, nullable=True)
    genre_id = foreign_key("genres.id", nullable=True)
    #: The project that created this text.
    project_id = foreign_key("proof_projects.id", nullable=True)
    #: Timestamp at which this text was created.
    #: Nullable for legacy reasons.
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=True)
    #: Timestamp at which this text was published.
    published_at = Column(DateTime, nullable=True)

    #: An ordered list of the sections contained within this text.
    sections = relationship("TextSection", backref="text", cascade="delete")
    #: The genre this text belogns to.
    genre = relationship("Genre", backref="texts")
    #: The project that created this text.
    project = relationship("Project", backref="texts")

    def __str__(self):
        return self.slug


@event.listens_for(Text, "before_insert")
@event.listens_for(Text, "before_update")
def validate_text(mapper, connection, text):
    if text.config:
        try:
            json.loads(text.config)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Text.meta must be a valid JSON document: {e}")


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
