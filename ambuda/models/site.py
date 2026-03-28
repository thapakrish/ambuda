"""Various site content unrelated to texts and proofing.

The idea is that a trusted user can edit site content by creating and modifyng
these objects. By doing so, they can update the site without waiting for a site
deploy.
"""

from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, String
from sqlalchemy import Text as Text_

from ambuda.models.base import Base, pk


class SiteConfigData(BaseModel):
    """Pydantic schema for site-wide configuration."""

    #: Slugs of texts to feature as "popular" on the homepage.
    popular_texts: list[str] = Field(default_factory=list)


class SiteConfig(Base):
    """A singleton row holding site-wide configuration as JSON.

    Edit via the admin UI. The `data` column stores a JSON blob
    validated by `SiteConfigData`.
    """

    __tablename__ = "site_config"

    id = pk()
    #: JSON blob validated by SiteConfigData.
    data = Column(Text_, nullable=False, default="{}")

    def parsed(self) -> SiteConfigData:
        """Return validated config."""
        return SiteConfigData.model_validate_json(self.data)

    def set_data(self, config: SiteConfigData):
        """Serialize config back to JSON."""
        self.data = config.model_dump_json()


class ProjectSponsorship(Base):
    """A project that a donor can sponsor."""

    __tablename__ = "site_project_sponsorship"

    #: Primary key.
    id = pk()
    #: Sanskrit title.
    sa_title = Column(String, nullable=False)
    #: English title.
    en_title = Column(String, nullable=False)
    #: A short description of this project.
    description = Column(Text_, nullable=False)
    #: The estimated cost of this project in Indian rupees (INR).
    cost_inr = Column(Integer, nullable=False)


class ContributorInfo(Base):
    """Information about an Ambuda contributor.

    For now, we use this for just proofreaders. Long-term, we might include
    other types of contributors here as well.
    """

    __tablename__ = "contributor_info"

    #: Primary key.
    id = pk()
    #: The contributor's name.
    name = Column(String, nullable=False)
    #: The contributor's title, role, occupation, etc.
    title = Column(String, nullable=False, default="")
    #: A short description of this proofer.
    description = Column(Text_, nullable=False, default="")
