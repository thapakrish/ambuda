"""Seed script for text collections.

Creates a default set of collections if none exist.
"""

from sqlalchemy.orm import Session

from ambuda.models.texts import TextCollection
from ambuda.seed.utils.data_utils import create_db


COLLECTIONS = [
    {
        "slug": "veda",
        "title": "Vedas",
        "order": 1,
        "children": [
            {"slug": "samhita", "title": "Samhitas", "order": 1},
            {"slug": "brahmana", "title": "Brahmanas", "order": 2},
            {"slug": "aranyaka", "title": "Aranyakas", "order": 3},
            {"slug": "upanishad", "title": "Upanishads", "order": 4},
        ],
    },
    {"slug": "itihasa", "title": "Itihasas", "order": 2},
    {"slug": "purana", "title": "Puranas", "order": 3},
    {
        "slug": "kavya",
        "title": "Kavya",
        "order": 4,
        "children": [
            {"slug": "mahakavya", "title": "Mahakavyas", "order": 1},
            {"slug": "nataka", "title": "Natakas", "order": 2},
            {"slug": "gadya-kavya", "title": "Gadya Kavya", "order": 3},
        ],
    },
    {"slug": "stotra", "title": "Stotras", "order": 5},
    {
        "slug": "shastra",
        "title": "Shastras",
        "order": 6,
        "children": [
            {"slug": "vyakarana", "title": "Vyakarana", "order": 1},
            {"slug": "darshana", "title": "Darshana", "order": 2},
            {"slug": "dharmashastra", "title": "Dharmashastra", "order": 3},
        ],
    },
]


def _create_collection(session: Session, data: dict, parent_id: int | None = None):
    collection = TextCollection(
        slug=data["slug"],
        title=data["title"],
        order=data.get("order", 0),
        parent_id=parent_id,
    )
    session.add(collection)
    session.flush()

    for child in data.get("children", []):
        _create_collection(session, child, parent_id=collection.id)


def run():
    engine = create_db()
    with Session(engine) as session:
        existing = session.query(TextCollection).first()
        if existing:
            print("Collections already exist, skipping seed.")
            return True

        for data in COLLECTIONS:
            _create_collection(session, data)

        session.commit()
        print("Created default text collections.")
    return True


if __name__ == "__main__":
    run()
