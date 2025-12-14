import dataclasses as dc

from vidyut.lipi import transliterate, Scheme

from ambuda import queries as q
from ambuda.database import Text, Genre, Author


@dc.dataclass
class TextEntry:
    text: Text
    children: list["TextEntry"]

    genre: Genre | None
    author: Author | None


def create_text_entries() -> list[TextEntry]:
    texts = q.texts()
    mula_texts = []
    child_texts = []
    for text in texts:
        is_mula = text.parent_id is None
        (child_texts, mula_texts)[is_mula].append(text)

    sorted_mula_texts = sorted(
        mula_texts,
        key=lambda x: transliterate(x.title, Scheme.HarvardKyoto, Scheme.Devanagari),
    )
    sorted_child_texts = sorted(
        child_texts,
        key=lambda x: transliterate(x.title, Scheme.HarvardKyoto, Scheme.Devanagari),
    )
    genre_map = {x.id: x for x in q.genres()}
    author_map = {x.id: x for x in q.authors()}

    text_entries = []
    text_entry_map = {}
    for text in sorted_mula_texts:
        assert text.parent_id is None

        genre = genre_map.get(text.genre_id)
        author = author_map.get(text.author_id)
        entry = TextEntry(
            text=text,
            children=[],
            genre=genre,
            author=author,
        )
        text_entries.append(entry)
        text_entry_map[text.id] = entry

    for text in sorted_child_texts:
        assert text.parent_id is not None

        entry = TextEntry(text=text, children=[], genre=None, author=None)
        try:
            parent = text_entry_map[text.parent_id]
            parent.children.append(entry)
        except KeyError:
            pass

    return text_entries
