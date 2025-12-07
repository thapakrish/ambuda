"""Utilities for manual text structuring."""

import dataclasses as dc
import re
import xml.etree.ElementTree as ET
import defusedxml.ElementTree as DET


@dc.dataclass
class Block:
    """A block of structured content."""

    #: The block's type (paragraph, verse, etc.)
    type: str
    content: str

    # general attributes
    #: The block's language ("sa", "hi", etc.)
    lang: str | None = None
    #: The internal text ID this block corresponds to.
    #: (Examples: "mula", "anuvada", "commentary", etc.)
    text: str | None = None

    # content attributes (verse, paragraph, etc.)
    #: The block's ordering ID ("43", "1.1", etc.)
    n: str | None = None
    #: If true, merge this block into the next one (e.g. if a block spans
    #: multiple pages.)
    merge_next: bool = False

    # footnote attributes
    #: the symbol that represents this footnote, e.g. "1".
    mark: str | None = None


@dc.dataclass
class StructuredPage:
    #: The page's blocks in order.
    blocks: list[Block]

    def _from_xml_string(text: str) -> "StructuredPage":
        # To prevent XML-based attacks
        root = DET.fromstring(text)
        if root.tag != "page":
            raise ValueError("Invalid root tag name")

        blocks = []
        for el in root:
            block_type = el.tag
            content = el.text or ""
            lang = el.get("lang", "sa")
            text = el.get("text", "")
            n = el.get("n", "")
            mark = el.get("mark", "")
            merge_next = el.get("merge-text", "false").lower() == "true"

            blocks.append(
                Block(
                    type=block_type,
                    content=content,
                    lang=lang,
                    text=text,
                    n=n,
                    mark=mark,
                    merge_next=merge_next,
                )
            )

        return StructuredPage(blocks=blocks)

    @staticmethod
    def from_string(text: str) -> "StructuredPage":
        try:
            return StructuredPage._from_xml_string(text)
        except Exception:
            pass

        text = text.strip()
        if not text:
            return StructuredPage(blocks=[])

        lines = [x.strip() for x in text.splitlines()]
        text_blocks = []
        cur = []
        for line in lines:
            if line:
                cur.append(line)
            else:
                if cur:
                    text_blocks.append("\n".join(cur))
                    cur = []
        if cur:
            text_blocks.append("\n".join(cur))

        blocks = []
        for content in text_blocks:
            language = detect_language(content)

            mark = None
            if content.startswith("[^"):
                block_type = "footnote"
                if m := re.match(r"^\[\^([^\]]+)\]\s*", content):
                    mark = m.group(1)
                    content = content[m.end() :]
            elif language == "sa" and is_verse(content):
                block_type = "verse"
            else:
                block_type = "p"

            blocks.append(
                Block(
                    type=block_type,
                    content=content,
                    lang=language,
                    n="",
                    text="",
                    mark=mark,
                )
            )
        return StructuredPage(blocks=blocks)

    def to_xml_string(self) -> str:
        root = ET.Element("page")
        root.text = "\n"
        for block in self.blocks:
            el = ET.SubElement(root, block.type)
            el.text = block.content.strip()
            if block.lang:
                el.set("lang", block.lang)
            if block.text:
                el.set("text", block.text)
            if block.n:
                el.set("n", block.n)
            if block.mark:
                el.set("mark", block.mark)
            if block.merge_next:
                el.set("merge-text", "true")
            el.tail = "\n"
        return ET.tostring(root, encoding="unicode")


def is_verse(text: str) -> bool:
    DANDA = "\u0964"
    DOUBLE_DANDA = "\u0965"
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    if len(lines) == 2:
        # 2 lines = 2 ardhas
        first_has_danda = DANDA in lines[0]
        second_has_double_danda = DOUBLE_DANDA in lines[1]
        return first_has_danda and second_has_double_danda

    elif len(lines) == 4:
        second_has_danda = DANDA in lines[1]
        fourth_has_double_danda = DOUBLE_DANDA in lines[3]
        return second_has_danda and fourth_has_double_danda

    else:
        return False


def detect_language(text: str) -> str:
    """Detect the text language with basic heuristics."""
    if not text or not text.strip():
        return "sa"

    devanagari_count = len(re.findall(r"[\u0900-\u097F]", text))
    latin_count = len(re.findall(r"[a-zA-Z]", text))

    # mostly latin --> mark as English
    if latin_count / len(text) > 0.5:
        return "en"

    tokens = set(text.split())

    hindi_markers = ["की", "में", "है", "हैं", "था", "थी", "थे", "नहीं", "और", "चाहिए"]
    if any(marker in tokens for marker in hindi_markers):
        return "hi"

    return "sa"
