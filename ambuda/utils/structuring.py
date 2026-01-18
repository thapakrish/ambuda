"""Utilities for manual text structuring."""

import dataclasses as dc
import defusedxml.ElementTree as DET
import re
import xml.etree.ElementTree as ET
from enum import StrEnum

from ambuda import database as db


DEFAULT_PRINT_PAGE_NUMBER = "-"


# Keep in sync with prosemirror-editor.ts::BLOCK_TYPES
class BlockType(StrEnum):
    PARAGRAPH = "p"
    VERSE = "verse"
    FOOTNOTE = "footnote"
    HEADING = "heading"
    TRAILER = "trailer"
    TITLE = "title"
    SUBTITLE = "subtitle"
    IGNORE = "ignore"


# Keep in sync with marks-config.ts::INLINE_MARKS
class InlineType(StrEnum):
    ERROR = "error"
    FIX = "fix"
    SPEAKER = "speaker"
    STAGE = "stage"
    REF = "ref"
    FLAG = "flag"
    CHAYA = "chaya"


@dc.dataclass
class ValidationSpec:
    children: set[str]
    attrib: set[str]


class ValidationType(StrEnum):
    WARNING = "warning"
    ERROR = "error"


@dc.dataclass
class ValidationResult:
    type: ValidationType
    message: str

    @staticmethod
    def error(message: str) -> "ValidationResult":
        return ValidationResult(type=ValidationType.ERROR, message=message)

    @staticmethod
    def warning(message: str) -> "ValidationResult":
        return ValidationResult(type=ValidationType.WARNING, message=message)


CORE_INLINE_TYPES = set(InlineType)
VALIDATION_SPECS = {
    "page": ValidationSpec(children=set(BlockType), attrib=set()),
    BlockType.PARAGRAPH: ValidationSpec(
        children=CORE_INLINE_TYPES,
        attrib={"lang", "text", "n", "merge-next", "merge-text"},
    ),
    BlockType.VERSE: ValidationSpec(
        children=CORE_INLINE_TYPES,
        attrib={"lang", "text", "n", "merge-next", "merge-text"},
    ),
    BlockType.FOOTNOTE: ValidationSpec(
        children=CORE_INLINE_TYPES, attrib={"lang", "text", "mark"}
    ),
    BlockType.HEADING: ValidationSpec(
        children=CORE_INLINE_TYPES, attrib={"lang", "text", "n"}
    ),
    BlockType.TRAILER: ValidationSpec(
        children=CORE_INLINE_TYPES, attrib={"lang", "text", "n"}
    ),
    BlockType.TITLE: ValidationSpec(
        children=CORE_INLINE_TYPES, attrib={"lang", "text", "n"}
    ),
    BlockType.SUBTITLE: ValidationSpec(
        children=CORE_INLINE_TYPES, attrib={"lang", "text", "n"}
    ),
    BlockType.IGNORE: ValidationSpec(
        children=CORE_INLINE_TYPES, attrib={"lang", "text"}
    ),
    **{
        tag: ValidationSpec(children=set(InlineType), attrib=set())
        for tag in InlineType
    },
}


def validate_page_xml(content: str) -> list[ValidationResult]:
    results = []

    try:
        root = DET.fromstring(content)
    except ET.ParseError as e:
        results.append(ValidationResult.error(f"XML parse error: {e}"))
        return results

    # Root tag should always be "page"
    if root.tag != "page":
        results.append(
            ValidationResult.error(f"Root tag must be 'page', got '{root.tag}'")
        )
        return results

    def _validate_element(el, tag, path=()):
        current_path = path + (tag,)

        if tag not in VALIDATION_SPECS:
            results.append(
                ValidationResult.error(
                    f"Unknown element '{tag}' at {'/'.join(current_path)}"
                )
            )
            return

        spec = VALIDATION_SPECS[tag]

        for attr in el.attrib:
            if attr not in spec.attrib:
                results.append(
                    ValidationResult.error(
                        f"Unexpected attribute '{attr}' on element '{tag}' at {'/'.join(current_path)}"
                    )
                )

        for child in el:
            if child.tag not in spec.children:
                results.append(
                    ValidationResult.error(
                        f"Unexpected child element '{child.tag}' in '{tag}' at {'/'.join(current_path)}"
                    )
                )
            _validate_element(child, child.tag, current_path)

    # Immediate children should be a known type (for dropdown support)
    _validate_element(root, "page")

    return results


def _inner_xml(el):
    buf = [el.text or ""]
    for child in el:
        buf.append(ET.tostring(child, encoding="unicode"))
    return "".join(buf)


def _rewrite_block_to_tei_xml(xml: ET.Element):
    # <speaker>
    try:
        speaker = next(x for x in xml if x.tag == "speaker")
    except StopIteration:
        speaker = None
    if speaker is not None:
        old_tag = xml.tag
        old_attrib = xml.attrib
        old_children = [x for x in xml if x.tag != "speaker"]

        speaker_tail = speaker.tail or ""
        speaker.tail = ""

        xml.clear()
        xml.tag = "sp"
        xml.append(speaker)
        if not old_children and not speaker_tail.strip():
            # Special case: <p> contains only speaker, so don't create a child elem.
            return

        xml.append(ET.Element(old_tag))
        xml[-1].attrib = old_attrib
        xml[-1].text = (speaker_tail + (xml[-1].text or "")).strip()
        xml[-1].extend(old_children)
        _rewrite_block_to_tei_xml(xml[-1])
        return

    # <error> and <fix>
    i = 0
    while i < len(xml):
        el = xml[i]
        el_tail = (el.tail or "").strip()
        if el.tag not in ("error", "fix"):
            i += 1
            continue

        # Standardize order: <error> then <fix>
        if i + 1 < len(xml):
            el_next = xml[i + 1]
            if (el.tag, el_next.tag) == ("fix", "error") and not el_tail:
                # Normalize to avoid weird errors later.
                el.tail = ""
                xml[i], xml[i + 1] = el_next, el

        # Reload since `el` may be stale after swap.
        el = xml[i]
        if el.tag == "error":
            error = xml[i]
            has_counterpart = (i + 1 < len(xml) and not el_tail)
            maybe_fix = xml[i + 1] if has_counterpart else None

            choice = ET.Element("choice")
            sic = ET.SubElement(choice, "sic")
            sic.text = el.text or ""
            corr = ET.SubElement(choice, "corr")
            if maybe_fix is not None:
                corr.text = maybe_fix.text or ""
                choice.tail = maybe_fix.tail
                del xml[i + 1]
            else:
                choice.tail = error.tail
            del xml[i]

            xml.insert(i, choice)
        elif el.tag == "fix":
            # Edge case: <fix> without <error> is renamed to <supplied>.
            el.tag = "supplied"

        i += 1

    tag_rename = {
        "verse": "lg",
        "heading": "head",
    }
    xml.tag = tag_rename.get(xml.tag, xml.tag)

    # <p> text normalization
    if xml.tag == "p":

        def _normalize_text(xml):
            xml.text = re.sub(r"-\n", "", xml.text or "", flags=re.M)
            xml.text = re.sub(r"\s+", " ", xml.text, flags=re.M)
            xml.tail = re.sub(r"-\n", "", xml.tail or "", flags=re.M)
            xml.tail = re.sub(r"\s+", " ", xml.tail, flags=re.M)
            for el in xml:
                _normalize_text(el)

        _normalize_text(xml)

        # <chaya> is currently supported only for <p> elements.
        try:
            chaya = next(x for x in xml if x.tag == "chaya")
        except StopIteration:
            chaya = None
        if chaya is not None:
            choice = ET.Element("choice")
            choice.attrib["type"] = "chaya"

            prakrit = ET.SubElement(choice, "seg")
            prakrit.attrib["xml:lang"] = "pra"
            prakrit.text = xml.text
            prakrit.extend(x for x in xml if x.tag != "chaya")

            sanskrit = ET.SubElement(choice, "seg")
            sanskrit.attrib["xml:lang"] = "sa"
            sanskrit.text = chaya.text
            sanskrit.extend(chaya)

            xml.text = ""
            del xml[:]
            xml.append(choice)

    # <verse> line splitting
    if xml.tag == "lg":
        lines = []
        for fragment in (xml.text or "").strip().splitlines():
            line = ET.Element("l")
            line.text = fragment
            lines.append(line)

        for el in xml:
            if not lines:
                lines.append(ET.Element("l"))
            lines[-1].append(el)
            for i, fragment in enumerate((el.tail or "").strip().splitlines()):
                if i == 0:
                    el.tail = fragment
                else:
                    lines.append(ET.Element("l"))
                    lines[-1].text = fragment

        xml.text = ""
        xml.clear()
        xml.extend(lines)


def _concatenate_tei_xml_blocks(first: ET.Element, second: ET.Element, page_number: str):
    """Concatenate two blocks of TEI xml by updating the first block in-place.

    Use case: merging blocks across page breaks.
    """
    if first.tag == "sp":
        # Special case for <sp>: concatenate children, leaving speaker alone.
        assert len(first) == 2
        _concatenate_tei_xml_blocks(first[1], second, page_number)
        return

    if first.tag in {"p", "lg"}:
        pb = ET.SubElement(first, "pb")
        pb.attrib["n"] = page_number
        pb.tail = second.text or ""

    first.extend(second)


@dc.dataclass
class ProofBlock:
    """A block of structured content from the proofreading environment."""

    #: The block's type (paragraph, verse, etc.)
    type: str
    #: The block payload.
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
class ProofPage:
    """A page of structured content from the proofing environment."""

    #: The page's database ID (for cross-referencing)
    id: int
    #: The page's blocks in order.
    blocks: list[ProofBlock]

    def _from_xml_string(content: str, page_id: int) -> "ProofPage":
        # To prevent XML-based attacks
        root = DET.fromstring(content)
        if root.tag != "page":
            raise ValueError("Invalid root tag name")

        blocks = []
        for el in root:
            block_type = el.tag
            el_content = _inner_xml(el)
            lang = el.get("lang", None)
            text = el.get("text", None)
            n = el.get("n", None)
            mark = el.get("mark", None)
            # Earlier versions had a typo "merge-text", so continue to support it until all old
            # projects are migrated off.
            merge_next = (
                el.get("merge-next", "false").lower() == "true"
                or el.get("merge-text", "false").lower() == "true"
            )

            blocks.append(
                ProofBlock(
                    type=block_type,
                    content=el_content,
                    lang=lang,
                    text=text,
                    n=n,
                    mark=mark,
                    merge_next=merge_next,
                )
            )

        return ProofPage(id=page_id, blocks=blocks)

    @staticmethod
    def from_revision(revision: db.Revision) -> "ProofPage":
        text = revision.content.strip()
        return ProofPage.from_content_and_page_id(text, revision.page_id)

    @staticmethod
    def from_content_and_page_id(text: str, page_id: int) -> "ProofPage":
        """Exposed for `def structuring_api`"""
        try:
            return ProofPage._from_xml_string(text, page_id)
        except Exception:
            pass

        if not text:
            return ProofPage(blocks=[], id=page_id)

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
                ProofBlock(
                    type=block_type,
                    content=content,
                    lang=language,
                    n=None,
                    text=None,
                    mark=mark,
                )
            )
        return ProofPage(id=page_id, blocks=blocks)

    def to_xml_string(self) -> str:
        root = ET.Element("page")
        root.text = "\n"
        for block in self.blocks:
            el = ET.SubElement(root, block.type)
            content = block.content.strip().replace("&", "&amp;")
            try:
                temp_wrapper = DET.fromstring(f"<temp>{content}</temp>")
            except Exception:
                temp_wrapper = ET.Element("temp")
                temp_wrapper.text = content

            el.text = temp_wrapper.text
            for child in temp_wrapper:
                el.append(child)

            if block.lang:
                el.set("lang", block.lang)
            if block.text:
                el.set("text", block.text)
            if block.n:
                el.set("n", block.n)
            if block.mark:
                el.set("mark", block.mark)
            if block.merge_next:
                el.set("merge-next", "true")
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
    if latin_count / len(text) > 0.90:
        return "en"

    tokens = set(text.split())

    hindi_markers = ["की", "में", "है", "हैं", "था", "थी", "थे", "नहीं", "और", "चाहिए"]
    if any(marker in tokens for marker in hindi_markers):
        return "hi"

    return "sa"


@dc.dataclass
class TEIBlock:
    """A structured block in TEI XML (publication-ready)."""

    xml: str
    slug: str
    page_id: int


@dc.dataclass
class TEISection:
    """A structured block section in TEI XML (publication-ready)."""

    slug: str
    blocks: list[TEIBlock]


@dc.dataclass
class TEIDocument:
    """A structured document in TEI XML (publication-ready)."""

    sections: list[TEISection]


@dc.dataclass
class ProofProject:
    """A structured project from the proofreading environment."""

    pages: list[ProofPage]

    @staticmethod
    def from_revisions(revisions: list[db.Revision]):
        """Create structured data from a project's latest revisions."""
        pages = []
        for revision in revisions:
            try:
                page = ProofPage._from_xml_string(revision.content, revision.page_id)
            except Exception as e:
                continue

            page.id = revision.page_id
            pages.append(page)

        return ProofProject(pages=pages)

    def to_tei_document(
        self, target: str, page_numbers: list[str]
    ) -> tuple[TEIDocument, list[str]]:
        """Convert the project to a TEI document for publication.

        Approach:
        - rewrite proof XML into TEI XML. Proofing XML is more user-friendly than TEI XMl,
          and we're using it for now until we improve our editor.
        - stitch pages and blocks together, accounting for page breaks, fragments, etc.

        :param target: the name of the `text` to target. (A project may contain multiple texts.)
        :param page_numbers: a map from page index (e.g, 1, 2, 3) to the book's actual
            page numbers (ii, 4, etc.)

        :return: a complete document.
        """

        errors = []

        def _iter_blocks():
            for i, page in enumerate(self.pages):
                for block in page.blocks:
                    if block.text == target and block.n:
                        yield (i, page, block)

        # TODO:
        # - generalize multi-line inline tag behavior for lg

        # n --> block
        tree_map = {}
        # n --> page ID
        page_map = {}
        for page_index, page, block in _iter_blocks():
            if block.type not in {"p", "verse"}:
                continue

            try:
                print_page_number = page_numbers[page_index]
            except IndexError:
                print_page_number = DEFAULT_PRINT_PAGE_NUMBER

            block_xml = DET.fromstring(f"<{block.type}>{block.content}</{block.type}>")
            _rewrite_block_to_tei_xml(block_xml)

            tag_name = block_xml.tag
            if block.n in tree_map:
                first = tree_map[block.n]
                _concatenate_tei_xml_blocks(first, block_xml, print_page_number)
            else:
                block_xml.attrib["n"] = block.n
                tree_map[block.n] = block_xml
                page_map[block.n] = page.id

        tei_sections = {}
        for block_slug, tree in tree_map.items():
            page_id = page_map.get(block_slug)
            block = TEIBlock(
                xml=ET.tostring(tree, encoding="unicode"),
                slug=block_slug,
                page_id=page_id,
            )

            # HACK: for now, strip out footnote markup -- it's not supported
            # and it looks ugly.
            block.xml = re.sub(r"\[\^.*?\]", "", block.xml)

            section_n, _, block_n = block_slug.rpartition(".")
            if not section_n:
                section_n = "all"

            if section_n in tei_sections:
                section = tei_sections[section_n]
            else:
                section = TEISection(slug=section_n, blocks=[])
                tei_sections[section_n] = section

            section.blocks.append(block)

        doc = TEIDocument(sections=list(tei_sections.values()))
        return (doc, errors)
