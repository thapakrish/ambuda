"""Utilities for manual text structuring."""

import dataclasses as dc
import defusedxml.ElementTree as DET
import re
import xml.etree.ElementTree as ET

from ambuda import database as db


DEFAULT_PRINT_PAGE_NUMBER = "-"
VALID_BLOCK_ELEMENTS = {
    "p",
    "verse",
    "footnote",
    "heading",
    "trailer",
    "title",
    "subtitle",
    "ignore",
}


def is_valid_page_xml(content: str) -> bool:
    try:
        root = DET.fromstring(content)
    except ET.ParseError:
        return False

    # Root tag should always be "page"
    if root.tag != "page":
        return False

    # Immediate children should be a known type (for dropdown support)
    for child in root:
        if child.tag not in VALID_BLOCK_ELEMENTS:
            return False

    return True


def _inner_xml(el):
    buf = [el.text or ""]
    for child in el:
        buf.append(ET.tostring(child, encoding="unicode"))
    return "".join(buf)


def _rewrite_to_tei_xml(xml):
    # TODO: footnote --> <note xml:id="..." type="footnote">
    # TODO: [^1] --> <ref target="#..." />
    for el in xml.iter():
        match el.tag:
            case "verse":
                el.tag = "lg"
            case "p":
                pass
            case "error":
                el.tag = "sic"
            case "fix":
                el.tag = "corr"
            case _:
                el.tag = None
                el.text = None


@dc.dataclass
class ProofBlock:
    """A block of structured content from the proofreading environment."""

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
    def from_revisions(revisions: list[str]):
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

        tei_tag_mapping = {
            "p": "p",
            "verse": "lg",
        }

        # TODO:
        # - generalize multi-line inline tag behavior for lg
        tree_map = {}
        page_map = {}
        for page_index, page, block in _iter_blocks():
            if block.type not in tei_tag_mapping:
                continue

            # Rewrite tags to match TEI
            #
            # TODO: double XML parse (once to create Block, once here.)
            # In the long run, use TEI XML as the backing store everywhere?
            block_xml = DET.fromstring(f"<{block.type}>{block.content}</{block.type}>")
            _rewrite_to_tei_xml(block_xml)

            tag_name = block_xml.tag
            if block.n in tree_map:
                root = tree_map[block.n]
            else:
                root = ET.Element(tag_name)
                root.attrib["n"] = block.n
                tree_map[block.n] = root
                page_map[block.n] = page.id

            root_has_children = len(root)
            root_has_text = root.text is not None

            try:
                print_page_number = page_numbers[page_index]
            except IndexError:
                print_page_number = DEFAULT_PRINT_PAGE_NUMBER

            match tag_name:
                case "lg":
                    if root.text:
                        errors.append("<lg> elements should have no direct text.")
                        continue

                    lines = [x.strip() for x in block.content.splitlines() if x.strip()]
                    # One <l> element per line.
                    for line in lines:
                        # HACK: inline markup. Use <p> so the root tag isn't cleared.
                        temp = DET.fromstring(f"<p>{line}</p>")
                        _rewrite_to_tei_xml(temp)

                        L = ET.SubElement(root, "l")
                        L.text = temp.text
                        L.extend(temp)
                case "p":
                    for el in block_xml.iter():
                        if el.text:
                            el.text = el.text.replace("-\n", "").replace("\n", " ")
                        if el.tail:
                            el.tail = el.tail.replace("-\n", "").replace("\n", " ")

                    if root_has_children or root_has_text:
                        pb = ET.SubElement(root, "pb")
                        pb.attrib["n"] = print_page_number
                        pb.tail = block_xml.text
                    else:
                        root.text = block_xml.text
                    root.extend(block_xml)

                case _:
                    pass

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
