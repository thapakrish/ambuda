"""Utilities for structuring text and converting it to TEI-confor XML."""

import copy
import dataclasses as dc
import defusedxml.ElementTree as DET
import re
import xml.etree.ElementTree as ET
from enum import StrEnum
from typing import Iterable

from ambuda import database as db


DEFAULT_PRINT_PAGE_NUMBER = "-"

# TODO:
# All numbers --> ignore
# Line break after "\d+ ||" and mark as verse
# scripts / macros
# directly type harvard kyoto?
# footnote (^\d+.)
# break apart multiple footnotes


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
    METADATA = "metadata"


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
    BlockType.METADATA: ValidationSpec(children=set(), attrib=set()),
    **{
        tag: ValidationSpec(children=set(InlineType), attrib=set())
        for tag in InlineType
    },
}


class TEITag(StrEnum):
    TITLE = "title"
    HEAD = "head"
    TRAILER = "trailer"

    LG = "lg"
    P = "p"

    SP = "sp"
    STAGE = "stage"
    SPEAKER = "stage"

    CHOICE = "choice"
    SEG = "seg"
    SIC = "sic"
    CORR = "corr"
    SUPPLIED = "supplied"

    REF = "ref"
    NOTE = "note"


# TODO: this is unused. hook it up somewhere.
TEI_XML_VALIDATION_SPEC = {
    TEITag.SP: ValidationSpec(
        children={TEITag.SPEAKER, TEITag.P, TEITag.LG, TEITag.STAGE}, attrib={"n"}
    ),
    TEITag.STAGE: ValidationSpec(children=set(), attrib={"rend"}),
    TEITag.LG: ValidationSpec(children={"l", "note", "choice", "ref"}, attrib={"n"}),
    TEITag.P: ValidationSpec(children={"note", "choice", "ref"}, attrib={"n"}),
    TEITag.CHOICE: ValidationSpec(
        children={TEITag.SEG, TEITag.CORR, TEITag.SIC}, attrib={"type"}
    ),
    TEITag.SEG: ValidationSpec(children=set(), attrib={"xml:lang"}),
    TEITag.HEAD: ValidationSpec(children=set(), attrib=set()),
    TEITag.TITLE: ValidationSpec(children=set(), attrib=set()),
    TEITag.TRAILER: ValidationSpec(children=set(), attrib=set()),
}


@dc.dataclass
class IndexedBlock:
    """A block of proofing XML, as seen during iteration."""

    # The revision this block comes from.
    revision: db.Revision
    # 1-indexed image number (for page numbers)
    image_number: int
    # 0-indexed block index within the page
    block_index: int
    # the raw page XML
    page_xml: ET.Element


class Filter:
    """Selects a project's blocks based on the given condition (`sexp`).

    Options:
    - `image` -- matches a range of image numberS
    - `label` -- matches blocks by label
    - `tag` -- matches blocks by tag (p, verse, ...)
    - `and` -- logical AND of multiple conditions
    - `or` -- logical OR of multiple conditions
    - `not` -- logical NOT of a condition
    """

    def __init__(self, sexp: str):
        sexp = sexp.strip()
        if not sexp:
            return []
        if not sexp.startswith("("):
            raise ValueError("S-expression must start with '('")

        i = 0

        def parse_list():
            nonlocal i
            result = []
            i += 1

            while i < len(sexp):
                while i < len(sexp) and sexp[i].isspace():
                    i += 1

                if i >= len(sexp):
                    raise ValueError("Unexpected end of input")

                if sexp[i] == ")":
                    i += 1
                    return result

                if sexp[i] == "(":
                    result.append(parse_list())
                else:
                    atom_start = i
                    while i < len(sexp) and sexp[i] not in "() \t\n\r":
                        i += 1
                    atom = sexp[atom_start:i]
                    result.append(atom)
            raise ValueError("Missing closing parenthesis")

        self.predicate = parse_list()

    def matches(self, block: IndexedBlock) -> bool:
        """Return whether `block` matches this filter's condition.

        If the filter is misconfigured, return `False`.
        """

        def _matches(sexp):
            try:
                key = sexp[0]
                if key == "image" or key == "page":
                    start = sexp[1]
                    try:
                        end = sexp[2]
                        return int(start) <= block.image_number <= int(end)
                    except IndexError:
                        return block.image_number == int(start)
                if key == "label":
                    return (
                        block.page_xml[block.block_index].attrib.get("text") == sexp[1]
                    )
                if key == "tag":
                    return block.page_xml[block.block_index].tag == sexp[1]

                if key == "and":
                    return all(_matches(x) for x in sexp[1:])
                if key == "or":
                    return any(_matches(x) for x in sexp[1:])
                if key == "not":
                    return not _matches(sexp[1])
            except Exception as e:
                return False

        return _matches(self.predicate)


def validate_proofing_xml(content: str) -> list[ValidationResult]:
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

    _validate_element(root, "page")
    return results


def _inner_xml(el):
    buf = [el.text or ""]
    for child in el:
        buf.append(ET.tostring(child, encoding="unicode"))
    return "".join(buf)


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

@dc.dataclass
class ProofProject:
    """A structured project from the proofreading environment."""

    pages: list[ProofPage]

    @staticmethod
    def from_revisions(revisions: list[db.Revision]) -> "ProofProject":
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


# TODO:
# - keep <l>-final "-" and mark as appropriate elem
# x concatenate within <sp> for speaker
#   - when building, reshape to partial "<sp>" for continuity
# - build footnote refs and check them with warnings
#   - <ref target="#X"> type="noteAnchor">
#   - <note xml:id="X" type="footnote">...</note>
def _rewrite_block_to_tei_xml(xml: ET.Element, image_number: int):
    # Text reshaping
    # TODO: move this elsewhere?
    for el in xml.iter():
        if el.tag == InlineType.STAGE:
            # Remove () for stage directions
            text = (el.text or "").strip()
            normed_text = re.sub(r"\(\s*(.*?)\s*\)", r"\1", text)
            if text != normed_text:
                el.attrib["rend"] = "parentheses"
            el.text = normed_text.strip()
            # add whitespace before following element
            if el.tail and el.tail[0] != " ":
                el.tail = " " + el.tail
        elif el.tag == InlineType.SPEAKER:
            # Remove trailing "-" for speakers
            text = el.text or ""
            text = re.sub(r"(.*?)\s*[-–]+\s*$", r"\1", text)
            el.text = text.strip()
        elif el.tag == InlineType.CHAYA:
            # Remove surrounding [ ] brackets.
            text = (el.text or "").strip()
            normed_text = re.sub(r"^\[\s*(.*)\s*\]", r"\1", text)
            if text != normed_text:
                el.attrib["rend"] = "brackets"
            el.text = normed_text.strip()
        elif el.tag == BlockType.VERSE:
            # Add consistent spacing around double dandas.
            text = (el.text or "").strip()
            text = re.sub(r"\s*॥\s*", " ॥ ", text)
            text = re.sub(r"॥ $", "॥", text)
            el.text = text
        elif el.tag == "ref":
            # Assign a temporary id for cross-referencing later.
            el.attrib["type"] = "noteAnchor"
            el.attrib["target"] = f"{image_number}.{el.text or ''}"

    # <speaker>
    try:
        speaker = next(x for x in xml if x.tag == InlineType.SPEAKER)
    except StopIteration:
        speaker = None
    if speaker is not None:
        old_tag = xml.tag
        old_attrib = xml.attrib
        old_children = [x for x in xml if x.tag != InlineType.SPEAKER]

        speaker_tail = speaker.tail or ""
        speaker.tail = ""

        xml.clear()
        xml.tag = "sp"
        xml.append(speaker)
        if not old_children and not speaker_tail.strip():
            # Special case: <p> contains only speaker, so don't create a child elem.
            return

        child = ET.SubElement(xml, old_tag, old_attrib)
        child.text = (speaker_tail + (xml[-1].text or "")).strip()
        child.extend(old_children)
        _rewrite_block_to_tei_xml(child, image_number)
        # Can lose other attrs, but must preserve n if present.
        if "n" in old_attrib:
            child.attrib["n"] = old_attrib["n"]
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
            has_counterpart = i + 1 < len(xml) and not el_tail
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

    if xml.tag == BlockType.VERSE:
        xml.tag = "lg"
    elif xml.tag == BlockType.HEADING:
        xml.tag = "head"
    elif xml.tag == BlockType.FOOTNOTE:
        xml.tag = "note"

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
            if "rend" in chaya.attrib:
                sanskrit.attrib["rend"] = chaya.attrib["rend"]
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
            for i, fragment in enumerate((el.tail or "").splitlines()):
                if i == 0:
                    el.tail = fragment.strip()
                else:
                    lines.append(ET.Element("l"))
                    lines[-1].text = fragment.strip()

        xml.text = ""
        xml.clear()
        xml.extend(lines)

    # At this point, block elements should have no attrib data left. Clean up lingering references
    # from our proofing XML.
    xml.attrib = {}
    if xml.tag == "note":
        xml.attrib["type"] = "footnote"


def _concatenate_tei_xml_blocks_across_page_boundary(
    first: ET.Element, second: ET.Element, page_number: str
):
    """Concatenate two blocks of TEI xml by updating the first block in-place.

    Use case: merging blocks across page breaks.
    """
    if first.tag == "sp":
        # Special case for <sp>: concatenate children, leaving speaker alone.
        assert len(first) == 2
        _concatenate_tei_xml_blocks_across_page_boundary(first[1], second, page_number)
        return

    pb = ET.SubElement(first, "pb")
    pb.attrib["n"] = page_number

    if first.tag in {"p", "lg"}:
        pb.tail = second.text or ""

    first.extend(second)


def create_tei_document(
    revisions: list[db.Revision], page_numbers: list[str], target: str
) -> tuple[TEIDocument, list[str], set[str]]:
    """Convert the project to a TEI document for publication.

    Approach:
    - rewrite proof XML into TEI XML. Proofing XML is more user-friendly than TEI XMl,
      and we're using it for now until we improve our editor.
    - stitch pages and blocks together, accounting for page breaks, fragments, etc.

    :param target: the name of the `text` to target. (A project may contain multiple texts.)
    :param page_numbers: a map from page index (e.g, 1, 2, 3) to the book's actual
        page numbers (ii, 4, etc.)

    :return: a tuple of (complete document, errors, set of page statuses used).
    """

    def _iter_blocks(revisions) -> Iterable[IndexedBlock]:
        """Iterate over all blocks in the given revisions."""
        for i, revision in enumerate(revisions):
            page_text = revision.content
            try:
                page_xml = DET.fromstring(page_text)
            except ET.ParseError:
                page_struct = ProofPage.from_content_and_page_id(page_text, 0)
                page_xml_str = page_struct.to_xml_string()
                page_xml = DET.fromstring(page_xml_str)

            image_number = i + 1
            for block_index, block in enumerate(page_xml):
                yield IndexedBlock(revision, image_number, block_index, page_xml)

    def _iter_filtered_blocks(revisions) -> Iterable[IndexedBlock]:
        if target.startswith("("):
            block_filter = Filter(target)
        else:
            # Legacy behavior.
            block_filter = Filter(f"(label {target})")

        for block in _iter_blocks(revisions):
            block_xml = block.page_xml[block.block_index]
            if block_xml.tag == BlockType.IGNORE:
                # Always skip "ignore" blocks.
                continue

            if block_filter.matches(block):
                yield block

    def _parse_metadata(text: str) -> dict[str, str]:
        ret = {}
        for line in text.splitlines():
            key, value = line.split("=")
            key = key.strip()
            value = value.strip()
            ret[key] = value
        return ret

    errors = []
    # n --> block
    element_map: dict[str, ET.Element] = {}
    # n -> <note> block
    footnote_map: dict[str, ET.Element] = {}
    # n --> page ID (for tying blocks to the pages they come from.)
    page_map: dict[str, int] = {}
    # str because this could be 1.1, etc.
    div_n: str = ""
    # tag -> last n for this tag type.
    block_ns: dict[str, str] = {}
    merge_next = None
    active_sp = None
    # Track page statuses
    page_statuses: set[str] = set()

    def _get_next_n(block_ns, tag):
        prev_n = block_ns.get(tag, f"{tag}0")
        if m := re.search(r"(.*?)(\d+)$", prev_n):
            n = f"{m.group(1)}{int(m.group(2)) + 1}"
        else:
            n = prev_n + "2"
        return n

    for block in _iter_filtered_blocks(revisions):
        proof_xml = block.page_xml[block.block_index]

        # Track page status
        revision = next((r for r in revisions if r.id == block.revision.id), None)
        if revision and revision.status:
            page_statuses.add(revision.status.name)

        if proof_xml.tag == BlockType.METADATA:
            # `metadata` contains special commands for this function.
            data = _parse_metadata(proof_xml.text or "")
            if "speaker" in data:
                # TODO: support other `speaker` settings.
                active_sp = None
            continue

        # Whether the block should merge into the next of the same type.
        should_merge_next = (
            proof_xml.get("merge-next", "false").lower() == "true"
            or proof_xml.get("merge-text", "false").lower() == "true"
        )

        # Deep copy to avoid mutating page XML
        tei_xml = copy.deepcopy(proof_xml)
        _rewrite_block_to_tei_xml(tei_xml, block.image_number)

        # TODO:
        # <lg> inheriting from previous n <-- should add +1
        # <sp><lg> <-- should be numbered
        # merge-next should merge into next of same type

        # Assign "n" to all blocks so that they have unique names (for translations, etc.)
        n = proof_xml.attrib.get("n")
        if tei_xml.tag in {"note"}:
            # Do nothing -- these elements should never have an "n" assigned.
            pass
        elif n and tei_xml.tag != "sp":
            # Explicit n is never added to `sp`, so skip.
            tei_xml.attrib["n"] = n
            block_ns[tei_xml.tag] = str(n)
        else:
            n = _get_next_n(block_ns, tei_xml.tag)
            tei_xml.attrib["n"] = n
            block_ns[tei_xml.tag] = n

            if tei_xml.tag == "sp":
                for child_xml in tei_xml:
                    if child_xml.tag == InlineType.SPEAKER:
                        # <speaker> should never be numbered.
                        continue
                    if "n" in child_xml.attrib:
                        # Don't overwrite existing `n`.
                        continue
                    n = _get_next_n(block_ns, child_xml.tag)
                    child_xml.attrib["n"] = n
                    block_ns[child_xml.tag] = n

        _has_no_text = not (tei_xml.text or "").strip()
        _has_one_stage_element = len(tei_xml) == 1 and tei_xml[0].tag == "stage"
        _stage_has_no_tail = len(tei_xml) == 1 and not (tei_xml[0].tail or "").strip()
        _is_stage_only = _has_no_text and _has_one_stage_element and _stage_has_no_tail
        if _is_stage_only:
            # TODO: how reliable is this?
            active_sp = None
        if tei_xml.tag == "sp":
            active_sp = None

        # Page number
        try:
            print_page_number = page_numbers[block.image_number]
        except IndexError:
            print_page_number = DEFAULT_PRINT_PAGE_NUMBER

        if merge_next is not None and (
            merge_next.tag == tei_xml.tag or merge_next.tag == "sp"
        ):
            # Only merge matching types (<p> and <p>, <lg> and <lg>, etc.)
            # Otherwise we get weird behavior, e.g. merging <lg> with <note>
            _concatenate_tei_xml_blocks_across_page_boundary(
                merge_next, tei_xml, print_page_number
            )
            merge_next = None
        else:
            if tei_xml.tag == "note":
                mark = proof_xml.attrib.get("mark")
                if mark:
                    note_name = f"{block.image_number}.{mark}"
                    footnote_map[note_name] = tei_xml
            elif n:
                page_map[n] = block.revision.page_id
                if tei_xml.tag in {"p", "lg"} and active_sp is not None:
                    active_sp.append(tei_xml)
                else:
                    element_map[n] = tei_xml

        if tei_xml.tag == "sp":
            active_sp = tei_xml

        if should_merge_next:
            merge_next = tei_xml

    # Insert footnotes where we find matches.
    if footnote_map:
        fn_n = 1
        for tei_block in element_map.values():
            for el in tei_block.iter():
                ref_target = el.attrib.get("target")
                if el.tag == "ref" and ref_target in footnote_map:
                    note_id = f"fn{fn_n}"
                    fn_n += 1
                    note = footnote_map[ref_target]
                    note.attrib["xml:id"] = note_id
                    el.attrib["target"] = f"#{note_id}"
                    tei_block.append(note)

    # Assemble blocks into a document.
    tei_sections = {}
    for block_slug, tree in element_map.items():
        assert block_slug in page_map
        page_id = page_map[block_slug]
        block = TEIBlock(
            xml=ET.tostring(tree, encoding="unicode"),
            slug=block_slug,
            page_id=page_id,
        )

        # HACK: for now, strip out footnote markup -- it's not supported
        # and it looks ugly.
        block.xml = re.sub(r"\[\^.*?\]", "", block.xml)

        section_n, _, verse_n = block_slug.rpartition(".")
        if not section_n:
            section_n = "all"

        if section_n in tei_sections:
            section = tei_sections[section_n]
        else:
            section = TEISection(slug=section_n, blocks=[])
            tei_sections[section_n] = section

        section.blocks.append(block)

    doc = TEIDocument(sections=list(tei_sections.values()))
    return (doc, errors, page_statuses)
