"""Utilities for exporting texts in various formats."""

import csv
import logging
import tempfile
from enum import StrEnum
from functools import cached_property
from pathlib import Path
from xml.etree import ElementTree as ET

from lxml import etree
from pydantic import BaseModel
from sqlalchemy.orm import object_session
from vidyut.lipi import transliterate, Scheme

import ambuda.database as db
from ambuda.utils.datetime import utc_datetime_timestamp
from ambuda.s3_utils import S3Path


EXPORT_DIR = Path(__file__).parent
logger = logging.getLogger(__name__)


def font_directory(s3_bucket: str) -> Path:
    """Get a path to our font files, loading from S3 if necessary.

    :param s3_bucket: the S3 bucket name
    """

    temp_dir = Path(tempfile.gettempdir())
    fonts_dir = temp_dir / "ambuda_fonts"
    fonts_dir.mkdir(parents=True, exist_ok=True)

    font_path = fonts_dir / "NotoSerifDevanagari.ttf"
    if font_path.exists():
        logger.info(f"Font path exists: {font_path}")
        return fonts_dir

    try:
        # TODO: variable fonts are not supported well in typst.
        path = S3Path(
            s3_bucket, "assets/fonts/NotoSerifDevanagari-VariableFont_wdth,wght.ttf"
        )
        logger.info(f"Downloading font from S3: {path.path}")
        path.download_file(font_path)
    except Exception as e:
        logger.error(f"Exception while downloading font: {e}")
    return fonts_dir


def create_xml_file(text: db.Text, out_path: Path) -> None:
    """Create a TEI XML file from the given path.

    TEI XML is our canonical file export format from which all other exports are derived.
    It contains structured text data and rich metadata.
    """

    with etree.xmlfile(out_path, encoding="utf-8") as xf:
        xf.write_declaration()

        with xf.element("TEI", xmlns="http://www.tei-c.org/ns/1.0"):
            with xf.element("teiHeader"):
                with xf.element("fileDesc"):
                    with xf.element("title"):
                        xf.write(text.title)
                    with xf.element("author"):
                        xf.write(text.author.name if text.author else "(missing)")

                with xf.element("publicationStmt"):
                    with xf.element("publisher"):
                        xf.write("Ambuda (https://ambuda.org)")
                    with xf.element("availability"):
                        xf.write("TODO")

                with xf.element("notesStmt"):
                    with xf.element("note"):
                        if text.project_id is not None:
                            xf.write(
                                "This text has been created by direct export from Ambuda's proofing system."
                            )
                        else:
                            xf.write(
                                "This text has been created by third-party import from another site."
                            )

                with xf.element("encodingDesc"):
                    with xf.element("projectDesc"):
                        with xf.element("p"):
                            xf.write(
                                "Ambuda is an online library of Sanskrit literature."
                            )

            # Main text
            session = object_session(text)
            assert session
            with xf.element("text"):
                with xf.element("body"):
                    for section in text.sections:
                        for block in section.blocks:
                            el = etree.fromstring(block.xml)
                            el.set("n", block.slug)
                            xf.write(el)
                        session.expire(section)


def create_plain_text(text: db.Text, file_path: Path, xml_path: Path) -> None:
    timestamp = utc_datetime_timestamp()

    if not xml_path.exists():
        raise FileNotFoundError(
            f"XML file not found at {xml_path}. "
            "XML must be generated before plain text export."
        )

    with open(file_path, "w") as f:
        f.write(f"# {text.title}\n")
        f.write(f"# Exported from ambuda.org on {timestamp}\n\n")

        is_first = True
        for event, elem in etree.iterparse(str(xml_path), events=("end",)):
            parent = elem.getparent()
            if parent is not None and parent.tag == "{http://www.tei-c.org/ns/1.0}body":
                slug = elem.get("n")
                if slug:
                    if not is_first:
                        f.write("\n\n")
                    is_first = False

                    f.write(f"# {slug}\n")

                    elem_str = etree.tostring(elem, encoding="unicode")
                    xml = ET.fromstring(elem_str)
                    for el in xml.iter():
                        if el.tag == "l":
                            el.tail = "\n"
                        el.tag = None
                    f.write(ET.tostring(xml, encoding="unicode").strip())

                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]


def create_pdf(text: db.Text, file_path: Path, s3_bucket: str, xml_path: Path) -> None:
    """Create a PDF file from the given text.

    :param text: the text to export
    :param file_path: where to write the PDF
    :param s3_bucket: the S3 bucket name (needed for font downloads)
    :param xml_path: explicit path to XML file, if None will guess based on file_path
    """
    import typst

    timestamp = utc_datetime_timestamp()

    if not xml_path.exists():
        raise FileNotFoundError(
            f"XML file not found at {xml_path}. "
            "XML must be generated before PDF export."
        )

    template_path = Path(__file__).parent.parent / "templates/exports/document.typ"
    with open(template_path, "r") as f:
        template = f.read()

    # Just in case
    text_title = transliterate(text.title, Scheme.HarvardKyoto, Scheme.Devanagari)

    parts = template.split("{content}")
    header = parts[0].format(title=text_title, timestamp=timestamp)
    footer = parts[1] if len(parts) > 1 else ""

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".typ", delete=False
    ) as typst_file:
        temp_typst_path = typst_file.name

        typst_file.write(header)

        for event, elem in etree.iterparse(str(xml_path), events=("end",)):
            parent = elem.getparent()
            if parent is not None and parent.tag == "{http://www.tei-c.org/ns/1.0}body":
                slug = elem.get("n")
                if slug is not None:
                    typst_file.write(
                        f'#text(size: 9pt, fill: rgb("#666666"))[{slug}]\n\n'
                    )

                    elem_str = etree.tostring(elem, encoding="unicode")
                    elem_copy = ET.fromstring(elem_str)
                    for el in elem_copy.iter():
                        if el.tag == "l":
                            el.tail = " \\\n" + (el.tail or "")
                        el.tag = None
                    content = ET.tostring(elem_copy, encoding="unicode").strip()

                    # Escape Typst special characters
                    content = content.replace("*", r"\*")

                    typst_file.write("#sa[\n")
                    typst_file.write(content)
                    typst_file.write("\n]\n\n")

                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

        typst_file.write(footer)

    try:
        font_paths = [font_directory(s3_bucket)]
        _, warnings = typst.compile_with_warnings(
            temp_typst_path,
            font_paths=font_paths,
            output=file_path,
        )
        for warning in warnings:
            logger.info(f"Typst warning: {warning.message}")
            logger.info(f"Typst trace: {warning.trace}")
    finally:
        if Path(temp_typst_path).exists():
            Path(temp_typst_path).unlink()


def maybe_create_tokens(text: db.Text, out_path: Path) -> None:
    session = object_session(text)
    assert session

    has_data = False
    with open(out_path, "w") as f:
        writer = csv.writer(f, delimiter=",")

        results = (
            session.query(db.BlockParse, db.TextBlock.slug)
            .join(db.TextBlock, db.BlockParse.block_id == db.TextBlock.id)
            .filter(db.BlockParse.text_id == text.id)
            .yield_per(1000)
        )

        for block_parse, block_slug in results:
            for line in block_parse.data.splitlines():
                fields = line.split("\t")
                if len(fields) != 3:
                    continue

                form, base, parse_data = fields
                parse_data = parse_data.replace(",", " ")
                writer.writerow([block_slug, form, base, parse_data])
                has_data = True

            session.expire(block_parse)

    if not has_data:
        out_path.unlink()


class ExportType(StrEnum):
    XML = "xml"
    PLAIN_TEXT = "plain-text"
    PDF = "pdf"
    TOKENS = "tokens"


class ExportConfig(BaseModel):
    label: str
    type: ExportType
    slug_pattern: str
    mime_type: str

    def slug(self, text: db.Text) -> str:
        return self.slug_pattern.format(text.slug)

    @cached_property
    def suffix(self) -> str:
        return self.slug_pattern.format("")

    def matches(self, filename: str) -> bool:
        return filename.endswith(self.suffix)


EXPORTS = [
    ExportConfig(
        label="XML",
        type=ExportType.XML,
        slug_pattern="{}.xml",
        mime_type="application/xml",
    ),
    ExportConfig(
        label="Plain text",
        type=ExportType.PLAIN_TEXT,
        slug_pattern="{}.txt",
        mime_type="text/csv",
    ),
    ExportConfig(
        label="PDF (Devanagari)",
        type=ExportType.PDF,
        slug_pattern="{}-devanagari.pdf",
        mime_type="application/pdf",
    ),
    ExportConfig(
        label="Token data (CSV)",
        type=ExportType.TOKENS,
        slug_pattern="{}-tokens.csv",
        mime_type="text/csv",
    ),
]
