"""Unit tests for ambuda.utils.text_exports."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from ambuda.utils.text_exports import create_plain_text


TEI_WITH_INVALID_XML_ID = """\
<?xml version="1.0" encoding="utf-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader/>
  <text xml:id="test" xml:lang="sa">
    <body>
      <div n="1">
        <lg xml:id="Ragh_1.34*" n="1.34">
          <l>rAmaH</l>
          <l>lakSmaNaH</l>
        </lg>
      </div>
    </body>
  </text>
</TEI>
"""


def test_create_plain_text_tolerates_invalid_xml_id():
    """create_plain_text should not crash on xml:id values like 'Ragh_1.34*'."""
    text = MagicMock()
    text.title = "Test"

    with tempfile.TemporaryDirectory() as tmp:
        xml_path = Path(tmp) / "test.xml"
        xml_path.write_text(TEI_WITH_INVALID_XML_ID)

        out_path = Path(tmp) / "test.txt"
        create_plain_text(text, out_path, xml_path)

        content = out_path.read_text()
        assert "rAmaH" in content
        assert "lakSmaNaH" in content
