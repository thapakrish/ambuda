import re
import xml.etree.ElementTree as ET

import pytest

from ambuda.utils import structuring as s


P = s.ProofPage
B = s.ProofBlock


@pytest.mark.parametrize(
    "input,expected",
    [
        ("<page></page>", []),
        # OK: block elements
        *[
            (f"<page><{tag}>foo</{tag}></page>", [])
            for tag in [
                "p",
                "verse",
                "footnote",
                "heading",
                "trailer",
                "title",
                "subtitle",
            ]
        ],
        # OK: inline elements
        *[
            (f"<page><p><{tag}>foo</{tag}></p></page>", [])
            for tag in ["error", "fix", "speaker", "stage", "ref", "flag", "chaya"]
        ],
        # ERR: unknown or unexpected tag
        ("<foo></foo>", ["must be 'page'"]),
        ("<page><unk>foo</unk></page>", ["Unexpected.*unk", "Unknown.*unk"]),
        ("<page><p><unk>foo</unk></p></page>", ["Unexpected.*unk", "Unknown.*unk"]),
        ("<page><p><verse>foo</verse></p></page>", ["Unexpected.*verse"]),
        # ERR: unknown or unexpected attribute
        ("<page unk='foo'></page>", ["Unexpected attribute.*unk"]),
        ("<page><p unk='foo'>foo</p></page>", ["Unexpected attribute.*unk"]),
    ],
)
def test_validate_page_xml(input, expected):
    actual = s.validate_page_xml(input)

    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert re.search(e, a.message)


@pytest.mark.parametrize(
    "input,expected",
    [
        # Basic usage
        ("<p>foo</p>", "<p>foo</p>"),
        ("<heading>foo</heading>", "<head>foo</head>"),
        ("<title>foo</title>", "<title>foo</title>"),
        ("<trailer>foo</trailer>", "<trailer>foo</trailer>"),
        # Other block types do not have a spec, so skip them for now.
        # <p>
        # <p> joins together text spread across multiple lines.
        ("<p>foo \nbar</p>", "<p>foo bar</p>"),
        ("<p>foo\nbar</p>", "<p>foo bar</p>"),
        ("<p>foo \n bar</p>", "<p>foo bar</p>"),
        # `-` at the end of a line joins words together across lines.
        ("<p>foo-\nbar</p>", "<p>foobar</p>"),
        ("<p>foo-bar\nbiz</p>", "<p>foo-bar biz</p>"),
        # <p> should respect and retain inline marks when joining text.
        ("<p><fix>foo</fix> \n bar</p>", "<p><supplied>foo</supplied> bar</p>"),
        # <lg>
        # <lg> breaks down lines (separated by whitespace) into separate <l> elements.
        ("<verse>foo</verse>", "<lg><l>foo</l></lg>"),
        ("<verse>foo\nbar</verse>", "<lg><l>foo</l><l>bar</l></lg>"),
        ("<verse>foo\nbar\nbiz</verse>", "<lg><l>foo</l><l>bar</l><l>biz</l></lg>"),
        # <lg> should respect and retain inline marks when splitting lines.
        (
            "<verse>f<fix>oo</fix>oo\nbar</verse>",
            "<lg><l>f<supplied>oo</supplied>oo</l><l>bar</l></lg>",
        ),
        # TODO: too hard
        # ("<verse>f<fix>oo\nbar</fix> biz</verse>", "<lg><l>f<supplied>oo</supplied></l><l><supplied>bar</supplied> biz</l></lg>"),
        # <error> and <fix>
        # Error and fix consecutively (despite whitespace) --> sic and corr
        (
            "<p>foo<error>bar</error> <fix>biz</fix> tail</p>",
            "<p>foo<choice><sic>bar</sic><corr>biz</corr></choice> tail</p>",
        ),
        # Invariant to order.
        (
            "<p>foo<fix>biz</fix> <error>bar</error></p>",
            "<p>foo<choice><sic>bar</sic><corr>biz</corr></choice></p>",
        ),
        # Error alone --> sic, with empty corr
        (
            "<p>foo<error>bar</error> tail</p>",
            "<p>foo<choice><sic>bar</sic><corr /></choice> tail</p>",
        ),
        # Fix alone --> supplied (no corr)
        ("<p>foo<fix>bar</fix></p>", "<p>foo<supplied>bar</supplied></p>"),
        # Separate fix and error -- don't group into a single choice
        ("<p>foo<error>bar</error> biz <fix>baf</fix> tail</p>", "<p>foo<choice><sic>bar</sic><corr /></choice> biz <supplied>baf</supplied> tail</p>"),
        # <chaya>
        (
            "<p>aoeu<x>foo</x><chaya>asdf<y>bar</y></chaya></p>",
            '<p><choice type="chaya"><seg xml:lang="pra">aoeu<x>foo</x></seg><seg xml:lang="sa">asdf<y>bar</y></seg></choice></p>',
        ),
        # <speaker> converts the block type to <sp>. <speaker> is yanked out of the block into <sp>,
        # preserving element order. The old block type is appended as a child to <sp>.
        ("<p><speaker>foo</speaker></p>", "<sp><speaker>foo</speaker></sp>"),
        (
            "<p><speaker>foo</speaker>bar-\nbiz</p>",
            "<sp><speaker>foo</speaker><p>barbiz</p></sp>",
        ),
        # No content --> don't preserve the <p>.
        ("<p> <speaker>foo</speaker> </p>", "<sp><speaker>foo</speaker></sp>"),
        (
            "<verse><speaker>foo</speaker>bar</verse>",
            "<sp><speaker>foo</speaker><lg><l>bar</l></lg></sp>",
        ),
    ],
)
def test_rewrite_block_to_tei_xml(input, expected):
    xml = ET.fromstring(input)
    s._rewrite_block_to_tei_xml(xml)
    actual = ET.tostring(xml)
    assert expected.encode("utf-8") == actual


@pytest.mark.parametrize(
    "input,expected",
    [
        ("<p>test</p>", "test"),
        ("<p>test <a>foo</a></p>", "test <a>foo</a>"),
        ("<p>test <a>foo</a> bar</p>", "test <a>foo</a> bar"),
        ("<p><a>foo</a> bar</p>", "<a>foo</a> bar"),
        ("<p><a>foo</a> <b>bar</b></p>", "<a>foo</a> <b>bar</b>"),
        # Unicode
        ("<p>अ <a>अ</a> अ</p>", "अ <a>अ</a> अ"),
    ],
)
def test_inner_xml(input, expected):
    assert s._inner_xml(ET.fromstring(input)) == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        ("<page></page>", P(id=0, blocks=[])),
        (
            "<page><verse>अ</verse></page>",
            P(id=0, blocks=[B(type="verse", content="अ")]),
        ),
        (
            "<page><p>अ</p></page>",
            P(id=0, blocks=[B(type="p", content="अ")]),
        ),
        (
            "<page><p>अ<b>अ</b></p></page>",
            P(id=0, blocks=[B(type="p", content="अ<b>अ</b>")]),
        ),
        (
            '<page><p merge-next="true">अ</p></page>',
            P(id=0, blocks=[B(type="p", content="अ", merge_next=True)]),
        ),
        (
            '<page><p merge-next="false">अ</p></page>',
            P(id=0, blocks=[B(type="p", content="अ", merge_next=False)]),
        ),
        # Legacy behavior
        (
            '<page><p merge-text="true">अ</p></page>',
            P(id=0, blocks=[B(type="p", content="अ", merge_next=True)]),
        ),
    ],
)
def test_from_xml_string(input, expected):
    assert s.ProofPage._from_xml_string(input, 0) == expected


def test_from_content_and_page_id():
    text = """
    अ

    क<error></error><fix>ख</fix>ग

    अ ।
    क ॥

    [^1] क
    """
    text = "\n".join(x.strip() for x in text.splitlines())
    assert s.ProofPage.from_content_and_page_id(text, 0) == P(
        id=0,
        blocks=[
            B(type="p", content="अ", lang="sa"),
            B(type="p", content="क<error></error><fix>ख</fix>ग", lang="sa"),
            B(type="verse", content="अ ।\nक ॥", lang="sa"),
            B(type="footnote", content="क", lang="sa", mark="1"),
        ],
    )


@pytest.mark.parametrize(
    "input,expected",
    [
        # Basic <p>
        (
            P(id=0, blocks=[B(type="p", content="अ", n="1")]),
            s.TEIBlock(xml='<p n="1">अ</p>', slug="1", page_id=0),
        ),
        # <p> with concatenation
        (
            P(
                id=0,
                blocks=[
                    B(type="p", content="अ", n="1", merge_next=True),
                    B(type="p", content="a", n="1"),
                ],
            ),
            s.TEIBlock(xml='<p n="1">अ<pb n="-" />a</p>', slug="1", page_id=0),
        ),
        # <p> with speaker
        (
            P(
                id=0,
                blocks=[
                    B(type="p", content="<speaker>foo</speaker> अ", n="1"),
                ],
            ),
            s.TEIBlock(
                xml='<sp n="1"><speaker>foo</speaker><p>अ</p></sp>', slug="1", page_id=0
            ),
        ),
        # <p> with speaker and concatenation
        (
            P(
                id=0,
                blocks=[
                    B(
                        type="p",
                        content="<speaker>foo</speaker> अ",
                        n="1",
                        merge_next=True,
                    ),
                    B(type="p", content="a", n="1"),
                ],
            ),
            s.TEIBlock(
                xml='<sp n="1"><speaker>foo</speaker><p>अ<pb n="-" />a</p></sp>',
                slug="1",
                page_id=0,
            ),
        ),
        # Basic <verse>
        (
            P(id=0, blocks=[B(type="verse", content="अ", n="1")]),
            s.TEIBlock(xml='<lg n="1"><l>अ</l></lg>', slug="1", page_id=0),
        ),
        # <verse> with concatenation
        (
            P(
                id=0,
                blocks=[
                    B(type="verse", content="अ", n="1", merge_next=True),
                    B(type="verse", content="a", n="1"),
                ],
            ),
            s.TEIBlock(xml='<lg n="1"><l>अ</l><pb n="-" /><l>a</l></lg>', slug="1", page_id=0),
        ),
        # Other
        (
            P(
                id=0,
                blocks=[
                    B(type="verse", content="अ<fix>क</fix>ख", n="1"),
                ],
            ),
            s.TEIBlock(
                xml='<lg n="1"><l>अ<supplied>क</supplied>ख</l></lg>',
                slug="1",
                page_id=0,
            ),
        ),
        (
            P(id=0, blocks=[B(type="p", content="अ<fix>क</fix>ख", n="1")]),
            s.TEIBlock(
                xml='<p n="1">अ<supplied>क</supplied>ख</p>', slug="1", page_id=0
            ),
        ),
    ],
)
def test_to_tei_document(input, expected):
    tei_doc, _errors = s.ProofProject(pages=[input]).to_tei_document(None, [])
    tei_block = tei_doc.sections[0].blocks[0]
    assert tei_block == expected
