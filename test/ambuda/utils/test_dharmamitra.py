import pytest
from ambuda.utils.dharmamitra import *


@pytest.mark.parametrize(
    "input,expected",
    [
        # Avyaya
        ("", "pada=a"),
        # Subantas
        (
            "Case=Nom|Gender=Masc|Number=Sing|Tense=Fut|VerbForm=Part",
            "pada=sup krt=lrt-sat li=pum vi=1 va=eka",
        ),
        (
            "Case=Nom|Gender=Masc|Number=Sing|VerbForm=Part",
            "pada=sup krt=nistha li=pum vi=1 va=eka",
        ),
        (
            "Case=Nom|Gender=Masc|Number=Sing|Tense=Pres|VerbForm=Part",
            "pada=sup krt=sat li=pum vi=1 va=eka",
        ),
        # Tinantas
        (
            "Tense=Past|Mood=Ind|Person=3|Number=Sing",
            "pada=tin la=lit pu=pra va=eka",
        ),
        (
            "Tense=Past|Mood=Ind|Person=2|Number=Dual",
            "pada=tin la=lit pu=ma va=dvi",
        ),
        (
            "Tense=Past|Mood=Ind|Person=1|Number=Plur",
            "pada=tin la=lit pu=u va=bahu",
        ),
        # Lakaras
        (
            "Tense=Pres|Mood=Opt|Person=3|Number=Sing",
            "pada=tin la=lin pu=pra va=eka",
        ),
    ],
)
def test_remapping(input, expected):
    map = parse_dharmamitra_tags(input)
    map = remap_dharmamitra_tags(map)
    actual = " ".join(f"{k}={v}" for k, v in map.items())
    assert expected == actual
