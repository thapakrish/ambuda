import functools
from pathlib import Path

from flask import current_app
from pydantic import BaseModel, TypeAdapter
from vidyut.kosha import Kosha, PadaEntry, PratipadikaEntry
from vidyut.lipi import transliterate, Scheme
from vidyut.prakriya import Lakara

from ambuda.utils.kosha import get_kosha


class AmbudaToken(BaseModel):
    form: str
    base: str
    parse: str


class DharmamitraToken(BaseModel):
    unsandhied: str
    lemma: str
    tag: str
    meanings: list[str]


class DharmamitraSentence(BaseModel):
    sentence: str
    grammatical_analysis: list[DharmamitraToken]


DharmamitraResponse = TypeAdapter(list[DharmamitraSentence])

remapping = {
    "Case": {
        "Nom": "vi=1",
        "Acc": "vi=2",
        "Ins": "vi=3",
        "Dat": "vi=4",
        "Abl": "vi=5",
        "Gen": "vi=6",
        "Loc": "vi=7",
        "Voc": "vi=s",
        "Cpd": "samasta=y",
    },
    "Gender": {"Masc": "li=pum", "Fem": "li=stri", "Neut": "li=na"},
    "Number": {"Sing": "va=eka", "Dual": "va=dvi", "Plur": "va=bahu"},
    "Person": {"3": "pu=pra", "2": "pu=ma", "1": "pu=u"},
    "Tense": {"Pres": "la=lat", "Imp": "la=lot", "Fut": "la=lrt"},
    "Mood": {"Opt": "la=lin"},
    "VerbForm": {
        "Inf": "krt=tumun",
        "Conv": "krt=ktva",
        "Gdv": "krt=krtya",
        "Part": "krt=sat",
    },
}


def parse_dharmamitra_tags(tag: str) -> dict[str, str]:
    items = [x.strip() for x in tag.split("|")]
    parsed = {}
    for item in items:
        key, _, value = item.partition("=")
        parsed[key] = value
    return parsed


def remap_dharmamitra_tags(map: dict[str, str]) -> dict[str, str]:
    tags = {}
    if "Person" in map:
        tags["pada"] = "tin"
    elif "Case" in map:
        tags["pada"] = "sup"
    else:
        tags["pada"] = "a"

    is_verb = tags["pada"] == "tin"
    if is_verb:
        la = None
        if map.get("Mood") == "Opt":
            la = "lin"
        elif map.get("Tense") == "Pres":
            la = "lat"
        elif map.get("Tense") == "Past":
            la = "lit"
        elif map.get("Tense") == "Imp":
            la = "lot"
        elif map.get("Tense") == "Fut":
            la = "lrt"
        if la:
            tags["la"] = la

    for key in ["VerbForm", "Person", "Gender", "Case", "Number", "Compound"]:
        value = map.get(key)
        if value is None:
            continue

        if key == "VerbForm" and value == "Part":
            tense = map.get("Tense")
            krt = None
            if tense == "Pres":
                krt = "sat"
            elif tense == "Fut":
                krt = "lrt-sat"
            else:
                krt = "nistha"
            assert krt
            tags["krt"] = krt
            continue

        try:
            tag = remapping[key][value]
            key, _, value = tag.partition("=")
            tags[key] = value
        except KeyError:
            pass

    return tags


LAKARAS = {
    Lakara.Lat: "lat",
    Lakara.Lit: "lit",
    Lakara.Lut: "lut",
    Lakara.Lrt: "lrt",
    Lakara.Lot: "lot",
    Lakara.Lan: "lan",
    Lakara.VidhiLin: "vidhi-lin",
    Lakara.AshirLin: "ashir-lin",
    Lakara.Lun: "lun",
    Lakara.Lrn: "lrn",
}


def _expand_tags_with_kosha(
    slp_form: str, slp_base: str, map: dict[str, str], kosha: Kosha
):
    if map.get("pada") != "tin":
        return

    key = slp_form
    if key.endswith("H"):
        key = key[:-1] + "s"

    for entry in kosha.get(key):
        if isinstance(entry, PadaEntry.Tinanta):
            map["la"] = LAKARAS.get(entry.lakara, map.get("la", "-"))


def serialize_ambuda_tags(data: dict[str, str]) -> str:
    return " ".join(f"{k}={v}" for k, v in data.items())


def remap_token(token: DharmamitraToken, kosha: Kosha | None = None) -> AmbudaToken:
    slp_form = transliterate(token.unsandhied, Scheme.Iast, Scheme.Slp1)
    slp_form = slp_form.replace("-", "")
    slp_base = transliterate(token.lemma, Scheme.Iast, Scheme.Slp1)

    dm_map = parse_dharmamitra_tags(token.tag)
    am_map = remap_dharmamitra_tags(dm_map)
    if kosha:
        _expand_tags_with_kosha(slp_form, slp_base, am_map, kosha)

    parse = serialize_ambuda_tags(am_map)
    return AmbudaToken(form=slp_form, base=slp_base, parse=parse)
