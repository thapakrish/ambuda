"""Slug generation for Sanskrit titles.

Mirrors the titleToSlug logic in ambuda/static/js/publish-config.js.
"""

import re
import unicodedata

from vidyut.lipi import transliterate, Scheme

_DIACRITICS = {
    "ś": "sh",
    "Ś": "Sh",
    "ṣ": "sh",
    "Ṣ": "Sh",
    "ā": "a",
    "Ā": "A",
    "ī": "i",
    "Ī": "I",
    "ū": "u",
    "Ū": "U",
    "ṛ": "r",
    "Ṛ": "R",
    "ṝ": "r",
    "Ṝ": "R",
    "ñ": "n",
    "Ñ": "N",
    "ṅ": "n",
    "Ṅ": "N",
    "ṇ": "n",
    "Ṇ": "N",
    "ṃ": "m",
    "Ṃ": "M",
    "ḥ": "h",
    "Ḥ": "H",
    "ṭ": "t",
    "Ṭ": "T",
    "ḍ": "d",
    "Ḍ": "D",
    "ḷ": "l",
    "Ḷ": "L",
}

_DIACRITICS_RE = re.compile("[" + re.escape("".join(_DIACRITICS)) + "]")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_TRIM_RE = re.compile(r"^-+|-+$")


def normalize_for_search(s: str) -> str:
    """Normalize a Sanskrit string (Devanagari, IAST, or HK) to lowercase ASCII.

    Used for fuzzy search matching across scripts.
    """
    if not s:
        return ""
    hk = transliterate(s, Scheme.Devanagari, Scheme.HarvardKyoto)
    if not hk:
        hk = s
    hk = hk.lower()
    hk = _DIACRITICS_RE.sub(lambda m: _DIACRITICS.get(m.group(), m.group()), hk)
    return hk


def title_to_slug(s: str) -> str:
    """Convert a Sanskrit title (Devanagari or IAST) to a URL slug."""
    if not s:
        return ""

    hk = transliterate(s, Scheme.Devanagari, Scheme.HarvardKyoto)
    if not hk:
        hk = s

    hk = hk.replace("G", "n").replace("J", "n")
    hk = re.sub(r"M(?=[pbhzSs])", "m", hk)
    hk = hk.replace("M", "n")
    hk = hk.replace("z", "sh").replace("S", "sh")

    hk = hk.lower()
    hk = _DIACRITICS_RE.sub(lambda m: _DIACRITICS.get(m.group(), m.group()), hk)
    hk = _NON_ALNUM_RE.sub("-", hk)
    hk = _TRIM_RE.sub("", hk)
    return hk
