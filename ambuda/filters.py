"""Manages various small template filters."""

from datetime import datetime, UTC

from dateutil.relativedelta import relativedelta
from flask import session
from jinja2 import pass_context
from ambuda.utils.vidyut_shim import transliterate, Scheme
from markdown_it import MarkdownIt

#: A markdown parser for user-generated text.
#:
#: - `js-default` is like Commonmark but it disables raw HTML.
#: - `typographer` enables specific typography improvements:
#:   - `replacements` replaces `---` with `&mdash;`, etc.
#:   - `smartquotes` replaces basic quotes with opening and closing quotes.
#:   - `linkify` converts URLs like `"github.com"` into clickable links.
#:
#: Docs: https://markdown-it-py.readthedocs.io/en/latest/using.html
MARKDOWN = MarkdownIt("js-default", {"typographer": True, "linkify": True}).enable(
    ["replacements", "smartquotes", "linkify"]
)


def slp_to_devanagari(s: str) -> str:
    """SLP1 to Devanagari."""
    return transliterate(s, Scheme.Slp1, Scheme.Devanagari)


def devanagari(s: str) -> str:
    """HK to Devanagari."""
    return transliterate(s, Scheme.HarvardKyoto, Scheme.Devanagari)


@pass_context
def hk_to_user_script(_ctx, s: str) -> str:
    """@pass_context prevents constant-folding in Jinja."""
    return transliterate(s, Scheme.HarvardKyoto, _user_scheme())


@pass_context
def hk_slug_to_user_script(_ctx, s: str) -> str:
    """Like hk_to_user_script but preserves dots (instead of converting to dandas)."""
    return transliterate(s, Scheme.HarvardKyoto, _user_scheme()).replace("\u0964", ".")


@pass_context
def devanagari_to_user_script(_ctx, s: str) -> str:
    """@pass_context prevents constant-folding in Jinja."""
    return transliterate(s, Scheme.Devanagari, _user_scheme())


def _user_scheme() -> Scheme:
    try:
        return Scheme.from_string(session.get("script", "Devanagari"))
    except ValueError:
        return Scheme.Devanagari


def roman(s: str) -> str:
    """HK to Roman."""
    return transliterate(s, Scheme.HarvardKyoto, Scheme.Iast)


def time_ago(dt: datetime, now=None) -> str:
    """Print a datetime relative to right now.

    :param dt: the datetime to check
    :param now: the "now" datetime. If not set, use current UTC time.

    """
    # FIXME: add i18n support
    now = now or datetime.now(UTC).replace(tzinfo=None)
    rd = relativedelta(now, dt)
    for name in ["years", "months", "days", "hours", "minutes", "seconds"]:
        n = getattr(rd, name)
        if n:
            if n == 1:
                name = name[:-1]
            return f"{n} {name} ago"
    return "now"


def markdown(text: str) -> str:
    """Render the given Markdown text as HTML."""
    return MARKDOWN.render(text)


def reject_keys(d, *keys):
    """Return a copy of dict *d* without the specified keys."""
    excluded = set(keys)
    return {k: v for k, v in d.items() if k not in excluded}


def human_readable_bytes(bytes: int) -> str:
    suffixes = ["B", "KiB", "MiB", "GiB"]

    amount = bytes
    index = 0
    while amount >= 1024 and index + 1 < len(suffixes):
        amount /= 1024
        index += 1

    if index == 0:
        return f"{amount} {suffixes[index]}"
    else:
        return f"{amount:.1f} {suffixes[index]}"
