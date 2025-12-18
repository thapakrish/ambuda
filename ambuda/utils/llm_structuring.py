"""Utilities for LLM-based text structuring."""

from google import genai


DEFAULT_STRUCTURING_PROMPT = """You are a highly specialized text structuring assistant. Your task
is to analyze the provided raw text page and add appropriate structural markup using specific XML
tags and attributes.

**CORE DIRECTIVES (STRICTLY ENFORCED):**

- Preserve the original text content exactly. Add only XML tags, attributes, and necessary whitespace.
- Do not translate, modify, remove, or reorder any text content, including special characters.
- Return ONLY the structured XML text with **no preamble, explanation, or commentary**.
- If XML tags are present within the page text already, preserve them EXACTLY.

**I. DOCUMENT WRAPPER:**

- Wrap the entire output in the `<page>` tag.
- RULE: The page MUST be wrapped in `<page>...</page>` to form a valid document.

**II. BLOCK-LEVEL TAGS (direct children of <page>):**

Identify the function of each contiguous block of text and wrap it with the appropriate tag. If
necessary, you may split a block of text into multiple blocks by inserting newlines.

- Use <verse> for verses (typically numbered or metered lines)
- Use <p> for prose paragraphs (non-verse text).
- Use <heading> for section titles and chapter titles.
- Use <footnote> for blocks that start with a footnote marker, such as [^१] or [^1].
  Example: if a block starts with [^१], wrap it with <footnote name="१">...</footnote>.
- Use <ignore> for all other text content that does not ma

**III. INLINE TAGS** (direct children of block-level tags)

- Use <ref target="..." /> for inline footnotes.
  Example: if [^१.] appears in a verse or paragraph, convert it to <ref target="१." />.
  EXACTLY preserve the text in [^...] as the `target` attribute value.

**IV. TAG ATTRIBUTES:**

- `text`
  - Values: "A", "B", "C", etc.
  - REQUIRED for every block tag. Assign the appropriate text identifier based on the
    CUSTOM INSTRUCTIONS below.
- `ref`
  - Values: "before", "after".
  - If the block is a translation or commentary for the block *preceding* it, use "before".
  - If the block is a translation or commentary for the block *following* it, use "after".
- `scope`
  - Values: "partial"
  - Add scope="partial" if a block clearly continues onto the next page.

**V. CUSTOM INSTRUCTIONS FOR THIS DOCUMENT:**

<custom-instructions>
This document contains two texts:
- text A is the original Sanskrit
- text B is a Hindi translation.
</custom-instructions>

TEXT TO STRUCTURE FOLLOWS:

<page-to-structure>
{content}
</page-to-structure>
"""


def run(
    content: str, api_key: str, prompt_template: str = DEFAULT_STRUCTURING_PROMPT
) -> str:
    if not content:
        raise ValueError("No content provided for structuring")

    if not api_key:
        raise ValueError("GEMINI_API_KEY not configured")

    client = genai.Client(api_key=api_key)
    prompt = prompt_template.format(content=content)
    response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)

    return response.text
