"""Utilities for LLM-based text structuring."""

GEMINI_MODEL = "gemini-3-flash-preview"


DEFAULT_STRUCTURING_PROMPT = """You are a highly specialized text structuring assistant. Your task
is to analyze the provided raw text page and add appropriate structural markup using specific XML
tags and attributes.

**CORE DIRECTIVES (STRICTLY ENFORCED):**

- Preserve the original text content exactly. Add only XML tags, attributes, and necessary whitespace.
    - Exception: you may delete clearly irrelevant text.
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
- Use <stage> for stage directions. Wrap parentheses if present.
- Use <speaker> for speakers. Wrap dashes if present.
- Use <chaya> for Sanskrit translations of Prakrit text. Wrap brackets if present.

**IV. TAG ATTRIBUTES:**

- `merge-next`
  - Values: "true"
  - Add merge-next="true" if a block clearly continues onto the next page. Otherwise, leave
    the attribute unset.

TEXT TO STRUCTURE FOLLOWS:

<text-to-structure>

{content}

</text-to-structure>
"""


def run(
    content: str, api_key: str, prompt_template: str = DEFAULT_STRUCTURING_PROMPT
) -> str:
    if not content:
        raise ValueError("No content provided for structuring")

    if not api_key:
        raise ValueError("GEMINI_API_KEY not configured")

    from google import genai

    client = genai.Client(api_key=api_key)
    prompt = prompt_template.format(content=content)
    response = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)

    return response.text


BATCH_DELIMITER_START = "===PAGE_START {slug}==="
BATCH_DELIMITER_END = "===PAGE_END {slug}==="

BATCH_WRAPPER = """You will process multiple pages. Each page is delimited below.
Apply the instructions to EACH page independently and return the results using the
EXACT same delimiters.

For each page, return:
===PAGE_START <slug>===
<your structured output for that page>
===PAGE_END <slug>===

Do NOT include any text outside of these delimiters.

INSTRUCTIONS:
{instructions}

PAGES TO PROCESS:
{pages}"""


def run_batch(
    pages: dict[str, str],
    api_key: str,
    prompt_template: str = DEFAULT_STRUCTURING_PROMPT,
) -> dict[str, str]:
    """Run the LLM over multiple pages in a single API call.

    Args:
        pages: mapping of page slug to page content.
        api_key: Gemini API key.
        prompt_template: the per-page prompt template (must contain {content}).

    Returns:
        mapping of page slug to LLM output.
    """
    if not pages:
        raise ValueError("No pages provided")
    if not api_key:
        raise ValueError("GEMINI_API_KEY not configured")

    # Build the combined input: instructions + all pages with delimiters.
    # Strip the {content} placeholder from the template to get just the instructions.
    instructions = prompt_template.replace("{content}", "<page content appears here>")

    page_sections = []
    for slug, content in pages.items():
        page_sections.append(
            f"{BATCH_DELIMITER_START.format(slug=slug)}\n"
            f"{content}\n"
            f"{BATCH_DELIMITER_END.format(slug=slug)}"
        )

    prompt = BATCH_WRAPPER.format(
        instructions=instructions,
        pages="\n\n".join(page_sections),
    )

    from google import genai

    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)

    return _parse_batch_response(response.text, set(pages.keys()))


def _parse_batch_response(
    response_text: str, expected_slugs: set[str]
) -> dict[str, str]:
    """Parse delimited multi-page LLM output back into per-page results."""
    import re

    results = {}
    pattern = re.compile(
        r"===PAGE_START\s+(\S+?)===\s*\n(.*?)\n\s*===PAGE_END\s+\1===",
        re.DOTALL,
    )
    for match in pattern.finditer(response_text):
        slug = match.group(1)
        content = match.group(2).strip()
        results[slug] = content

    return results
