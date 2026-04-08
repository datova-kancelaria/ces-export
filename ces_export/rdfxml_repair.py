from __future__ import annotations

import re
from xml.sax.saxutils import escape


LANGTAG_RE = re.compile(r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$")

EMPTY_BAD_LANG_ELEMENT_RE = re.compile(
    r"""
    <(?P<tag>[A-Za-z_][\w:.\-]*)
    (?P<before>[^<>]*?)
    \bxml:lang\s*=\s*"(?P<lang>[^"]+)"
    (?P<after>[^<>]*?)>
    (?P<body>\s*)
    </(?P=tag)\s*>
    """,
    re.VERBOSE | re.DOTALL,
)


def _is_reasonable_langtag(tag: str) -> bool:
    return bool(LANGTAG_RE.fullmatch(tag.strip()))


def _repair_empty_bad_lang_descriptions(text: str) -> tuple[str, int]:
    count = 0

    def repl(m: re.Match[str]) -> str:
        nonlocal count

        tag = m.group("tag")
        local_name = tag.split(":")[-1]

        if local_name != "description":
            return m.group(0)

        bad_lang = m.group("lang")
        body = m.group("body")

        if _is_reasonable_langtag(bad_lang):
            return m.group(0)

        if body.strip():
            return m.group(0)

        count += 1
        return (
            f'<{tag}{m.group("before")} xml:lang="sk"{m.group("after")}>'
            f"{escape(bad_lang)}"
            f"</{tag}>"
        )

    repaired = EMPTY_BAD_LANG_ELEMENT_RE.sub(repl, text)
    return repaired, count


def repair_rdfxml_text(
    text: str,
    *,
    dataset: str | None = None,
    chunk_name: str | None = None,
) -> str:
    repaired = text

    repaired, count = _repair_empty_bad_lang_descriptions(repaired)

    if count:
        where = chunk_name or "<memory>"
        if dataset:
            print(f"RDF/XML repair [{dataset}] {where}: fixed {count} bad xml:lang description element(s)")
        else:
            print(f"RDF/XML repair {where}: fixed {count} bad xml:lang description element(s)")

    return repaired


def repair_rdfxml_bytes(
    payload: bytes,
    *,
    dataset: str | None = None,
    chunk_name: str | None = None,
) -> bytes:
    original_text: str | None = None

    for enc in ("utf-8", "cp1250", "latin-1"):
        try:
            original_text = payload.decode(enc)
            break
        except UnicodeDecodeError:
            continue

    if original_text is None:
        return payload

    repaired_text = repair_rdfxml_text(
        original_text,
        dataset=dataset,
        chunk_name=chunk_name,
    )

    if repaired_text == original_text:
        return payload

    return repaired_text.encode("utf-8")
