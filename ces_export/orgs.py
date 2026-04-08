from __future__ import annotations

import os
import unicodedata
from pathlib import Path
from typing import Any

from .io_utils import atomic_write_text


def fold_for_match(s: str) -> str:
    s = s.casefold()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def pick_hierarchy_node_code(
    items: list[dict[str, Any]],
    *,
    code: str | None,
    org_name: str | None,
) -> str:
    if code:
        if any((it.get("code") == code) for it in items):
            return code
        raise RuntimeError(f"Hierarchy node code not found in OD_003: {code}")

    if org_name:
        needle = fold_for_match(org_name)
        matches = []
        for it in items:
            name = (it.get("name") or "")
            if needle in fold_for_match(name):
                matches.append(it)

        if len(matches) == 1:
            return str(matches[0].get("code"))

        if len(matches) == 0:
            raise RuntimeError(
                f"No OD_003 org matches --org-name={org_name!r}. "
                f"Use --list-orgs to see available names."
            )

        preview = "\n".join(f"  {m.get('code')}  |  {m.get('name')}" for m in matches[:30])
        raise RuntimeError(
            f"Multiple OD_003 orgs match --org-name={org_name!r} ({len(matches)} matches). "
            f"Make org-name more specific or pass --hierarchy-node-code.\n{preview}"
        )

    raise RuntimeError(
        "No hierarchy node selected. Provide --org-name or --hierarchy-node-code "
        "(or set CES_ORG_NAME / CES_HIERARCHY_NODE_CODE). Use --list-orgs to inspect."
    )


def cache_path(base_out_dir: Path) -> Path:
    return base_out_dir / ".hierarchy_node_code.txt"


def load_cached_code(base_out_dir: Path) -> str | None:
    p = cache_path(base_out_dir)
    if not p.exists():
        return None
    val = p.read_text(encoding="utf-8").strip()
    return val or None


def save_cached_code(base_out_dir: Path, code: str) -> None:
    base_out_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_text(cache_path(base_out_dir), code + "\n")


def print_orgs(items: list[dict[str, Any]], pattern: str | None = None) -> None:
    pat = pattern.casefold() if pattern else None
    for it in items:
        code = it.get("code")
        name = it.get("name") or ""
        if pat and pat not in name.casefold() and pat not in str(code).casefold():
            continue
        print(f"{code}\t{name}")


def choose_hierarchy_node_code(
    items: list[dict[str, Any]],
    *,
    base_out_dir: Path,
    cli_code: str | None,
    cli_name: str | None,
    no_cache_org: bool,
) -> str:
    env_code = os.environ.get("CES_HIERARCHY_NODE_CODE")
    env_name = os.environ.get("CES_ORG_NAME")
    cached = None if no_cache_org else load_cached_code(base_out_dir)

    preferred_code = cli_code or env_code or cached
    preferred_name = cli_name or env_name

    code = pick_hierarchy_node_code(
        items,
        code=preferred_code,
        org_name=None if preferred_code else preferred_name,
    )

    if not no_cache_org and cached != code:
        save_cached_code(base_out_dir, code)

    return code
