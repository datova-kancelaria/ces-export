from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    atomic_write_bytes(path, text.encode(encoding))


def atomic_write_path(path: Path, write_tmp: Callable[[Path], None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        write_tmp(tmp)
        tmp.replace(path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_meta(meta_path: Path) -> dict[str, Any] | None:
    return load_json(meta_path)


def write_meta(meta_path: Path, meta: dict[str, Any]) -> None:
    atomic_write_bytes(meta_path, (json.dumps(meta, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))


def meta_matches(meta: dict[str, Any] | None, want: dict[str, Any]) -> bool:
    if not meta:
        return False
    keys = ["datasetName", "hierarchyNodeCode", "dateFrom", "dateTo", "fileFormat", "mergeStrategy", "window"]
    return all(meta.get(k) == want.get(k) for k in keys)
