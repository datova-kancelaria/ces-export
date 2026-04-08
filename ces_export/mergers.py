from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from rdflib import Graph

from .io_utils import atomic_write_bytes, atomic_write_text
from .rdfxml_repair import repair_rdfxml_bytes


def merge_csv_chunks(chunks: list[bytes]) -> bytes:
    out_lines: list[str] = []
    header_norm: Optional[str] = None

    for b in chunks:
        if not b:
            continue
        txt = b.decode("utf-8", errors="replace")
        if not txt.strip():
            continue

        lines = txt.splitlines(True)
        if not lines:
            continue

        first_line = lines[0].lstrip("\ufeff")
        if header_norm is None:
            header_norm = first_line.strip("\r\n")
            out_lines.append(first_line if first_line.endswith(("\n", "\r")) else first_line + "\n")
            out_lines.extend(lines[1:])
        else:
            if first_line.lstrip("\ufeff").strip("\r\n") == header_norm:
                out_lines.extend(lines[1:])
            else:
                out_lines.extend(lines)

    return "".join(out_lines).encode("utf-8")


class RDFXMLMergeError(RuntimeError):
    def __init__(self, report: dict[str, Any]) -> None:
        super().__init__(str(report.get("detail", "RDF/XML merge failed")))
        self.report = report


def _serialize_graph(g: Graph) -> bytes:
    out = g.serialize(format="pretty-xml")
    return out.encode("utf-8") if isinstance(out, str) else bytes(out)


def _parse_rdfxml_payloads(payloads: list[bytes], chunk_names: list[str]) -> Graph:
    g = Graph()
    for idx, b in enumerate(payloads):
        chunk_name = chunk_names[idx]
        try:
            g.parse(data=b, format="xml")
        except Exception as e:
            raise ValueError(f"{chunk_name}: {type(e).__name__}: {e}") from e
    return g


def merge_rdfxml_chunks(
    payloads: list[bytes],
    *,
    dataset: str | None = None,
    chunk_names: list[str] | None = None,
    enable_postprocess_retry: bool = True,
) -> tuple[bytes, dict[str, Any]]:
    pairs: list[tuple[bytes, str]] = []
    for i, payload in enumerate(payloads):
        if not payload or not payload.strip():
            continue
        name = chunk_names[i] if chunk_names and i < len(chunk_names) else f"chunk_{i + 1}"
        pairs.append((payload, name))

    if not pairs:
        return (
            b"",
            {
                "outcome": "success_first_try",
                "detail": "no non-empty RDF/XML chunks",
                "usedPostprocessRetry": False,
                "repairedChunks": [],
            },
        )

    raw_payloads = [p for p, _ in pairs]
    names = [n for _, n in pairs]

    try:
        g = _parse_rdfxml_payloads(raw_payloads, names)
        return (
            _serialize_graph(g),
            {
                "outcome": "success_first_try",
                "detail": "merged RDF/XML chunks on first try",
                "usedPostprocessRetry": False,
                "repairedChunks": [],
            },
        )
    except Exception as first_err:
        if not enable_postprocess_retry:
            raise RDFXMLMergeError(
                {
                    "outcome": "failed_after_postprocess",
                    "detail": "raw RDF/XML merge failed; postprocess retry disabled",
                    "usedPostprocessRetry": False,
                    "repairedChunks": [],
                    "firstError": str(first_err),
                }
            ) from first_err

        repaired_payloads: list[bytes] = []
        repaired_chunks: list[str] = []

        for payload, name in pairs:
            repaired = repair_rdfxml_bytes(
                payload,
                dataset=dataset,
                chunk_name=name,
            )
            if repaired != payload:
                repaired_chunks.append(name)
            repaired_payloads.append(repaired)

        if not repaired_chunks:
            raise RDFXMLMergeError(
                {
                    "outcome": "failed_after_postprocess",
                    "detail": "raw RDF/XML merge failed and postprocess made no changes",
                    "usedPostprocessRetry": True,
                    "repairedChunks": [],
                    "firstError": str(first_err),
                    "secondError": "not retried because no chunks changed",
                }
            ) from first_err

        try:
            g = _parse_rdfxml_payloads(repaired_payloads, names)
            return (
                _serialize_graph(g),
                {
                    "outcome": "success_after_postprocess",
                    "detail": "merged RDF/XML chunks after postprocess retry",
                    "usedPostprocessRetry": True,
                    "repairedChunks": repaired_chunks,
                    "firstError": str(first_err),
                },
            )
        except Exception as second_err:
            raise RDFXMLMergeError(
                {
                    "outcome": "failed_after_postprocess",
                    "detail": "raw RDF/XML merge failed and postprocess retry also failed",
                    "usedPostprocessRetry": True,
                    "repairedChunks": repaired_chunks,
                    "firstError": str(first_err),
                    "secondError": str(second_err),
                }
            ) from second_err


def write_chunk_files(chunk_dir: Path, chunks: list[tuple[str, bytes]]) -> list[Path]:
    paths: list[Path] = []
    chunk_dir.mkdir(parents=True, exist_ok=True)
    for filename, payload in chunks:
        p = chunk_dir / filename
        atomic_write_bytes(p, payload)
        paths.append(p)
    return paths


def write_chunk_manifest(
    manifest_path: Path,
    *,
    dataset: str,
    fmt: str,
    d_from: str,
    d_to: str,
    merge_strategy: str,
    chunk_files: list[Path],
    merged: bool,
    reason: str,
    main_output: Path | None = None,
    merge_outcome: str | None = None,
    merge_details: dict[str, Any] | None = None,
) -> None:
    payload = {
        "dataset": dataset,
        "format": fmt,
        "dateFrom": d_from,
        "dateTo": d_to,
        "merged": merged,
        "reason": reason,
        "chunks": [p.name for p in chunk_files],
    }
    if main_output is not None:
        payload["mainOutput"] = main_output.name
    if merge_outcome is not None:
        payload["mergeOutcome"] = merge_outcome
    if merge_details is not None:
        payload["mergeDetails"] = merge_details

    atomic_write_text(
        manifest_path,
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    )
