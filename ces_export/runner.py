from __future__ import annotations

import os

from datetime import date, datetime
from pathlib import Path

import requests

from .ces_api import create_od001_request, decode_payload_from_od002, poll_od002_until_done
from .date_rules import split_range
from .io_utils import atomic_write_bytes, load_meta, meta_matches, write_meta
from .mergers import RDFXMLMergeError, merge_csv_chunks, merge_rdfxml_chunks, write_chunk_manifest
from .models import AppSettings, ExportJob, RunResult



def want_meta(job: ExportJob, hierarchy_node_code: str) -> dict[str, object]:
    return {
        "datasetName": job.dataset,
        "hierarchyNodeCode": hierarchy_node_code,
        "dateFrom": job.d_from.isoformat(),
        "dateTo": job.d_to.isoformat(),
        "fileFormat": job.fmt,
        "mergeStrategy": job.merge_strategy,
        "window": {"mode": job.window.mode, "size": job.window.size},
    }


def _chunk_filename(fmt: str, d_from: date, d_to: date) -> str:
    return f"{d_from.isoformat()}_{d_to.isoformat()}.{fmt}"


def _chunk_dir(job: ExportJob) -> Path:
    return job.out_path.parent / f"{job.out_path.name}.chunks"


def _chunk_path(job: ExportJob, d_from: date, d_to: date) -> Path:
    return _chunk_dir(job) / _chunk_filename(job.fmt, d_from, d_to)


def touch_path_to_date(path: Path, d: date, when: datetime | None = None) -> None:
    when = when or datetime.now().astimezone()
    stamped = when.replace(year=d.year, month=d.month, day=d.day)
    ts = stamped.timestamp()
    os.utime(path, (ts, ts))


def run_job(
    s: requests.Session,
    common_headers: dict[str, str],
    settings: AppSettings,
    job: ExportJob,
    hierarchy_node_code: str,
    *,
    dry_run: bool,
    force_rerun: bool,
) -> RunResult:
    want = want_meta(job, hierarchy_node_code)
    have = load_meta(job.meta_path)

    if (not force_rerun) and meta_matches(have, want) and job.out_path.exists():
        print(f"\nSKIP (up-to-date): {job.out_path}")
        return RunResult(
            dataset=job.dataset,
            fmt=job.fmt,
            requested_range=(job.d_from, job.d_to),
            main_output=job.out_path,
            meta_output=job.meta_path,
            chunk_outputs=(),
            merged=True,
            skipped=True,
            message="up-to-date",
        )

    print(f"\nWILL FETCH: {job.dataset} [{job.fmt}]  {job.d_from} -> {job.d_to}")
    print(f"  -> {job.out_path}")

    windows = split_range(job.d_from, job.d_to, job.window)

    if dry_run:
        return RunResult(
            dataset=job.dataset,
            fmt=job.fmt,
            requested_range=(job.d_from, job.d_to),
            main_output=job.out_path,
            meta_output=job.meta_path,
            chunk_outputs=tuple(_chunk_path(job, d0, d1) for d0, d1 in windows),
            merged=(len(windows) == 1),
            skipped=False,
            message="dry-run",
        )

    chunk_dir = _chunk_dir(job)
    chunk_files: list[Path] = []

    for d_from, d_to in windows:
        chunk_path = _chunk_path(job, d_from, d_to)

        if chunk_path.exists() and not force_rerun:
            print(f"SKIP CHUNK (already exists): {chunk_path}")
        else:
            rid = create_od001_request(
                s=s,
                common_headers=common_headers,
                settings=settings,
                dataset_name=job.dataset,
                hierarchy_node_code=hierarchy_node_code,
                d_from=d_from.isoformat(),
                d_to=d_to.isoformat(),
                file_format=job.fmt,
            )
            j_done = poll_od002_until_done(s, common_headers, settings, rid)
            payload = decode_payload_from_od002(j_done)
            atomic_write_bytes(chunk_path, payload)

        chunk_files.append(chunk_path)

        write_chunk_manifest(
            chunk_dir / "manifest.json",
            dataset=job.dataset,
            fmt=job.fmt,
            d_from=job.d_from.isoformat(),
            d_to=job.d_to.isoformat(),
            merge_strategy=job.merge_strategy,
            chunk_files=chunk_files,
            merged=False,
            reason=f"chunks collected so far; merge_strategy={job.merge_strategy}",
            merge_outcome="collecting_chunks",
        )

    if len(chunk_files) == 1 and job.merge_strategy != "rdfxml_graph":
        payload = chunk_files[0].read_bytes()
        atomic_write_bytes(job.out_path, payload)
        write_meta(job.meta_path, want)

        write_chunk_manifest(
            chunk_dir / "manifest.json",
            dataset=job.dataset,
            fmt=job.fmt,
            d_from=job.d_from.isoformat(),
            d_to=job.d_to.isoformat(),
            merge_strategy=job.merge_strategy,
            chunk_files=chunk_files,
            merged=True,
            reason="single payload",
            main_output=job.out_path,
            merge_outcome="success_first_try",
            merge_details={
                "outcome": "success_first_try",
                "detail": "single payload copied without merge",
                "usedPostprocessRetry": False,
                "repairedChunks": [],
            },
        )

        if job.touch_mtime_to_range_end:
            touch_path_to_date(job.out_path, job.d_to)
            touch_path_to_date(job.meta_path, job.d_to)
            touch_path_to_date(chunk_dir / "manifest.json", job.d_to)

        return RunResult(
            dataset=job.dataset,
            fmt=job.fmt,
            requested_range=(job.d_from, job.d_to),
            main_output=job.out_path,
            meta_output=job.meta_path,
            chunk_outputs=tuple(chunk_files),
            merged=True,
            skipped=False,
            message="single payload",
        )

    if job.merge_strategy == "skip_if_chunked":
        write_chunk_manifest(
            chunk_dir / "manifest.json",
            dataset=job.dataset,
            fmt=job.fmt,
            d_from=job.d_from.isoformat(),
            d_to=job.d_to.isoformat(),
            merge_strategy=job.merge_strategy,
            chunk_files=chunk_files,
            merged=False,
            reason=f"merge_strategy={job.merge_strategy}",
            merge_outcome="skipped_if_chunked",
        )
        return RunResult(
            dataset=job.dataset,
            fmt=job.fmt,
            requested_range=(job.d_from, job.d_to),
            main_output=None,
            meta_output=None,
            chunk_outputs=tuple(chunk_files),
            merged=False,
            skipped=False,
            message="merge skipped by merge_strategy=skip_if_chunked; chunks saved",
        )

    if job.merge_strategy == "keep_chunks" or job.keep_chunks:
        write_chunk_manifest(
            chunk_dir / "manifest.json",
            dataset=job.dataset,
            fmt=job.fmt,
            d_from=job.d_from.isoformat(),
            d_to=job.d_to.isoformat(),
            merge_strategy=job.merge_strategy,
            chunk_files=chunk_files,
            merged=False,
            reason=f"merge_strategy={job.merge_strategy}",
            merge_outcome="kept_chunks",
        )
        write_meta(job.meta_path, want)

        if job.touch_mtime_to_range_end:
            touch_path_to_date(job.meta_path, job.d_to)
            touch_path_to_date(chunk_dir / "manifest.json", job.d_to)

        return RunResult(
            dataset=job.dataset,
            fmt=job.fmt,
            requested_range=(job.d_from, job.d_to),
            main_output=None,
            meta_output=job.meta_path,
            chunk_outputs=tuple(chunk_files),
            merged=False,
            skipped=False,
            message="kept chunks",
        )

    merge_outcome: str | None = None
    merge_details: dict[str, object] | None = None

    if job.merge_strategy == "csv_header":
        merged = merge_csv_chunks([p.read_bytes() for p in chunk_files])
        merge_outcome = "success_first_try"
        merge_details = {
            "outcome": "success_first_try",
            "detail": "merged CSV chunks on first try",
            "usedPostprocessRetry": False,
            "repairedChunks": [],
        }
    elif job.merge_strategy == "rdfxml_graph":
        try:
            merged, merge_details = merge_rdfxml_chunks(
                [p.read_bytes() for p in chunk_files],
                dataset=job.dataset,
                chunk_names=[p.name for p in chunk_files],
                enable_postprocess_retry=True,
            )
            merge_outcome = str(merge_details.get("outcome"))
        except RDFXMLMergeError as e:
            write_chunk_manifest(
                chunk_dir / "manifest.json",
                dataset=job.dataset,
                fmt=job.fmt,
                d_from=job.d_from.isoformat(),
                d_to=job.d_to.isoformat(),
                merge_strategy=job.merge_strategy,
                chunk_files=chunk_files,
                merged=False,
                reason=str(e),
                merge_outcome=str(e.report.get("outcome", "failed_after_postprocess")),
                merge_details=e.report,
            )
            raise
    elif job.merge_strategy == "concat":
        merged = b"".join(p.read_bytes() for p in chunk_files)
        merge_outcome = "success_first_try"
        merge_details = {
            "outcome": "success_first_try",
            "detail": "concatenated chunks on first try",
            "usedPostprocessRetry": False,
            "repairedChunks": [],
        }
    else:
        raise ValueError(f"Unsupported merge strategy: {job.merge_strategy}")

    atomic_write_bytes(job.out_path, merged)
    write_meta(job.meta_path, want)

    write_chunk_manifest(
        chunk_dir / "manifest.json",
        dataset=job.dataset,
        fmt=job.fmt,
        d_from=job.d_from.isoformat(),
        d_to=job.d_to.isoformat(),
        merge_strategy=job.merge_strategy,
        chunk_files=chunk_files,
        merged=True,
        reason=(merge_details.get("detail") if merge_details else f"merged using {job.merge_strategy}"),
        main_output=job.out_path,
        merge_outcome=merge_outcome,
        merge_details=merge_details,
    )

    if job.touch_mtime_to_range_end:
        touch_path_to_date(job.out_path, job.d_to)
        touch_path_to_date(job.meta_path, job.d_to)
        touch_path_to_date(chunk_dir / "manifest.json", job.d_to)

    return RunResult(
        dataset=job.dataset,
        fmt=job.fmt,
        requested_range=(job.d_from, job.d_to),
        main_output=job.out_path,
        meta_output=job.meta_path,
        chunk_outputs=tuple(chunk_files),
        merged=True,
        skipped=False,
        message=f"merged using {job.merge_strategy}",
    )


def postprocess_result(result: RunResult, job: ExportJob) -> None:
    if not result.main_output or not result.main_output.exists():
        return

    from .postprocess import csv_file_to_xlsx, rdfxml_file_to_jsonld

    for step in job.postprocess:
        if step == "xlsx" and result.main_output.suffix.lower() == ".csv":
            xlsx_path = result.main_output.with_suffix(".xlsx")
            csv_file_to_xlsx(result.main_output, xlsx_path, delimiter=";")
            if job.touch_mtime_to_range_end and xlsx_path.exists():
                touch_path_to_date(xlsx_path, job.d_to)

        elif step == "jsonld" and result.main_output.suffix.lower() == ".xml":
            jsonld_path = result.main_output.with_suffix(".jsonld")
            rdfxml_file_to_jsonld(result.main_output, jsonld_path)
            if job.touch_mtime_to_range_end and jsonld_path.exists():
                touch_path_to_date(jsonld_path, job.d_to)
