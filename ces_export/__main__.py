from __future__ import annotations

import argparse
import os
import requests
from dataclasses import replace
from datetime import date
from pathlib import Path

from .ces_api import fetch_od003_items
from .dataset_config import load_config
from .orgs import choose_hierarchy_node_code, print_orgs
from .planner import build_jobs
from .runner import postprocess_result, run_job
from .settings import build_session, common_headers, load_app_settings, load_credentials


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", type=Path, required=True, help="Path to dataset config JSON")
    ap.add_argument("--hierarchy-node-code", help="Exact hierarchy node code from OD_003 (e.g. 0COMP_CODE:...)")
    ap.add_argument("--org-name", help="Substring match on OD_003 item name (case-insensitive)")
    ap.add_argument("--list-orgs", action="store_true", help="Print OD_003 org codes+names and exit")
    ap.add_argument("--list-orgs-filter", help="Filter string for --list-orgs output")
    ap.add_argument("--no-cache-org", action="store_true", help="Do not read/write cached org code")
    ap.add_argument("--today", help="Override today's date (YYYY-MM-DD) for testing")
    ap.add_argument("--out-dir", type=Path, help="Override config defaults.out_dir")
    ap.add_argument("--dry-run", action="store_true", help="Print what would be fetched; do nothing")
    ap.add_argument("--force", action="store_true", help="Re-download even if meta matches")
    ap.add_argument("--start-year", type=int, help="Override start_year on schedules that use years")
    ap.add_argument("--end-year", type=int, help="Override end_year on schedules that use years")
    ap.add_argument("--include-dataset", action="append", default=[], help="Only run selected dataset(s)")
    ap.add_argument("--exclude-dataset", action="append", default=[], help="Skip selected dataset(s)")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.list_orgs_filter and not args.list_orgs:
        args.list_orgs = True

    today = date.fromisoformat(args.today) if args.today else date.today()

    config = load_config(args.config)

    out_dir_override = args.out_dir or (
        Path(os.environ["CES_EXPORT_OUT_DIR"])
        if os.environ.get("CES_EXPORT_OUT_DIR")
        else None
    )

    resolved_out_dir = out_dir_override or config.defaults.out_dir
    if resolved_out_dir is None:
        raise SystemExit("No output directory configured. Set CES_EXPORT_OUT_DIR or pass --out-dir.")

    config = replace(config, defaults=replace(config.defaults, out_dir=resolved_out_dir))

    creds = load_credentials()
    session = build_session(creds)
    settings = load_app_settings()

    headers = common_headers(creds)
    items = fetch_od003_items(session, headers, settings)

    if args.list_orgs:
        print_orgs(items, pattern=args.list_orgs_filter)
        return 0

    hierarchy_node_code = choose_hierarchy_node_code(
        items,
        base_out_dir=config.defaults.out_dir,
        cli_code=args.hierarchy_node_code,
        cli_name=args.org_name,
        no_cache_org=args.no_cache_org,
    )
    print("Using hierarchyNodeCode:", hierarchy_node_code)

    jobs = build_jobs(
        config,
        today=today,
        start_year_override=args.start_year,
        end_year_override=args.end_year,
        include_datasets=set(args.include_dataset),
        exclude_datasets=set(args.exclude_dataset),
    )

    soft_failures: list[str] = []

    for job in jobs:
        try:
            result = run_job(
                session,
                headers,
                settings,
                job,
                hierarchy_node_code,
                dry_run=args.dry_run,
                force_rerun=args.force,
            )
            print(f"[{job.fmt}] {job.dataset}: {result.message}")
            if not args.dry_run:
                postprocess_result(result, job)

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None

            # hard fail on auth/permission
            if status in {401, 403}:
                raise

            # optionally also hard-fail on service outage
            # if status in {429, 500, 502, 503, 504}:
            #     raise

            msg = f"[{job.fmt}] FAILED: {job.dataset} {job.d_from} -> {job.d_to} | HTTP {status}"
            print(msg)
            print("error:", repr(e))
            soft_failures.append(msg)

        except Exception as e:
            # dataset/postprocess/content problems: log and continue
            msg = f"[{job.fmt}] FAILED: {job.dataset} {job.d_from} -> {job.d_to}"
            print(msg)
            print("error:", repr(e))
            soft_failures.append(f"{msg} | {repr(e)}")

    if soft_failures:
        print("\nCompleted with non-fatal dataset failures:")
        for msg in soft_failures:
            print(" -", msg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
