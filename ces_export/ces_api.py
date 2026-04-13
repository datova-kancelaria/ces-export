from __future__ import annotations

import base64
import json
import time
from typing import Any

import requests

from .models import AppSettings

MAX_POLLS = 240
POLL_SLEEP_SEC = 2.0

WAIT_STATUSES = {"new", "processing"}
DONE_STATUS = "done"


def b64_json(obj: dict[str, Any]) -> str:
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def dump_response(r: requests.Response, label: str) -> None:
    print(f"\n=== {label} RESPONSE ===")
    print("status:", r.status_code, r.reason)
    print("url:", r.url)
    print("x-correlationid:", r.headers.get("x-correlationid"))
    print("x-vcap-request-id:", r.headers.get("x-vcap-request-id"))
    print("content-type:", r.headers.get("content-type"))
    print("body (first 1200 chars):")
    print((r.text or "")[:1200])


def fetch_od003_items(
    s: requests.Session,
    common_headers: dict[str, str],
    settings: AppSettings,
) -> list[dict[str, Any]]:
    r = s.get(settings.od003, headers=common_headers, timeout=30)
    dump_response(r, "OD_003 (sanity GET)")
    r.raise_for_status()

    j = r.json()
    items = j.get("items")
    if not isinstance(items, list):
        raise RuntimeError(f"OD_003: expected 'items' list, got: {type(items)}")
    return items


def create_od001_request(
    s: requests.Session,
    common_headers: dict[str, str],
    settings: AppSettings,
    dataset_name: str,
    hierarchy_node_code: str,
    d_from: str,
    d_to: str,
    file_format: str,
) -> int:
    payload = {
        "datasetName": dataset_name,
        "hierarchyNodeCode": hierarchy_node_code,
        "dateFrom": d_from,
        "dateTo": d_to,
        "fileFormat": file_format,
    }
    body = {"operation": "opendata", "payload": b64_json(payload)}
    headers_post = {**common_headers, "Content-Type": "application/json"}

    print("\n=== OD_001 REQUEST ===")
    print("POST", settings.od001)
    print("dataset:", dataset_name, "range:", d_from, "->", d_to, "format:", file_format)

    r = s.post(settings.od001, headers=headers_post, json=body, timeout=60)
    dump_response(r, "OD_001 (create request)")
    r.raise_for_status()

    j = r.json()
    rid = j.get("requestId")
    if not isinstance(rid, int):
        raise RuntimeError(f"OD_001 did not return integer requestId. JSON={j}")
    return rid


def poll_od002_until_done(
    s: requests.Session,
    common_headers: dict[str, str],
    settings: AppSettings,
    request_id: int,
) -> dict[str, Any]:
    print("\n=== OD_002 POLL ===", request_id)

    for attempt in range(1, MAX_POLLS + 1):
        r = s.get(f"{settings.od002}/{request_id}", headers=common_headers, timeout=30)
        print(f"[{attempt}] http:", r.status_code, r.reason)
        if r.status_code >= 400:
            print("error body:", (r.text or "")[:2000])
            r.raise_for_status()

        j = r.json()
        status = (j.get("status") or "").lower()
        err = j.get("errorMessage")
        print("status:", status, "| err:", err)

        if status in WAIT_STATUSES:
            time.sleep(POLL_SLEEP_SEC)
            continue
        if status == DONE_STATUS:
            return j

        raise RuntimeError(f"OD_002 returned unexpected status: {json.dumps(j, ensure_ascii=False)[:2000]}")

    raise TimeoutError("Timed out waiting for OD_002 to finish")


def decode_payload_from_od002(j: dict[str, Any]) -> bytes:
    payload_b64 = j.get("payload")
    if not isinstance(payload_b64, str) or not payload_b64:
        raise RuntimeError(f"DONE but no payload. responsePath={j.get('responsePath')}")
    try:
        return base64.b64decode(payload_b64, validate=True)
    except Exception as exc:
        raise RuntimeError("DONE response payload is not valid Base64") from exc
