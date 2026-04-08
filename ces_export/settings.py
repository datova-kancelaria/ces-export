from __future__ import annotations

import os
import json
from pathlib import Path

import requests

from .models import AppSettings, Credentials


def credentials_directory() -> Path:
    creds_dir_env = os.environ.get("CREDENTIALS_DIRECTORY")
    if not creds_dir_env:
        raise RuntimeError(
            "CREDENTIALS_DIRECTORY not set "
            "(run under systemd with LoadCredential=... or systemd-run -p LoadCredential=...)"
        )
    return Path(creds_dir_env)


def read_cred(creds_dir: Path, name: str) -> str:
    p = creds_dir / name
    b = p.read_bytes()
    while b.endswith(b"\n") or b.endswith(b"\r"):
        b = b[:-1]
    return b.decode("utf-8")


def load_credentials() -> Credentials:
    creds_dir = credentials_directory()
    return Credentials(
        apikey=read_cred(creds_dir, "APIKEY"),
        user=read_cred(creds_dir, "USER"),
        password=read_cred(creds_dir, "PASS"),
    )


def need_url(data: dict[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"URI credential file missing non-empty string key: {key}")
    return value.strip().rstrip("/")


def load_app_settings() -> AppSettings:
    creds_dir = credentials_directory()
    raw = read_cred(creds_dir, "URI")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in URI credential file: {e}") from e

    return AppSettings(
        od001=need_url(data, "od001"),
        od002=need_url(data, "od002"),
        od003=need_url(data, "od003"),
    )


def build_session(creds: Credentials) -> requests.Session:
    s = requests.Session()
    s.auth = (creds.user, creds.password)
    s.trust_env = (os.environ.get("CES_TRUST_ENV") == "1")
    return s


def common_headers(creds: Credentials) -> dict[str, str]:
    return {"APIKey": creds.apikey, "Accept": "application/json"}
