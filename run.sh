#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="${CES_CONFIG:-$SCRIPT_DIR/config/datasets.json}"

if [[ -z "${PYTHON_BIN:-}" ]]; then
  PYTHON_BIN=python3
fi
RUN_USER="${CES_RUN_USER:-$(id -un)}"
ORG_NAME="${CES_ORG_NAME:?Set CES_ORG_NAME}"
SECRETS_DIR="${CES_SECRETS_DIR:?Set CES_SECRETS_DIR}"
while [[ "$SECRETS_DIR" == */ ]]; do
  SECRETS_DIR="${SECRETS_DIR%/}"
done
for name in APIKEY USER PASS URI; do
  if ! sudo test -f "$SECRETS_DIR/$name"; then
    echo "Missing CES secret file or not accessible via sudo: $SECRETS_DIR/$name" >&2
    exit 2
  fi
done


if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "CES config not found: $CONFIG_PATH" >&2
  exit 2
fi

cmd=(
  "$PYTHON_BIN"
  -m ces_export
  --config "$CONFIG_PATH"
  --org-name "$ORG_NAME"
)

if [[ -n "${CES_EXPORT_OUT_DIR:-}" ]]; then
  cmd+=( --out-dir "$CES_EXPORT_OUT_DIR" )
fi

cmd+=( "$@" )

if [[ "${1:-}" == "--print-cmd" ]]; then
  shift
  printf '%q ' "$PYTHON_BIN" -m ces_export --config "$CONFIG_PATH" --org-name "$ORG_NAME" "$@"
  echo
  exit 0
fi

unit="ces-export-$(date +%s)"

sudo --preserve-env=http_proxy,https_proxy,HTTP_PROXY,HTTPS_PROXY,NO_PROXY,CES_CONFIG \
  systemd-run --collect --unit="$unit" --wait --pipe \
  -E http_proxy="${http_proxy:-}" \
  -E https_proxy="${https_proxy:-}" \
  -E HTTP_PROXY="${HTTP_PROXY:-}" \
  -E HTTPS_PROXY="${HTTPS_PROXY:-}" \
  -E NO_PROXY="${NO_PROXY:-}" \
  -E CES_TRUST_ENV=1 \
  -E CES_CONFIG="$CONFIG_PATH" \
  -p User="$RUN_USER" \
  -p WorkingDirectory="$SCRIPT_DIR" \
  -p LoadCredential=APIKEY:"$SECRETS_DIR/APIKEY" \
  -p LoadCredential=USER:"$SECRETS_DIR/USER" \
  -p LoadCredential=PASS:"$SECRETS_DIR/PASS" \
  -p LoadCredential=URI:"$SECRETS_DIR/URI" \
  "${cmd[@]}"
