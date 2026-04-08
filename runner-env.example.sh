#!/usr/bin/env bash
# Copy this file to a machine-local path such as:
#   /opt/ces-export/runner-env.sh
# Then replace the placeholder values with real ones.
#
# This file is meant to be sourced:
#   source /opt/ces-export/runner-env.sh

# Python interpreter used by the pipeline.
# Leave empty to fall back to `python3` on PATH.
export PYTHON_BIN=/absolute/path/to/venv/bin/python

# Persistent output location for exported datasets.
# Set to empty string to disable scheduled runs.
export CES_EXPORT_OUT_DIR=/absolute/path/to/persistent/ces-export-data

# Optional alternate config file.
# Leave empty to use the repository default: <repo>/config/datasets.json
# export CES_CONFIG=/opt/ces-export/datasets.json

# Directory containing CES credential files.
# Expected files:
#   APIKEY
#   USER
#   PASS
#   URI
export CES_SECRETS_DIR=/absolute/path/to/ces-secrets

# Stable organization name used by the CES pipeline.
export CES_ORG_NAME='your organization name here'

# Optional override for the Unix user that runs CES through systemd-run.
# export CES_RUN_USER="$USER"

# Optional proxy configuration.
# export http_proxy=http://proxy-host:3128
# export https_proxy=http://proxy-host:3128
# export HTTP_PROXY=http://proxy-host:3128
# export HTTPS_PROXY=http://proxy-host:3128
# export NO_PROXY=localhost,127.0.0.1
