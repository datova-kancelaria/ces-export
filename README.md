# ces-export

Fetches datasets from CES open-data endpoints and writes them to a filesystem tree according to a JSON schedule config.

## What it does

`ces-export` talks to the CES OD_001 / OD_002 / OD_003 endpoints, chooses an organization, plans dataset/date windows from `config/datasets.json`, downloads the payloads, and then optionally merges or postprocesses them.

It supports:

- per-dataset schedules
- per-format enable/disable flags
- chunking large time ranges into smaller windows
- CSV merging
- RDF/XML graph merge with a repair retry path
- postprocessing such as CSV → XLSX and RDF/XML → JSON-LD
- dry-run and selective dataset inclusion/exclusion

## Files and directories

- `run.sh` — wrapper that launches the Python package under `systemd-run` with `LoadCredential=`
- `config/datasets.json` — dataset scheduling and format/chunking config
- `ces_export/__main__.py` — CLI entrypoint
- `ces_export/ces_api.py` — OD_001 / OD_002 / OD_003 HTTP calls
- `ces_export/settings.py` — loads credentials and endpoint URLs from systemd credentials
- `ces_export/planner.py` — expands schedules into concrete jobs
- `ces_export/runner.py` — executes jobs, writes chunks, merges and postprocesses
- `ces_export/mergers.py` — CSV and RDF/XML merge logic
- `ces_export/postprocess.py` — extra output conversions

## Runtime requirements

This module is the only part of the repo that requires non-public credentials.

Required when CES is enabled:

- `CES_ORG_NAME`
- `CES_SECRETS_DIR`
- `CES_EXPORT_OUT_DIR` (or pass `--out-dir` to the wrapper / CLI)
- credential files inside `CES_SECRETS_DIR`:
  - `APIKEY`
  - `USER`
  - `PASS`
  - `URI`

Optional:

- `CES_CONFIG` — alternate config path. Default: `config/datasets.json`
- `CES_RUN_USER` — Unix user for `systemd-run`
- `http_proxy`, `https_proxy`, `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`
- `PYTHON_BIN` — defaults to `python3` if unset

At runtime the Python package itself expects `CREDENTIALS_DIRECTORY`, which is supplied automatically by `systemd-run -p LoadCredential=...` in `run.sh`.

## Runner-local endpoint file

The CES endpoint URLs are not hardcoded in the Python package.

They are loaded at runtime from `CREDENTIALS_DIRECTORY/URI`, where `URI` is a machine-local JSON file populated from the endpoint information provided by MFSR. The file should have the following structure:

```json
{
  "od001": "https://.../API_OD_001",
  "od002": "https://.../API_OD_002",
  "od003": "https://.../API_OD_003"
}
```

`run.sh` passes this file into the service as the `URI` systemd credential.

## Wrapper usage

Typical env-driven invocation:

```bash
source /opt/ces-export/runner-env.sh
./run.sh
```

Typical repo-level invocation:

```bash
./run.sh --out-dir /path/to/output
```

Show the command that would be run:

```bash
./run.sh --print-cmd --out-dir /path/to/output
```

The wrapper always passes:

- `--config <path>`
- `--org-name "$CES_ORG_NAME"`
- any extra CLI arguments supplied by the caller

## Python CLI arguments

From `python -m ces_export`:

- `--config PATH` — required config JSON
- `--hierarchy-node-code CODE` — exact OD_003 code
- `--org-name TEXT` — substring match against OD_003 org names
- `--list-orgs` — print organizations and exit
- `--list-orgs-filter TEXT` — filter for `--list-orgs`
- `--no-cache-org` — do not read/write `.hierarchy_node_code.txt`
- `--today YYYY-MM-DD` — override current date for testing
- `--out-dir PATH` — sets the output dir
- `--dry-run` — print planned work only
- `--force` — ignore matching metadata and refetch
- `--start-year N`, `--end-year N` — override schedule year bounds
- `--include-dataset NAME` — include only selected dataset(s)
- `--exclude-dataset NAME` — skip selected dataset(s)

## Config model

See `config/datasets.json`.

Important parts:

- `defaults.formats.<fmt>` — default per-format behavior
- `datasets.<name>.schedules[]` — one or more schedules per dataset
- `datasets.<name>.formats.<fmt>` — dataset-specific format overrides

Format options:

- `enabled`
- `window.mode`
- `window.size`
- `merge_strategy`
- `postprocess`
- `keep_chunks`

Common window modes used by the repo:

- `none`
- `calendar_month`

Merge strategies implemented by the runner:

- `csv_header`
- `rdfxml_graph`
- `concat`
- `skip_if_chunked`
- `keep_chunks`

## Execution flow

1. load config JSON
2. resolve output directory from `--out-dir` or `CES_EXPORT_OUT_DIR`
3. load CES credentials and endpoint URLs from `CREDENTIALS_DIRECTORY`
4. fetch OD_003 organizations
5. choose organization by `--hierarchy-node-code`, `--org-name`, `CES_ORG_NAME`, or cached code
6. build concrete export jobs from schedules
7. for each job:
   - skip if metadata already matches and the main output exists
   - split date ranges into chunks according to `window`
   - submit an OD_001 request whose payload contains:
     - `datasetName`
     - `hierarchyNodeCode`
     - `dateFrom`
     - `dateTo`
     - `fileFormat`
   - the OD_001 request body is sent as JSON with:
     - `operation: "opendata"`
     - `payload: "..."`, where `payload` is the Base64-encoded JSON object listed above
   - the OD_001 request creates an asynchronous export request and returns an integer `requestId`
   - poll `OD_002/<requestId>` until the export is ready
     - while the job is still being processed, OD_002 returns `status: "processing"`
     - when the export is ready, OD_002 returns `status: "done"`
     - in the `done` response, the exported dataset is returned in the same `OD_002` response under `payload`
     - `payload` is a Base64-encoded string containing the raw output file bytes (for example CSV or XML, depending on the requested format)
     - the runner decodes that Base64 string and writes the resulting bytes to the chunk file
   - write chunk payloads
   - merge or keep chunks according to `merge_strategy`
   - run postprocessing steps
   - write metadata and chunk manifest files

## Output behavior

Depending on the schedule and format, a dataset may produce:

- a single merged file
- multiple chunk files plus a manifest
- postprocessed derivatives such as `.xlsx` or `.jsonld`

The runner does not pretend every dataset produces one merged file. `RunResult` records whether the dataset was skipped, merged, or left as chunks.

## XML merge note

For RDF/XML merges, the runner first attempts a normal graph parse/merge. If that fails, it retries after applying `ces_export/rdfxml_repair.py` to the chunks. The manifest records whether the merge succeeded immediately, succeeded after repair, or failed after the retry.

## CES endpoint details

The CES flow is asynchronous:

- `OD_003` lists available organizations / hierarchy nodes
- `OD_001` creates an export request
- `OD_002/<requestId>` is polled until the request finishes

In the current implementation:

- `OD_001` is called as a JSON `POST`
- the top-level request body contains `operation` and `payload`
- `payload` is not raw JSON; it is a Base64-encoded JSON object
- after `OD_002` returns `status: "done"`, the runner reads the dataset from the same OD_002 response field `payload`
- that `payload` is Base64-decoded into raw file bytes and written directly to disk
- `responsePath` is not used by the current implementation
