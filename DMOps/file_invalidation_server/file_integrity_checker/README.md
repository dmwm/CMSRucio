# File Integrity Checker

A Django application integrated into the CMS file invalidation server that allows users to submit file integrity check requests for CMS ROOT files stored on WLCG sites. The tool copies files locally, validates checksums, and performs content checks by decompression via a Kubernetes job.

---

## How it works

```
User submits LFNs via API
        │
        ▼
Django creates FileIntegrityRequest (status=SUBMITTED)
+ placeholder FileReplica rows
Returns request_id immediately — user does not wait
        │
        ▼
queue_processor.py CronJob (every 1 min)
Checks: how many jobs IN_PROGRESS?
If below FIC_MAX_CONCURRENT_JOBS: triggers next SUBMITTED request
If at limit: skips — tries again next minute
        │
        ▼
Kubernetes job created — runs the integrity check tool container
Tool copies files from WLCG, validates checksum + decompression
Prints JSON results to stdout
        │
        ▼
process_jobs.py CronJob (every 5 min) polls K8s for completed jobs
Reads pod logs, parses JSON, updates FileReplica rows with real results
Sets request status to COMPLETED or FAILED
        │
        ▼
User queries results via API
```

### Key concepts

- **One request per submission** — a user submits 1–N LFNs in a single request
- **One placeholder replica per LFN at submission** — real replicas (one per RSE) are populated after the job completes
- **Replicas are replaced, not updated** — `process_jobs.py` deletes placeholder rows and creates real rows from tool output
- **Logs are persisted** — raw pod logs are saved to the DB so results survive pod deletion

---

## Project structure

```
file_invalidation_server/
├── manage.py
├── requirements.txt
├── populate_db_integrity.py     ← seed script for local testing seed script for local testing of this app
│
├── file_invalidation_server/    ← Django project settings
│   ├── settings.py
│   └── urls.py
│
├── fi_manager/
│   └── utils.py                 ← provides get_cern_username() — used by this app
│
├── file_integrity_checker/      ← THIS APP
│   ├── models.py                — DB models
│   ├── serializers.py           — API input validation
│   ├── views.py                 — all API views
│   ├── tasks.py                 — job trigger logic + constants
│   ├── process_queue.py         — CronJob: picks up SUBMITTED requests, triggers jobs
│   ├── process_jobs.py          — CronJob: polls K8s, parses results, updates DB
│   ├── cleanup_jobs.py          — CronJob: removes orphaned PVC files and K8s jobs
│   ├── urls.py                  — URL routing
│   ├── tests.py                 — automated tests
│   └── migrations/              — DB migrations
│
└── controllers/
    ├── job_integrity.yaml                      — Kubernetes Job template for the checker
    ├── cronjob_integrity_process_queue.yaml    — CronJob for process_queue.py
    ├── cronjob_process_jobs.yaml               — CronJob (jobs-log-processor): runs both
    │                                             fi_manager/process_jobs.py and
    │                                             file_integrity_checker/process_jobs.py
    └── cronjob_integrity_cleanup_jobs.yaml     — CronJob for cleanup_jobs.py
```

---

## Data models

### FileIntegrityRequest

One row per user submission.

| Field | Type | Description |
|---|---|---|
| `request_id` | UUID | Unique identifier for the request |
| `requested_by` | string | CERN email from oauth2-proxy header |
| `rse_expression` | string | Optional RSE filter passed to the tool |
| `full_scan` | bool | Whether to read every basket (slower) |
| `status` | string | `SUBMITTED` → `IN_PROGRESS` → `COMPLETED` / `FAILED` |
| `job_id` | string | 8-character Kubernetes job identifier |
| `logs` | text | Raw pod logs saved after job completes |
| `created_at` | datetime | Auto-set on creation |
| `updated_at` | datetime | Auto-updated on every save |

### FileReplica

One row per replica per LFN. Created as `pending` at submission, replaced with real results after job completes.

| Field | Type | Description |
|---|---|---|
| `request` | FK | Parent FileIntegrityRequest |
| `scope` | string | Rucio scope (default: `cms`) |
| `lfn` | string | Logical file name |
| `rse` | string | RSE name (null until job completes) |
| `pfn` | string | Physical file name / full URL |
| `status` | string | `pending` / `OK` / `CORRUPTED` / `ERROR` |

---

## API reference

Base path: `/api/integrity/`

Authentication is handled by oauth2-proxy at the ingress level. All requests must be authenticated via CERN SSO.

---

### POST `/api/integrity/submit/`

Submit a new file integrity check request.

**Fields:**

| Field | Required | Description |
|---|---|---|
| `lfns` | Yes | LFNs to check, one per line. Scope optional — `cms` used as default. Max `FIC_MAX_LFNS_PER_REQUEST` LFNs. |
| `rse_expression` | No | RSE expression to filter replicas. If omitted, all replicas are checked. |
| `full_scan` | No | If true, reads every basket. More thorough but slower. Default: false. |

**Example response (201 Created):**

The request is only *queued* at submit time — `process_queue.py` assigns the
`job_id` and moves it to `IN_PROGRESS` on its next run. So the response reports
`status: SUBMITTED` and a `job_id` of `null`. Poll the `links` for progress.

```json
{
    "message": "2 LFN(s) submitted for integrity check. The request is queued; a job will be assigned shortly. Track progress via the links below.",
    "request_id": "...",
    "job_id": null,
    "status": "SUBMITTED",
    "links": {
        "detail":   "https://file-invalidation.app.cern.ch/api/integrity/query/requests/?request_id=...",
        "files":    "https://file-invalidation.app.cern.ch/api/integrity/query/files/?request_id=...",
        "replicas": "https://file-invalidation.app.cern.ch/api/integrity/query/replicas/?request_id=..."
    }
}
```

---

### GET `/api/integrity/query/requests/`

Query requests.

#### List view (no `request_id`)

**Parameters:**

| Param | Description |
|---|---|
| `status` | Filter by request status: `SUBMITTED`, `IN_PROGRESS`, `COMPLETED`, `FAILED` |
| `include_summary` | Include aggregate replica counts per request. Default: `false` |

#### Detail view (`request_id` required)

**Parameters:**

| Param | Default | Description |
|---|---|---|
| `request_id` | — | UUID of the request |
| `include_replicas` | `true` | Include per-file replica breakdown |
| `include_summary` | `true` | Include aggregate counts |
| `include_logs` | `false` | Include raw Kubernetes pod logs (can be large) |

**Example response:**
```json
{
    "request_id": "...",
    "requested_by": "username_c",
    "status": "COMPLETED",
    "job_id": "abc12345",
    "created_at": "2026-04-10T20:06:38.131276Z",
    "updated_at": "2026-04-11T21:00:03.470805Z",
    "links": {
        "details":   "https://file-invalidation.app.cern.ch/api/integrity/query/requests/?request_id=...",
        "files":    "https://file-invalidation.app.cern.ch/api/integrity/query/files/?request_id=...",
        "replicas": "https://file-invalidation.app.cern.ch/api/integrity/query/replicas/?request_id=..."
    }
    "summary": {
        "lfn_count": 2,
        "replica_count": 5,
        "OK": 3,
        "CORRUPTED": 1,
        "ERROR": 1,
        "pending": 0
    },
    "files": [
        {
            "scope": "cms",
            "lfn": "/store/data/Run2024/file1.root",
            "file_status": "PARTIALLY_CORRUPTED",
            "summary": {"replica_count": 3, "OK": 2, "CORRUPTED": 1, "ERROR": 0, "pending": 0},
            "replicas": [
                {"rse": "T1_US_FNAL_Disk", "pfn": "davs://...", "status": "OK"},
                {"rse": "T0_CH_CERN_Tape", "pfn": "davs://...", "status": "CORRUPTED"}
            ]
        }
    ]
}
```

---

### GET `/api/integrity/query/files/`

Query files (LFNs) within a request. `request_id` always required.

**Parameters:**

| Param | Description |
|---|---|
| `request_id` | UUID of the request (required) |
| `file_status` | Filter by derived file status (see values below) |
| `output` | `lfns` — returns plain text LFN list instead of JSON |

**File status values:**

| Value | Condition |
|---|---|
| `FULLY_CORRUPTED` | All replicas are CORRUPTED |
| `PARTIALLY_CORRUPTED` | At least one replica is CORRUPTED |
| `FULLY_OK` | All replicas are OK |
| `OK` | At least one replica is OK |
| `PENDING` | At least one replica is pending |
| `ERROR` | At least one replica errored |
| `UNKNOWN` | No replicas or unrecognised combination |

**Examples:**
```
GET /api/integrity/query/files/?request_id=<uuid>
GET /api/integrity/query/files/?request_id=<uuid>&file_status=FULLY_CORRUPTED
GET /api/integrity/query/files/?request_id=<uuid>&file_status=FULLY_CORRUPTED&output=lfns
```

**Plain text output (`output=lfns`):**
```
/store/data/Run2024/file1.root
/store/data/Run2024/file2.root
```

---

### GET `/api/integrity/query/replicas/`

Query individual replicas within a request. `request_id` always required.

**Parameters:**

| Param | Description |
|---|---|
| `request_id` | UUID of the request (required) |
| `replica_status` | Filter by raw replica status: `OK`, `CORRUPTED`, `ERROR`, `pending` |
| `lfn` | Filter by specific LFN |
| `output` | `lfns_per_rse` — returns plain text grouped by RSE instead of JSON |

The `output=lfns_per_rse` parameter works with any combination of filters.

**Examples:**
```
GET /api/integrity/query/replicas/?request_id=<uuid>
GET /api/integrity/query/replicas/?request_id=<uuid>&replica_status=CORRUPTED
GET /api/integrity/query/replicas/?request_id=<uuid>&lfn=/store/data/Run2024/file1.root
GET /api/integrity/query/replicas/?request_id=<uuid>&replica_status=CORRUPTED&output=lfns_per_rse
```

**Plain text output (`output=lfns_per_rse`):**
```
T1_UK_RAL_Disk
/store/data/Run2024/file1.root

T1_US_FNAL_Disk
/store/data/Run2024/file1.root
/store/data/Run2024/file2.root
```

RSEs are sorted alphabetically. Blocks are separated by a blank line.

---

## Environment variables

All variables use the `FIC_` prefix (File Integrity Checker). All have sensible defaults and are optional in local development.

| Variable | Default | Description |
|---|---|---|
| `FIC_MAX_LFNS_PER_REQUEST` | `20` | Maximum number of LFNs allowed per request. |
| `FIC_MAX_CONCURRENT_JOBS` | `3` | Maximum number of integrity check jobs running simultaneously. |
| `FIC_PVC_MOUNT_PATH_HOST` | `/shared-data-integrity` | Where the Django pod mounts the integrity PVC. Must match the Django deployment yaml volumeMount. |
| `FIC_PVC_MOUNT_PATH_CONTAINER` | `/input` | Where the job container mounts the same PVC. Must match `mountPath` in `job_integrity.yaml`. |
| `FIC_JOB_WORKDIR` | `/tmp` | Writable directory inside the job container for temporary file copies. |
| `FIC_NAMESPACE` | `file-invalidation-tool` | Kubernetes namespace where jobs are created. |
| `FIC_JOB_LOG_VERBOSITY` | `2` | Verbosity level passed to the tool: 0=Error, 1=Warning, 2=Info, 3=Debug. |

---

## Local development

### Prerequisites

- Python 3.9+ (lxplus has 3.9; 3.11–3.13 also work in a venv)
- CERN environment with Rucio available via CVMFS (only needed to exercise the
  real tool — not needed to run the test suite)

### Self-contained local mode (`LOCAL_TESTING`)

The server uses MySQL and CERN secrets in production. For local work, set the
`LOCAL_TESTING=true` environment variable. This is read in `settings.py` and
switches the database to a local SQLite file and fills the required secrets
(`SECRET_KEY`, `JIRA_PAT`, `GROUP_AUTHORIZATION_CLIENT_SECRET`) with harmless dev
defaults — **no `.env` file and no source edits are required.** With
`LOCAL_TESTING` unset (the default) production behaviour is unchanged.

```bash
# From file_invalidation_server/ — install deps once into a venv
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### Running the tests

```bash
# From file_invalidation_server/
LOCAL_TESTING=true python3 manage.py test file_integrity_checker
```

The test DB is built from the migrations (the `managed = False` models still get
their tables created during test setup). 62 tests covering: scope parsing,
request creation, replica creation, queue processing (mocked K8s), tool output
parsing, replica update logic (including idempotency), file-status derivation,
boolean query-param coercion, plain-text formatting, HTML template rendering,
and all API endpoints.

### Running the dev server + accessing the UI

```bash
# From file_invalidation_server/
LOCAL_TESTING=true python3 manage.py migrate
LOCAL_TESTING=true python3 manage.py shell < populate_db_integrity.py   # optional seed data
LOCAL_TESTING=true python3 manage.py runserver 0.0.0.0:8080
```

If you are on lxplus, open an SSH tunnel from your laptop:

```bash
ssh -L 8080:localhost:8080 yourusername@lxplus.cern.ch
```

The browsable UI pages (rendered HTML) are:

| Page | URL |
|---|---|
| Submit a request (form) | `http://localhost:8080/api/integrity/submit/` |
| Requests list | `http://localhost:8080/api/integrity/query/requests/` |
| Request detail (files + replicas) | `…/api/integrity/query/requests/?request_id=<uuid>` |
| Files / replicas (linked from detail) | `…/api/integrity/query/files/` and `…/query/replicas/` |

In production the same pages live under `https://file-invalidation.app.cern.ch`.

### Expected local behaviour

The following are not errors — they are expected in local development:

- **PVC write warning** — `Could not write LFN file to /shared-data-integrity/...` — the PVC does not exist locally
- **K8s config error** — `Could not load in-cluster Kubernetes config` — jobs can only be submitted from inside the OKD cluster
- **`requested_by: unknown`** — oauth2-proxy is not present locally so the username header is never set

To simulate the production username header locally:
```bash
curl -X POST http://localhost:8080/api/integrity/submit/ \
  -H "X-Forwarded-Preferred-Username: yourusername" \
  -H "Content-Type: application/json" \
  -d '{"lfns": "cms:/store/data/Run2024/file.root"}'
```

---

## Deployment

The app runs on CERN's OKD (OpenShift) cluster in the `file-invalidation-tool`
namespace, alongside the file invalidation server (they share the Django project
and image `registry.paas.cern.ch/file-invalidation-tool/...`).

**Managed in OKD (not in this repo):** the Django `Deployment`, its `Route`
(`https://file-invalidation.app.cern.ch`), the oauth2-proxy sidecar that injects
the CERN SSO username header, the `integrity-input-file-pvc`, and the DB/secret
objects referenced by the manifests below.

**Versioned in this repo** — apply with `oc apply -f <file> -n file-invalidation-tool`:

| Manifest | Kind | Role |
|---|---|---|
| `controllers/job_integrity.yaml` | Job | Template the queue processor patches and submits per request |
| `controllers/cronjob_integrity_process_queue.yaml` | CronJob (1 min) | Runs `process_queue.py` — picks up SUBMITTED requests |
| `controllers/cronjob_process_jobs.yaml` | CronJob (5 min) | `jobs-log-processor`: runs **both** `fi_manager/process_jobs.py` and `file_integrity_checker/process_jobs.py` |
| `controllers/cronjob_integrity_cleanup_jobs.yaml` | CronJob | Runs `cleanup_jobs.py` — removes orphaned PVC files and old K8s jobs |

> Note: any management script that calls `django.setup()` (the three CronJobs
> above) needs the same env/secrets as the web pod — `SECRET_KEY`, `JIRA_PAT`,
> `GROUP_AUTHORIZATION_CLIENT_SECRET`, and `DB_*` — because `settings.py` reads
> them at import time.