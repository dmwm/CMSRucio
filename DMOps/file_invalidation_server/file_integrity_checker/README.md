# File Integrity Checker

A Django application integrated into the CMS file invalidation server that allows users to submit file integrity check requests for CMS ROOT files stored on WLCG sites. The tool copies files locally, validates checksums, and performs content checks by decompression via a Kubernetes job.

---

## How it works

```
User submits LFNs via API
        │
        ▼
Django creates a FileIntegrityRequest + placeholder FileReplica rows
        │
        ▼
LFN list written to a PVC file
        │
        ▼
Kubernetes job created — runs the integrity check tool container
        │
        ▼
Tool copies each file from WLCG, validates checksum + decompression
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
├── populate_db_integrity.py     ← seed script for local testingseed script for local testing of this app
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
│   ├── process_jobs.py          — CronJob: polls K8s, parses results, updates DB
│   ├── cleanup_jobs.py          — CronJob: removes orphaned PVC files and K8s jobs
│   ├── urls.py                  — URL routing
│   ├── tests.py                 — automated tests
│   └── migrations/              — DB migrations
│
└── controllers/
    ├── job_integrity.yaml                      — Kubernetes Job template for the checker
    ├── cronjob_process_jobs_integrity.yaml     — CronJob for process_jobs.py
    └── cronjob_cleanup_jobs_integrity.yaml     — CronJob for cleanup_jobs.py
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
```json
{
    "message": "2 LFN(s) submitted for integrity check. Job ID: abc12345.",
    "request_id": "9f11159f-b0cf-4722-8c95-fd3d42e665c6",
    "job_id": "abc12345",
    "status": "IN_PROGRESS",
    "query_url": "/api/integrity/query/?request_id=9f11159f-b0cf-4722-8c95-fd3d42e665c6"
}
```

---

### GET `/api/integrity/query/`

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
            "lfn": "/store/data/Run2024/file1.root",
            "scope": "cms",
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

### GET `/api/integrity/files/`

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
GET /api/integrity/files/?request_id=<uuid>
GET /api/integrity/files/?request_id=<uuid>&file_status=FULLY_CORRUPTED
GET /api/integrity/files/?request_id=<uuid>&file_status=FULLY_CORRUPTED&output=lfns
```

**Plain text output (`output=lfns`):**
```
/store/data/Run2024/file1.root
/store/data/Run2024/file2.root
```

---

### GET `/api/integrity/replicas/`

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
GET /api/integrity/replicas/?request_id=<uuid>
GET /api/integrity/replicas/?request_id=<uuid>&replica_status=CORRUPTED
GET /api/integrity/replicas/?request_id=<uuid>&lfn=/store/data/Run2024/file1.root
GET /api/integrity/replicas/?request_id=<uuid>&replica_status=CORRUPTED&output=lfns_per_rse
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
| `FIC_MAX_LFNS_PER_REQUEST` | `20` | Maximum number of LFNs allowed per request |
| `FIC_PVC_MOUNT_PATH_HOST` | `/shared-data-integrity` | Where the Django pod mounts the integrity PVC. Must match the Django deployment yaml volumeMount. |
| `FIC_PVC_MOUNT_PATH_CONTAINER` | `/input` | Where the job container mounts the same PVC. Must match `mountPath` in `job_integrity.yaml`. |
| `FIC_JOB_WORKDIR` | `/tmp` | Writable directory inside the job container for temporary file copies. |
| `FIC_NAMESPACE` | `file-invalidation-tool` | Kubernetes namespace where jobs are created. |
| `FIC_JOB_LOG_VERBOSITY` | `2` | Verbosity level passed to the tool: 0=Error, 1=Warning, 2=Info, 3=Debug. |

---

## Local development

### Prerequisites

- Python 3.9 on lxplus
- CERN environment with Rucio available via CVMFS

### Setup

The server uses MySQL in production but SQLite locally. Add `USE_SQLITE=true` to your `.env` and patch the `DATABASES` block in `../file_invalidation_server/settings.py`:

```python
# Replace the DATABASES block with:
if config('USE_SQLITE', default='false') == 'true':
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': BASE_DIR / 'db.sqlite3',
        }
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.mysql',
            'NAME': config('DB_NAME'),
            'USER': config('DB_USER'),
            'PASSWORD': config('DB_PASSWORD'),
            'HOST': config('DB_HOST', default='localhost'),
            'PORT': config('DB_PORT', default='3306', cast=int),
            'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
        }
    }
```

Update the following elements `../fi_manager/models.py`:

```python
    rse = models.TextField(blank=True, null=True)
    request_user = models.TextField(blank=True, null=True)
    approve_user = models.TextField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'file_invalidation_requests'
        unique_together = (('request_id', 'file_name'),)
```

Create a `../.env` file in the project root:

```bash
SECRET_KEY=any-local-dev-string
USE_SQLITE=true
DB_NAME=db.sqlite3
DB_USER=admin
DB_PASSWORD=password
DB_HOST=localhost
DB_PORT=3306
FIC_MAX_LFNS_PER_REQUEST=20
FIC_PVC_MOUNT_PATH_HOST=/shared-data-integrity
FIC_PVC_MOUNT_PATH_CONTAINER=/input
FIC_JOB_WORKDIR=/tmp
FIC_NAMESPACE=file-invalidation-tool
FIC_JOB_LOG_VERBOSITY=2
```

### Running locally

```bash
# From file_invalidation_server/

# Apply migrations
python3 manage.py makemigrations file_integrity_checker
python3 manage.py migrate file_integrity_checker

# Run tests
python3 manage.py test file_integrity_checker

# Seed the DB with test data for this app
python3 manage.py shell < populate_db_integrity.py

# Start the dev server
python3 manage.py runserver 0.0.0.0:8080
```

Open an SSH tunnel from your laptop:

```bash
ssh -L 8080:localhost:8080 yourusername@lxplus.cern.ch
```

Then visit `http://localhost:8080/api/integrity/submit/`

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

### Running tests

```bash
# From file_invalidation_server/
python3 manage.py test file_integrity_checker
```

43 tests covering: scope parsing, request creation, replica creation, job triggering (mocked), tool output parsing, replica update logic (including idempotency), file status derivation, plain text formatting, and all API endpoints.