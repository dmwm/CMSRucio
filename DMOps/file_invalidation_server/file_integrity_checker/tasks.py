import os
import uuid
import yaml
import logging
from decouple import config as decouple_config
from kubernetes import client, config as k8s_config
from .models import FileIntegrityRequest, FileReplica

logger = logging.getLogger(__name__)

MAX_LFNS_PER_REQUEST = decouple_config('FIC_MAX_LFNS_PER_REQUEST', default=20, cast=int)

# ---------------------------------------------------------------------------
# Path constants — change these if mount paths or filenames change
# ---------------------------------------------------------------------------

# Resolved path to the job yaml template, relative to this file
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir     = os.path.dirname(current_dir)
yaml_path   = os.path.join(src_dir, 'controllers', 'job_integrity.yaml')

# Where the Django pod mounts the integrity PVC
# Must match the Django deployment yaml volumeMount for integrity-input-file-pvc
PVC_MOUNT_PATH_HOST = decouple_config('FIC_PVC_MOUNT_PATH_HOST', default='/shared-data-integrity')

# Where the job container mounts the same PVC
# Must match mountPath of integrity-input-file in job_integrity.yaml
PVC_MOUNT_PATH_CONTAINER = decouple_config('FIC_PVC_MOUNT_PATH_CONTAINER', default='/input')

# Workdir passed to run_check.py — writable directory inside the job container
JOB_WORKDIR = decouple_config('FIC_JOB_WORKDIR', default='/tmp')

# Kubernetes namespace where jobs are created
NAMESPACE = decouple_config('FIC_NAMESPACE', default='file-invalidation-tool')

# Job log verbosity (0=warning, 1=info, 2=debug) passed as -v flags to run_check.py
JOB_LOG_VERBOSITY = decouple_config('FIC_JOB_LOG_VERBOSITY', default=2, cast=int)

# ---------------------------------------------------------------------------


def process_integrity_check(integrity_request, raw_lfns):
    """
    Main entry point called by views.

    1. Validates the LFN list
    2. Creates one FileReplica placeholder row per LFN (rse=None, status=pending)
    3. Triggers the Kubernetes job
    4. Updates the request with job_id and status
    """
    if not raw_lfns:
        raise ValueError("No LFNs provided.")

    if len(raw_lfns) > MAX_LFNS_PER_REQUEST:
        raise ValueError(
            f"Too many LFNs: {len(raw_lfns)} provided, "
            f"maximum is {MAX_LFNS_PER_REQUEST}."
        )

    # Create placeholder rows — one per LFN.
    # rse is None because replicas are only known after the job runs.
    # process_jobs.py will delete these and create real rows from tool output.
    for lfn in raw_lfns:
        scope, bare_lfn = split_scope(lfn)
        FileReplica.objects.create(
            request=integrity_request,
            scope=scope,
            lfn=bare_lfn,
            status='pending'
        )

    logger.info(
        f"Created {len(raw_lfns)} FileReplica placeholder rows "
        f"for request {integrity_request.request_id}"
    )

    job_id, job_status = trigger_job(integrity_request)

    # update_fields must include updated_at explicitly because auto_now=True
    # fields are only auto-updated when update_fields is not specified
    integrity_request.job_id = job_id
    integrity_request.status = job_status
    integrity_request.save(update_fields=['job_id', 'status', 'updated_at'])

    logger.info(
        f"Request {integrity_request.request_id} → "
        f"status={job_status}, job_id={job_id}"
    )

    return job_id, job_status


def trigger_job(integrity_request):
    """
    Writes the LFN input file to the PVC and submits the Kubernetes job.

    The LFN file path written here (PVC_MOUNT_PATH_HOST) and the path
    passed as an arg to the container (PVC_MOUNT_PATH_CONTAINER) point
    to the same file on the same PVC, just via different mount paths:

        Django pod:    writes /shared-data-integrity/integrity_<job_id>.txt
        Job container: reads  /input/integrity_<job_id>.txt

    Returns: (job_id: str, status: str)
    """
    lfns = [
        f"{r.scope}:{r.lfn}"
        for r in integrity_request.replicas.filter(status='pending')
    ]

    job_id   = uuid.uuid4().hex[:8]
    job_name = f"integrity-job-{job_id}"

    # Unique filename per job — no global PVC wipe, safe for concurrent jobs.
    # Cleanup of this file is handled by process_jobs.py after job completion,
    # and by the PVC cleanup CronJob for any orphaned files.
    lfn_filename     = f"integrity_{job_id}.txt"
    lfn_path_host    = os.path.join(PVC_MOUNT_PATH_HOST, lfn_filename)
    lfn_path_container = os.path.join(PVC_MOUNT_PATH_CONTAINER, lfn_filename)

    # Write the LFN file to the PVC from the Django pod
    try:
        with open(lfn_path_host, "w") as f:
            f.write("\n".join(lfns))
        logger.info(f"Wrote {len(lfns)} LFNs to {lfn_path_host}")
    except OSError as e:
        # PVC not mounted locally — expected in local dev, fatal in production
        logger.warning(
            f"Could not write LFN file to {lfn_path_host}: {e}. "
            f"PVC not mounted — expected in local dev."
        )

    # Build args for run_check.py:
    # python3 run_check.py <lfn_file> <workdir> [--rse-expression X] [--full-scan]
    args = [lfn_path_container, JOB_WORKDIR]

    if integrity_request.rse_expression:
        args += ["--rse-expression", integrity_request.rse_expression]

    if integrity_request.full_scan:
        args.append("--full-scan")
        
    if JOB_LOG_VERBOSITY > 0:
        args.append("-" + "v" * JOB_LOG_VERBOSITY)

    # Load yaml template and patch name and args
    with open(yaml_path) as f:
        job_definition = yaml.safe_load(f)

    job_definition['metadata']['name'] = job_name
    job_definition['spec']['template']['spec']['containers'][0]['args'] = args

    # Submit to Kubernetes
    try:
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            raise Exception(
                "Could not load in-cluster Kubernetes config. "
                "Job submission only works inside the OKD cluster."
            )

        batch_v1 = client.BatchV1Api()
        batch_v1.create_namespaced_job(
            namespace=NAMESPACE,
            body=job_definition
        )
        logger.info(
            f"Job {job_name} created in namespace {NAMESPACE} | "
            f"request={integrity_request.request_id} | "
            f"user={integrity_request.requested_by} | "
            f"LFNs={len(lfns)} | "
            f"rse_expression={integrity_request.rse_expression} | "
            f"full_scan={integrity_request.full_scan}"
        )
        return job_id, FileIntegrityRequest.Status.IN_PROGRESS

    except Exception as e:
        logger.error(
            f"Failed to create job {job_name} for request "
            f"{integrity_request.request_id}: {e}"
        )
        return job_id, FileIntegrityRequest.Status.FAILED


def split_scope(lfn):
    """
    Split 'cms:/store/data/...' into ('cms', '/store/data/...').
    Defaults to 'cms' if no scope prefix is given.
    """
    if ':' in lfn:
        scope, bare = lfn.split(':', 1)
        return scope.strip(), bare.strip()
    return 'cms', lfn.strip()