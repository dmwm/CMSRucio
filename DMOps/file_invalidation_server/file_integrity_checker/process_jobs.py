import os
import sys
import re
import json
import logging

import django
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'file_invalidation_server.settings')
django.setup()

from kubernetes import client, config
from file_integrity_checker.models import FileIntegrityRequest, FileReplica
from file_integrity_checker.tasks import (
    PVC_MOUNT_PATH_HOST,
    NAMESPACE,
    split_scope
)

logging.basicConfig(
    level=logging.INFO,
    format='(%(asctime)s) [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

JOB_NAME_PREFIX = "file-integrity-job-"


def fetch_and_process():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        logger.error("Could not load in-cluster Kubernetes config.")
        return

    batch_v1 = client.BatchV1Api()
    core_v1  = client.CoreV1Api()

    jobs = batch_v1.list_namespaced_job(namespace=NAMESPACE)

    for job in jobs.items:
        job_name = job.metadata.name

        if not job_name.startswith(JOB_NAME_PREFIX):
            continue
        
        # Skip if this is a CronJob-spawned job (shouldn't happen with our naming, but safe)
        if job.metadata.owner_references:
            owner_kinds = [ref.kind for ref in job.metadata.owner_references]
            if 'CronJob' in owner_kinds:
                continue

        if not job.status.conditions:
            logger.info(f"Job {job_name} still running — skipping")
            continue

        condition_types = {c.type: c.status for c in job.status.conditions}

        if condition_types.get("Complete") == "True":
            logger.info(f"Job {job_name} completed — processing results")
            handle_completed_job(job_name, batch_v1, core_v1)

        elif condition_types.get("Failed") == "True":
            logger.warning(f"Job {job_name} failed — marking request as failed")
            handle_failed_job(job_name, core_v1)


def read_pod_logs(job_name, core_v1):
    """
    Finds the most recent pod for a job and reads its logs.
    Returns log string or None if no pod found.
    """
    pods = core_v1.list_namespaced_pod(
        namespace=NAMESPACE,
        label_selector=f"job-name={job_name}"
    )

    try:
        latest_pod = sorted(
            pods.items,
            key=lambda p: p.status.start_time or p.metadata.creation_timestamp,
            reverse=True
        )[0]
    except IndexError:
        logger.error(f"No pods found for job {job_name}")
        return None

    pod_name = latest_pod.metadata.name
    job_id   = extract_job_id(job_name)
    logger.info(f"Reading logs from pod {pod_name} (job_id={job_id})")

    try:
        return core_v1.read_namespaced_pod_log(pod_name, namespace=NAMESPACE)
    except client.exceptions.ApiException as e:
        logger.error(f"Could not read logs for pod {pod_name}: {e}")
        return None


def handle_completed_job(job_name, batch_v1, core_v1):
    job_id = extract_job_id(job_name)
    if not job_id:
        logger.error(f"Could not extract job_id from: {job_name}")
        return

    try:
        integrity_request = FileIntegrityRequest.objects.get(job_id=job_id)
    except FileIntegrityRequest.DoesNotExist:
        logger.error(f"No FileIntegrityRequest for job_id: {job_id}")
        return

    logs = read_pod_logs(job_name, core_v1)

    # Always save raw logs to DB — they survive pod deletion
    if logs:
        integrity_request.logs = logs
        integrity_request.save(update_fields=['logs', 'updated_at'])

    try:
        results = parse_tool_output(logs or "")
        update_replicas(integrity_request, results)
        integrity_request.status = FileIntegrityRequest.Status.COMPLETED
        integrity_request.save(update_fields=['status', 'updated_at'])
        logger.info(
            f"Request {integrity_request.request_id} → COMPLETED | "
            f"job_id={job_id} | processed {len(results)} LFN(s)"
        )
    except Exception as e:
        logger.error(
            f"Failed to process results for job {job_name} "
            f"(job_id={job_id}): {e}"
        )
        integrity_request.status = FileIntegrityRequest.Status.FAILED
        integrity_request.logs   = f"{str(e)}\n\n{logs or 'No logs available.'}"
        integrity_request.save(update_fields=['status', 'logs', 'updated_at'])

    cleanup_lfn_file(job_id)


def handle_failed_job(job_name, core_v1):
    job_id = extract_job_id(job_name)
    if not job_id:
        return

    try:
        integrity_request = FileIntegrityRequest.objects.get(job_id=job_id)
    except FileIntegrityRequest.DoesNotExist:
        logger.error(f"No FileIntegrityRequest for job_id: {job_id}")
        return

    # Read and save logs even for failed jobs before pod is deleted
    logs = read_pod_logs(job_name, core_v1)

    integrity_request.status = FileIntegrityRequest.Status.FAILED
    integrity_request.logs   = logs or "Job failed. No logs available."
    integrity_request.save(update_fields=['status', 'logs', 'updated_at'])

    logger.warning(
        f"Request {integrity_request.request_id} → FAILED | "
        f"job_id={job_id}"
    )

    cleanup_lfn_file(job_id)


def parse_tool_output(logs: str) -> list:
    """
    Parses tool output from pod logs.
    The tool prints results as JSON via json.dumps() on the last line.
    We scan backwards for the first line starting with '[' to be robust
    against any logging output that precedes it.
    """
    result_line = None
    for line in reversed(logs.splitlines()):
        line = line.strip()
        if line.startswith('['):
            result_line = line
            break

    if not result_line:
        raise ValueError("Could not find results list in pod logs.")

    results = json.loads(result_line)

    if not isinstance(results, list):
        raise ValueError(
            f"Expected a list from tool output, got: {type(results)}"
        )

    return results


def update_replicas(integrity_request, results: list):
    """
    Replaces placeholder FileReplica rows with real results from the tool.

    For each LFN:
    - Deletes the pending placeholder row
    - If tool reported a Rucio-level error: creates one ERROR row
    - Otherwise: creates one row per replica found
    Uses get_or_create to be idempotent if called twice.
    """
    for file_result in results:
        lfn_with_scope = file_result.get("filename")
        if not lfn_with_scope:
            logger.warning(f"Missing filename in result: {file_result}")
            continue

        scope, bare_lfn = split_scope(lfn_with_scope)

        deleted, _ = FileReplica.objects.filter(
            request=integrity_request,
            lfn=bare_lfn,
            status='pending'
        ).delete()
        logger.info(
            f"Deleted {deleted} placeholder row(s) for LFN {bare_lfn}"
        )

        # Rucio-level error — couldn't even look up replicas
        if "error" in file_result:
            FileReplica.objects.get_or_create(
                request=integrity_request,
                lfn=bare_lfn,
                rse=None,
                defaults={
                    "scope":  scope,
                    "status": "ERROR",
                    "pfn":    None,
                }
            )
            logger.warning(
                f"LFN {bare_lfn} Rucio error: {file_result['error']}"
            )
            continue

        replicas = file_result.get("replicas", [])
        if not replicas:
            logger.warning(f"No replicas found for LFN {bare_lfn}")

        for replica in replicas:
            FileReplica.objects.get_or_create(
                request=integrity_request,
                lfn=bare_lfn,
                rse=replica.get("rse"),
                defaults={
                    "scope":  scope,
                    "status": replica.get("status", "ERROR"),
                    "pfn":    replica.get("pfn"),
                }
            )

        logger.info(
            f"LFN {bare_lfn} → {len(replicas)} replica(s) processed"
        )


def cleanup_lfn_file(job_id: str):
    lfn_filepath = os.path.join(PVC_MOUNT_PATH_HOST, f"integrity_{job_id}.txt")
    try:
        os.remove(lfn_filepath)
        logger.info(f"Cleaned up LFN file: {lfn_filepath}")
    except FileNotFoundError:
        logger.warning(f"LFN file already gone (OK): {lfn_filepath}")
    except OSError as e:
        logger.error(f"Could not delete LFN file {lfn_filepath}: {e}")


def extract_job_id(job_name: str) -> str:
    match = re.search(rf"{JOB_NAME_PREFIX}(\w{{8}})", job_name)
    return match.group(1) if match else None


if __name__ == "__main__":
    fetch_and_process()