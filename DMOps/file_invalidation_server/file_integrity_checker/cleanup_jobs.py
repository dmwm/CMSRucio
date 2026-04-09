import os
import sys
import re
import time
import logging

import django
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'file_invalidation_server.settings')
django.setup()

from kubernetes import client, config
from file_integrity_checker.models import FileIntegrityRequest
from file_integrity_checker.tasks import (
    PVC_MOUNT_PATH_HOST,
    NAMESPACE,
)

logging.basicConfig(
    level=logging.INFO,
    format='(%(asctime)s) [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

JOB_NAME_PREFIX      = "file-integrity-job-"
# Files older than this with no running job are considered orphaned
ORPHAN_FILE_MAX_AGE_SECONDS = 3 * 3600  # 3 hours
# Completed/failed jobs older than this are deleted from K8s
COMPLETED_JOB_MAX_AGE_SECONDS = 24 * 3600  # 24 hours


def cleanup():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        logger.error("Could not load in-cluster config.")
        return

    batch_v1 = client.BatchV1Api()
    cleanup_orphaned_pvc_files(batch_v1)
    cleanup_completed_k8s_jobs(batch_v1)


def cleanup_orphaned_pvc_files(batch_v1):
    """
    Deletes LFN files from the PVC that are older than ORPHAN_FILE_MAX_AGE_SECONDS
    and have no corresponding running K8s job.
    """
    if not os.path.exists(PVC_MOUNT_PATH_HOST):
        logger.warning(f"PVC mount path {PVC_MOUNT_PATH_HOST} not found.")
        return

    # Get all currently running job IDs from K8s
    running_job_ids = set()
    jobs = batch_v1.list_namespaced_job(namespace=NAMESPACE)
    for job in jobs.items:
        job_name = job.metadata.name  
        if not job_name.startswith(JOB_NAME_PREFIX):
            continue
        # Job is still running if it has no terminal conditions
        if not job.status.conditions:
            match = re.search(rf"{JOB_NAME_PREFIX}(\w{{8}})", job.metadata.name)
            if match:
                running_job_ids.add(match.group(1))

    now = time.time()
    for filename in os.listdir(PVC_MOUNT_PATH_HOST):
        if not filename.startswith("integrity_") or not filename.endswith(".txt"):
            continue

        filepath = os.path.join(PVC_MOUNT_PATH_HOST, filename)
        file_age = now - os.path.getmtime(filepath)

        if file_age < ORPHAN_FILE_MAX_AGE_SECONDS:
            continue

        # Extract job_id from filename: integrity_<job_id>.txt
        match = re.match(r"integrity_(\w{8})\.txt", filename)
        if not match:
            continue

        job_id = match.group(1)

        if job_id in running_job_ids:
            logger.info(f"File {filename} is old but job {job_id} still running — skipping")
            continue

        try:
            os.remove(filepath)
            logger.info(f"Deleted orphaned LFN file: {filepath}")
        except OSError as e:
            logger.error(f"Could not delete {filepath}: {e}")


def cleanup_completed_k8s_jobs(batch_v1):
    """
    Deletes completed or failed K8s jobs older than COMPLETED_JOB_MAX_AGE_SECONDS.
    Keeps jobs that are still running.
    """
    jobs = batch_v1.list_namespaced_job(namespace=NAMESPACE)
    now = time.time()

    for job in jobs.items:
        job_name = job.metadata.name

        if not job_name.startswith(JOB_NAME_PREFIX):
            continue

        if not job.status.conditions:
            continue  # still running

        # Check completion time
        completion_time = (
            job.status.completion_time or
            job.metadata.creation_timestamp
        )
        if completion_time:
            age = now - completion_time.timestamp()
            if age < COMPLETED_JOB_MAX_AGE_SECONDS:
                logger.info(
                    f"Job {job_name} completed but only {age/3600:.1f}h ago — keeping"
                )
                continue

        try:
            batch_v1.delete_namespaced_job(
                name=job_name,
                namespace=NAMESPACE,
                body=client.V1DeleteOptions(propagation_policy='Foreground')
            )
            logger.info(f"Deleted completed job: {job_name}")
        except client.exceptions.ApiException as e:
            logger.error(f"Could not delete job {job_name}: {e}")


if __name__ == "__main__":
    cleanup()