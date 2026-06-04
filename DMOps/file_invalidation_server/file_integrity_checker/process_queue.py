import os
import sys
import logging

import django
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'file_invalidation_server.settings')
django.setup()

from file_integrity_checker.models import FileIntegrityRequest
from file_integrity_checker.tasks import trigger_job, MAX_CONCURRENT_JOBS

logging.basicConfig(
    level=logging.INFO,
    format='(%(asctime)s) [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def process_queue():
    """
    Picks up SUBMITTED requests and triggers their K8s jobs
    when the number of IN_PROGRESS jobs is below the limit.

    Called by a CronJob every minute. Each run:
      1. Counts currently running jobs
      2. Fills available slots with oldest SUBMITTED requests
      3. Triggers one job per available slot
    """
    in_progress = FileIntegrityRequest.objects.filter(
        status=FileIntegrityRequest.Status.IN_PROGRESS
    ).count()

    available_slots = MAX_CONCURRENT_JOBS - in_progress

    if available_slots <= 0:
        logger.info(
            f"No available slots — "
            f"{in_progress}/{MAX_CONCURRENT_JOBS} jobs running"
        )
        return

    logger.info(
        f"{in_progress}/{MAX_CONCURRENT_JOBS} jobs running — "
        f"{available_slots} slot(s) available"
    )

    # Pick oldest SUBMITTED requests — FIFO order
    pending = FileIntegrityRequest.objects.filter(
        status=FileIntegrityRequest.Status.SUBMITTED
    ).order_by('created_at')[:available_slots]

    if not pending:
        logger.info("No queued requests waiting")
        return

    for integrity_request in pending:
        logger.info(
            f"Triggering job for request {integrity_request.request_id} "
            f"by {integrity_request.requested_by} "
            f"({integrity_request.replicas.count()} LFNs)"
        )
        job_id, job_status = trigger_job(integrity_request)
        integrity_request.job_id = job_id
        integrity_request.status = job_status
        integrity_request.save(update_fields=['job_id', 'status', 'updated_at'])
        logger.info(
            f"Request {integrity_request.request_id} → "
            f"status={job_status}, job_id={job_id}"
        )


if __name__ == "__main__":
    process_queue()