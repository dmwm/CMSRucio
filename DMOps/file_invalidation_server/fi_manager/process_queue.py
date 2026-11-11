import os
import sys
import logging
from datetime import timedelta

import django
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'file_invalidation_server.settings')
django.setup()

from django.conf import settings
from django.db.models import Min
from django.utils import timezone
from fi_manager.models import FileInvalidationRequests
from fi_manager.utils import process_invalidation, invalidation_job_running, update_ticket, JiraStatus

logging.basicConfig(level=logging.INFO, format='(%(asctime)s) [%(name)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# 'approved' = approved (manually or automatically) but not started yet.
# 'queued'   = dry run submitted but not started yet.
PENDING_STATUSES = ['approved', 'queued']


def process_queue():
    """
    Starts the oldest pending invalidation request if no invalidation job is running.

    Invalidation jobs share one PVC and fixed intermediate file names, so only one
    may run at a time. Requests that arrive while a job is running stay in a pending
    status and are picked up here (FIFO), one per run. Called by a CronJob every minute.
    """
    if invalidation_job_running():
        logger.info("An invalidation job is running, nothing to start.")
        return

    # Age guard so stale rows left behind by old errors are never picked up unexpectedly
    cutoff = timezone.now() - timedelta(days=settings.INVALIDATION_QUEUE_MAX_AGE_DAYS)
    pending = (FileInvalidationRequests.objects
               .filter(status__in=PENDING_STATUSES, submitted_date__gte=cutoff)
               .values('request_id')
               .annotate(first_submitted=Min('submitted_date'))
               .order_by('first_submitted'))

    if not pending:
        logger.info("No pending invalidation requests.")
        return

    request_id = pending[0]['request_id']
    first = FileInvalidationRequests.objects.filter(request_id=request_id, status__in=PENDING_STATUSES).first()
    logger.info(f"Starting queued request {request_id} (status '{first.status}', requested by {first.request_user}).")

    try:
        message = process_invalidation(
            request_id, first.reason,
            dry_run=first.dry_run, mode=first.mode, rse=first.rse,
            to_process=first.status,
            global_invalidate_last_replicas=first.global_invalidate_last_replicas,
        )
        logger.info(message)
    except Exception as e:
        # Mark as failed so a broken request doesn't block the queue forever
        logger.exception(f"Could not start queued request {request_id}")
        FileInvalidationRequests.objects.filter(request_id=request_id, status__in=PENDING_STATUSES).update(
            status='failed', logs=f"Could not start queued invalidation job: {e}")
        update_ticket(request_id=request_id, new_status=JiraStatus.FAILED.value)


if __name__ == "__main__":
    process_queue()
