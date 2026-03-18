import uuid
import logging
from .models import FileIntegrityRequest, FileReplica

logger = logging.getLogger(__name__)

MAX_LFNS_PER_REQUEST = 20


def process_integrity_check(integrity_request, raw_lfns):
    if not raw_lfns:
        raise ValueError("No LFNs provided.")

    if len(raw_lfns) > MAX_LFNS_PER_REQUEST:
        raise ValueError(
            f"Too many LFNs: {len(raw_lfns)} provided, "
            f"maximum is {MAX_LFNS_PER_REQUEST}."
        )

    for lfn in raw_lfns:
        scope, bare_lfn = split_scope(lfn)
        FileReplica.objects.create(
            request=integrity_request,
            scope=scope,
            lfn=bare_lfn,
            status='pending'
        )

    logger.info(
        f"Created {len(raw_lfns)} FileReplica rows "
        f"for request {integrity_request.request_id}"
    )

    job_id, job_status = trigger_job(integrity_request)

    integrity_request.job_id = job_id
    integrity_request.status = job_status
    integrity_request.save(update_fields=['job_id', 'status'])

    logger.info(
        f"Request {integrity_request.request_id} → "
        f"status={job_status}, job_id={job_id}"
    )

    return job_id, job_status


def split_scope(lfn):
    if ':' in lfn:
        scope, bare = lfn.split(':', 1)
        return scope.strip(), bare.strip()
    return 'cms', lfn.strip()


def trigger_job(integrity_request):
    lfns = [
        f"{r.scope}:{r.lfn}"
        for r in integrity_request.replicas.filter(status='pending')
    ]

    job_id = uuid.uuid4().hex[:8]

    logger.info(
        f"[STUB] Would trigger job {job_id} for request "
        f"{integrity_request.request_id} by {integrity_request.requested_by} for "
        f"{len(lfns)} LFNs with rse_expression={integrity_request.rse_expression} and "
        f"full_scan={integrity_request.full_scan}"
    )

    return job_id, FileIntegrityRequest.Status.IN_PROGRESS