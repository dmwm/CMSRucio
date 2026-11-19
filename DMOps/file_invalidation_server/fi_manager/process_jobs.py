import os
import django
import sys
import re
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'file_invalidation_server.settings')
django.setup()

from collections import Counter
from kubernetes import client, config
from fi_manager.models import FileInvalidationRequests
from fi_manager.utils import update_ticket, JiraStatus
import logging

logging.basicConfig(level=logging.INFO,format='(%(asctime)s) [%(name)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

NAMESPACE = 'file-invalidation-tool'
JOB_PREFIX = 'file-invalidation-job-'

# The `logs` column is MySQL TEXT (max 65,535 bytes). A container-level job can
# print >100k per-file lines, so only a condensed log is stored, capped below that.
LOG_DB_MAX_BYTES = 60000

# Per-DID lines printed by the invalidation tool. They are counted, not stored.
PER_ITEM_LINE = re.compile(
    r'(Would declare file|Declared file|Would invalidate file on DBS:|Invalidation OK for file:|'
    r'Would invalidate dataset on DBS:|Invalidation OK for dataset:|Would Delete rule|Deleted rule|'
    r'Would delete did|Deleted did|Would re-stuck Rule|Re-stuck Rule)'
)
# The tool catches these and keeps going (some at INFO level), so the k8s job still
# reports Complete. Any of them means the run did not finish its work.
FATAL_MARKERS = ('Error running shell script', 'Error running invalidation script',
                 'Error running invalidations', 'Traceback (most recent call last)')
# Lines the tool prints once a phase has finished, per request mode.
COMPLETION_MARKERS = {
    'global': ('DBS invalidation was performed correctly', 'Rucio invalidation was performed correctly'),
    'rucio-only': ('Finished Rucio invalidation',),
    'local': ('Finished Rucio invalidation',),
    'dbs-only': ('Finished DBS invalidation',),
}
# Modes in which the tool invalidates DBS datasets and logs one line per container.
DBS_DATASET_MODES = ('global', 'dbs-only')

LFN = r'(/store/\S+?\.root)'
DBS_DATASET = r'(/[^\s/#]+/[^\s/#]+/[^\s/#]+)\s*$'
LINE_PATTERNS = {
    True: {
        'rucio_files': rf'Would declare file {LFN} as bad at',
        'dbs_files': rf'Would invalidate file on DBS: {LFN}\s*$',
        'dbs_datasets': rf'Would invalidate dataset on DBS: {DBS_DATASET}',
    },
    False: {
        'rucio_files': rf'Declared file {LFN} as bad at',
        'dbs_files': rf'Invalidation OK for file: {LFN}\s*$',
        'dbs_datasets': rf'Invalidation OK for dataset: {DBS_DATASET}',
    },
}


def job_is_dry_run(job):
    """Read dry-run from the job spec, not the logs: a dry run whose DIDs have no
    Rucio replicas prints no 'Would declare file' line at all."""
    args = job.spec.template.spec.containers[0].args or []
    return '--dry-run' in args


def did_level(name):
    if name.endswith('.root') or name.startswith('/store/'):
        return 'file'
    if '#' in name:
        return 'dataset'
    return 'container'


def parse_job_logs(logs: str, dry_run: bool):
    patterns = LINE_PATTERNS[dry_run]
    parsed = {key: set(re.findall(pattern, logs, flags=re.MULTILINE)) for key, pattern in patterns.items()}
    lines = logs.splitlines()
    parsed['error_lines'] = [line for line in lines
                             if line.startswith('ERROR') or any(m in line for m in FATAL_MARKERS)]
    parsed['fatal'] = any(m in logs for m in FATAL_MARKERS)
    parsed['logs'] = logs
    return parsed


def condense_logs(logs: str, max_bytes: int = LOG_DB_MAX_BYTES):
    """Drop per-DID lines (replacing them with counts) and cap the size for the TEXT column."""
    counts = Counter()
    kept = []
    for line in logs.splitlines():
        match = PER_ITEM_LINE.search(line)
        if match:
            counts[match.group(1).rstrip(':')] += 1
        else:
            kept.append(line)
    header = [f'[log-processor] {n} x "{kind}" lines omitted' for kind, n in sorted(counts.items())]
    text = '\n'.join(header + kept)
    encoded = text.encode('utf-8')
    if len(encoded) <= max_bytes:
        return text
    marker = '\n[log-processor] ... truncated ...\n'
    half = (max_bytes - len(marker)) // 2
    return (encoded[:half].decode('utf-8', 'ignore') + marker
            + encoded[-half:].decode('utf-8', 'ignore'))


def resolve_row(name, request_mode, parsed):
    """Return (status, mode_label or None, note) for one request row."""
    if parsed['fatal']:
        return 'failed', None, 'the invalidation tool reported a fatal error'
    if any(name in line for line in parsed['error_lines']):
        return 'failed', None, f'error reported for {name}'

    if did_level(name) == 'file':
        in_rucio = name in parsed['rucio_files']
        in_dbs = name in parsed['dbs_files']
        if in_rucio and in_dbs:
            return 'success', 'global', None
        if in_rucio:
            return 'success', 'rucio_only', None
        if in_dbs:
            return 'success', 'dbs_only', None
        return 'failed', None, f'{name} not found in the job output'

    # Container- and dataset-level requests: the tool logs files, not the requested
    # DID, so success is inferred from the run finishing cleanly.
    markers = COMPLETION_MARKERS.get(request_mode, ())
    if not markers or not all(m in parsed['logs'] for m in markers):
        return 'failed', None, f'the {request_mode} run did not reach its completion marker'
    if parsed['error_lines']:
        return 'failed', None, 'the job logged errors that cannot be attributed to a single DID; review the log'
    if (did_level(name) == 'container' and request_mode in DBS_DATASET_MODES
            and name not in parsed['dbs_datasets']):
        return 'failed', None, f'{name} was not invalidated in DBS'
    return 'success', None, None


def update_database(job_id, parsed):
    rows = list(FileInvalidationRequests.objects.filter(job_id=job_id, status='in_progress'))
    if not rows:
        logger.info(f"No in_progress rows for job {job_id}; nothing to update.")
        return None

    condensed = condense_logs(parsed['logs'])
    outcome = Counter()
    notes = Counter()
    for row in rows:
        status, mode_label, note = resolve_row(row.file_name, row.mode, parsed)
        row.status = status
        if mode_label:
            row.mode = mode_label
        row.logs = condensed
        outcome[status] += 1
        if note:
            notes[note if did_level(row.file_name) != 'file' else 'per-file failure'] += 1
    FileInvalidationRequests.objects.bulk_update(rows, ['status', 'mode', 'logs'], batch_size=200)

    logger.info(f"Job {job_id}: {dict(outcome)}. Failure reasons: {dict(notes) or 'none'}")
    request_id = rows[0].request_id
    final = JiraStatus.DONE if outcome['failed'] == 0 else JiraStatus.FAILED
    update_ticket(request_id=request_id, new_status=final.value)
    return outcome


def update_database_for_failed_job(job_id, logs):
    rows = FileInvalidationRequests.objects.filter(job_id=job_id, status='in_progress')
    if not rows.exists():
        return 0
    request_id = rows.first().request_id
    updated = rows.update(status='failed', logs=condense_logs(logs))
    update_ticket(request_id=request_id, new_status=JiraStatus.FAILED.value)
    return updated


def latest_pod_logs(core_v1, job_name):
    pods = core_v1.list_namespaced_pod(namespace=NAMESPACE, label_selector=f"job-name={job_name}")
    if not pods.items:
        return None
    latest_pod = sorted(pods.items,
                        key=lambda pod: pod.status.start_time or pod.metadata.creation_timestamp,
                        reverse=True)[0]
    logger.info(f"Pod name: {latest_pod.metadata.name}")
    return core_v1.read_namespaced_pod_log(latest_pod.metadata.name, namespace=NAMESPACE)


def process_job(job, batch_v1, core_v1):
    job_name = job.metadata.name
    job_id = job_name[len(JOB_PREFIX):]
    conditions = {cond.type: cond.status for cond in job.status.conditions}

    if conditions.get("Failed") == "True":
        # Keep the k8s job for debugging; only record the failure once.
        try:
            logs = latest_pod_logs(core_v1, job_name) or ''
        except client.exceptions.ApiException as e:
            logs = f'Could not read pod logs: {e}'
        if update_database_for_failed_job(job_id, f'Kubernetes job {job_name} failed.\n{logs}'):
            logger.warning(f"Job {job_name} failed; request rows marked failed.")
        return

    if conditions.get("Complete") != "True":
        return

    logs = latest_pod_logs(core_v1, job_name)
    if logs is None:
        logger.error(f"There are no pods under the {job_name} job name.")
        return

    parsed = parse_job_logs(logs, dry_run=job_is_dry_run(job))
    logger.info(f"Job {job_name}: {len(parsed['rucio_files'])} files in Rucio, {len(parsed['dbs_files'])} files "
                f"and {len(parsed['dbs_datasets'])} datasets in DBS, {len(parsed['error_lines'])} error lines.")
    update_database(job_id, parsed)

    # Only delete once the outcome is stored; if anything above raised, the job is
    # left in place and retried on the next run.
    batch_v1.delete_namespaced_job(name=job_name, namespace=NAMESPACE,
                                   body=client.V1DeleteOptions(propagation_policy='Foreground'))


def fetch_and_process():
    config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()

    jobs = batch_v1.list_namespaced_job(namespace=NAMESPACE)
    for job in jobs.items:
        if not job.status.conditions or not job.metadata.name.startswith(JOB_PREFIX):
            continue
        try:
            process_job(job, batch_v1, core_v1)
        except Exception:
            logger.exception(f"Failed to process job {job.metadata.name}; it will be retried on the next run.")


if __name__ == "__main__":
    fetch_and_process()
