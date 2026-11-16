import uuid
from datetime import timedelta
from unittest.mock import patch, mock_open

from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework.test import APIClient

from fi_manager.models import FileInvalidationRequests
from fi_manager.utils import process_invalidation
from fi_manager.process_queue import process_queue


def make_request(n_files=2, status='approved', dry_run=False, submitted_date=None, user='someone'):
    request_id = uuid.uuid4()
    rows = [FileInvalidationRequests(request_id=request_id, file_name=f'/store/test/{request_id.hex}_{i}.root',
                                     status=status, mode='global', dry_run=dry_run, reason='test',
                                     request_user=user) for i in range(n_files)]
    FileInvalidationRequests.objects.bulk_create(rows)
    if submitted_date is not None:
        FileInvalidationRequests.objects.filter(request_id=request_id).update(submitted_date=submitted_date)
    return request_id


@override_settings(LOCAL_TESTING=False)
@patch('fi_manager.utils.update_ticket')
@patch('fi_manager.utils.os.listdir', return_value=[])
@patch('builtins.open', new_callable=mock_open)
@patch('fi_manager.utils.create_job_from_yaml', return_value='in_progress')
class ProcessInvalidationSingleJobTests(TestCase):

    @patch('fi_manager.utils.invalidation_job_running', return_value=True)
    def test_does_not_start_job_while_another_is_running(self, _running, create_job, *_):
        request_id = make_request()
        message = process_invalidation(request_id, 'test', dry_run=False, to_process='approved')

        create_job.assert_not_called()
        self.assertIn('queued', message)
        statuses = set(FileInvalidationRequests.objects.filter(request_id=request_id).values_list('status', flat=True))
        self.assertEqual(statuses, {'approved'})

    @patch('fi_manager.utils.invalidation_job_running', return_value=False)
    def test_starts_job_when_idle(self, _running, create_job, *_):
        request_id = make_request()
        process_invalidation(request_id, 'test', dry_run=False, to_process='approved')

        create_job.assert_called_once()
        statuses = set(FileInvalidationRequests.objects.filter(request_id=request_id).values_list('status', flat=True))
        self.assertEqual(statuses, {'in_progress'})


@override_settings(LOCAL_TESTING=False)
@patch('fi_manager.process_queue.process_invalidation')
class ProcessQueueTests(TestCase):

    @patch('fi_manager.process_queue.invalidation_job_running', return_value=True)
    def test_nothing_started_while_job_running(self, _running, process):
        make_request()
        process_queue()
        process.assert_not_called()

    @patch('fi_manager.process_queue.invalidation_job_running', return_value=False)
    def test_starts_oldest_pending_request_only(self, _running, process):
        now = timezone.now()
        newer = make_request(submitted_date=now - timedelta(minutes=1))
        older = make_request(status='queued', dry_run=True, submitted_date=now - timedelta(minutes=10))
        make_request(status='waiting_approval', submitted_date=now - timedelta(hours=1))

        process_queue()

        process.assert_called_once()
        self.assertEqual(process.call_args.args[0], older)
        self.assertEqual(process.call_args.kwargs['to_process'], 'queued')
        self.assertTrue(process.call_args.kwargs['dry_run'])

    @override_settings(INVALIDATION_QUEUE_MAX_AGE_DAYS=7)
    @patch('fi_manager.process_queue.invalidation_job_running', return_value=False)
    def test_ignores_stale_pending_requests(self, _running, process):
        make_request(submitted_date=timezone.now() - timedelta(days=30))
        process_queue()
        process.assert_not_called()

    @patch('fi_manager.process_queue.update_ticket')
    @patch('fi_manager.process_queue.invalidation_job_running', return_value=False)
    def test_failure_to_start_marks_request_failed(self, _running, _ticket, process):
        process.side_effect = RuntimeError('boom')
        request_id = make_request()

        process_queue()

        statuses = set(FileInvalidationRequests.objects.filter(request_id=request_id).values_list('status', flat=True))
        self.assertEqual(statuses, {'failed'})


@override_settings(AUTO_APPROVE_USERS=['cmsunified'], AUTO_APPROVE_MAX_ENTRIES=3,
                   AUTO_APPROVE_REASON_MARKER='Automatic McM invalidation')
@patch('fi_manager.views.send_approval_alert')
@patch('fi_manager.views.create_ticket_for_invalidation')
@patch('fi_manager.views.process_invalidation', return_value='started')
class AutoApproveTests(TestCase):

    MCM_REASON = 'Automatic McM invalidation of request XYZ'

    def submit(self, user, n_files, reason=MCM_REASON):
        client = APIClient()
        files = '\n'.join(f'/store/test/{uuid.uuid4().hex}.root' for _ in range(n_files))
        return client.post('/api/upload/', {'reason': reason, 'file_content': files, 'mode': 'global'},
                           format='json', HTTP_X_FORWARDED_PREFERRED_USERNAME=user, HTTP_ACCEPT='application/json')

    def statuses(self, response):
        return set(FileInvalidationRequests.objects.filter(request_id=response.data['request_id'])
                   .values_list('status', flat=True))

    def test_unified_under_threshold_is_auto_approved(self, process, ticket, _alert):
        response = self.submit('cmsunified', 2)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.statuses(response), {'approved'})
        self.assertEqual(set(FileInvalidationRequests.objects.values_list('approve_user', flat=True)), {'auto-approved'})
        ticket.assert_called_once()
        process.assert_called_once()
        self.assertEqual(process.call_args.kwargs['to_process'], 'approved')

    def test_unified_at_threshold_needs_approval(self, process, *_):
        response = self.submit('cmsunified', 3)
        self.assertEqual(self.statuses(response), {'waiting_approval'})
        process.assert_not_called()

    def test_other_user_needs_approval(self, process, *_):
        response = self.submit('someone', 1)
        self.assertEqual(self.statuses(response), {'waiting_approval'})
        process.assert_not_called()

    def test_unified_without_mcm_reason_needs_approval(self, process, *_):
        response = self.submit('cmsunified', 1, reason='Manual invalidation, bad files')
        self.assertEqual(self.statuses(response), {'waiting_approval'})
        process.assert_not_called()


# ---------------------------------------------------------------------------
# Log processor (fi_manager/process_jobs.py)
# ---------------------------------------------------------------------------
from types import SimpleNamespace
from unittest.mock import MagicMock

from fi_manager import process_jobs

CONT_A = '/PrimA/Era22-v1/MINIAODSIM'
CONT_B = '/PrimB/Era22-v1/NANOAODSIM'
FILE_A = '/store/mc/Era22/PrimA/MINIAODSIM/v1/2530000/aaaa.root'
FILE_B = '/store/mc/Era22/PrimA/MINIAODSIM/v1/2530000/bbbb.root'
TS = 'INFO 2026-11-16 19:02:29 - '


def tool_log(*body):
    return '\n'.join(TS + line for line in body) + '\n'


def global_container_log(dry_run=True, containers=(CONT_A, CONT_B), files=(FILE_A, FILE_B), extra=()):
    w = dry_run
    return tool_log(
        'Submitted lists correctly', 'Starting DBS invalidation',
        *[(f'Would invalidate file on DBS: {f}' if w else f'Invalidation OK for file: {f}') for f in files],
        *[(f'Would invalidate dataset on DBS: {c}' if w else f'Invalidation OK for dataset: {c}') for c in containers],
        'Finished DBS invalidation', 'DBS invalidation was performed correctly', 'Starting Rucio invalidation',
        *[(f"Would declare file {f} as bad at ['T1_IT_CNAF_Tape']" if w else f"Declared file {f} as bad at ['T1_IT_CNAF_Tape']") for f in files],
        *extra,
        'Finished Rucio invalidation', 'Rucio invalidation was performed correctly')


def make_rows(names, dry_run=True, mode='global', job_id='abcd1234', status='in_progress'):
    request_id = uuid.uuid4()
    FileInvalidationRequests.objects.bulk_create([
        FileInvalidationRequests(request_id=request_id, file_name=n, status=status, mode=mode, dry_run=dry_run,
                                 reason='test', request_user='someone', job_id=job_id) for n in names])
    return request_id


def rows_by_name(job_id='abcd1234'):
    return {r.file_name: r for r in FileInvalidationRequests.objects.filter(job_id=job_id)}


def fake_job(status, dry_run=True, job_id='abcd1234'):
    args = ['global', '--reason', '$(REASON)', '--rucio-mode'] + (['--dry-run'] if dry_run else [])
    return SimpleNamespace(
        metadata=SimpleNamespace(name=f'file-invalidation-job-{job_id}'),
        status=SimpleNamespace(conditions=[SimpleNamespace(type=status, status='True')]),
        spec=SimpleNamespace(template=SimpleNamespace(spec=SimpleNamespace(containers=[SimpleNamespace(args=args)]))))


class CondenseLogsTests(TestCase):

    def test_per_item_lines_are_counted_not_stored(self):
        text = process_jobs.condense_logs(global_container_log())
        self.assertNotIn(FILE_A, text)
        self.assertIn('2 x "Would declare file" lines omitted', text)
        self.assertIn('2 x "Would invalidate dataset on DBS" lines omitted', text)
        self.assertIn('Rucio invalidation was performed correctly', text)

    def test_size_is_capped_for_the_text_column(self):
        noise = tool_log(*[f'some unrelated line {i} ' + 'x' * 100 for i in range(5000)])
        text = process_jobs.condense_logs(noise, max_bytes=10000)
        self.assertLessEqual(len(text.encode('utf-8')), 10000)
        self.assertIn('truncated', text)


@patch('fi_manager.process_jobs.update_ticket')
class LogProcessorUpdateTests(TestCase):

    def process(self, logs, dry_run=True):
        return process_jobs.update_database('abcd1234', process_jobs.parse_job_logs(logs, dry_run=dry_run))

    def test_container_dry_run_is_marked_success_and_keeps_dry_run_flag(self, ticket):
        make_rows([CONT_A, CONT_B], dry_run=True)
        self.process(global_container_log(dry_run=True))
        rows = rows_by_name()
        self.assertEqual({r.status for r in rows.values()}, {'success'})
        self.assertTrue(all(r.dry_run for r in rows.values()))
        self.assertIn('Rucio invalidation was performed correctly', rows[CONT_A].logs)
        self.assertEqual(ticket.call_args.kwargs['new_status'], process_jobs.JiraStatus.DONE.value)

    def test_container_real_run_is_marked_success(self, _ticket):
        make_rows([CONT_A, CONT_B], dry_run=False)
        self.process(global_container_log(dry_run=False), dry_run=False)
        self.assertEqual({r.status for r in rows_by_name().values()}, {'success'})

    def test_dry_run_without_rucio_replicas_is_not_mistaken_for_real_run(self, _ticket):
        # Containers with no replicas print no "Would declare file" line at all.
        make_rows([CONT_A], dry_run=True)
        self.process(global_container_log(dry_run=True, containers=(CONT_A,), files=()))
        row = rows_by_name()[CONT_A]
        self.assertEqual(row.status, 'success')
        self.assertTrue(row.dry_run)

    def test_container_missing_from_dbs_output_fails(self, ticket):
        make_rows([CONT_A, CONT_B])
        self.process(global_container_log(containers=(CONT_A,)))
        rows = rows_by_name()
        self.assertEqual(rows[CONT_A].status, 'success')
        self.assertEqual(rows[CONT_B].status, 'failed')
        self.assertEqual(ticket.call_args.kwargs['new_status'], process_jobs.JiraStatus.FAILED.value)

    def test_error_caught_by_the_tool_fails_the_request(self, _ticket):
        # The tool logs this at INFO and exits 0, so the k8s job reports Complete.
        make_rows([CONT_A])
        logs = tool_log('Submitted lists correctly', 'Starting DBS invalidation',
                        'Error running invalidation script:', 'boom')
        self.process(logs)
        self.assertEqual(rows_by_name()[CONT_A].status, 'failed')

    def test_unattributable_error_lines_fail_container_rows(self, _ticket):
        make_rows([CONT_A, CONT_B])
        err = f"ERROR 2026-11-16 19:02:29 - Error declaring file {FILE_A} as bad at ['X']: timeout"
        self.process(global_container_log() + err + '\n')
        self.assertEqual({r.status for r in rows_by_name().values()}, {'failed'})

    def test_rucio_only_container_succeeds_on_completion_marker(self, _ticket):
        make_rows([CONT_A], mode='rucio-only')
        logs = tool_log('Starting Rucio invalidation', f"Would declare file {FILE_A} as bad at ['X']",
                        'Finished Rucio invalidation')
        self.process(logs)
        self.assertEqual(rows_by_name()[CONT_A].status, 'success')

    def test_file_level_rows_are_matched_individually(self, _ticket):
        make_rows([FILE_A, FILE_B])
        logs = tool_log(f'Would invalidate file on DBS: {FILE_A}', f"Would declare file {FILE_A} as bad at ['X']",
                        'DBS invalidation was performed correctly', 'Rucio invalidation was performed correctly')
        self.process(logs)
        rows = rows_by_name()
        self.assertEqual((rows[FILE_A].status, rows[FILE_A].mode), ('success', 'global'))
        self.assertEqual(rows[FILE_B].status, 'failed')

    def test_file_named_in_an_error_line_fails(self, _ticket):
        make_rows([FILE_A])
        logs = tool_log(f'Would invalidate file on DBS: {FILE_A}') + \
            f'ERROR 2026-11-16 19:02:29 - Error declaring file {FILE_A} as bad at [\'X\']: timeout\n'
        self.process(logs)
        self.assertEqual(rows_by_name()[FILE_A].status, 'failed')

    def test_rows_already_resolved_are_left_alone(self, ticket):
        make_rows([CONT_A], status='success')
        self.assertIsNone(self.process(global_container_log()))
        ticket.assert_not_called()


@patch('fi_manager.process_jobs.update_ticket')
class LogProcessorJobHandlingTests(TestCase):

    def clients(self, logs):
        core = MagicMock()
        core.list_namespaced_pod.return_value.items = [SimpleNamespace(
            metadata=SimpleNamespace(name='pod-1', creation_timestamp=1), status=SimpleNamespace(start_time=1))]
        core.read_namespaced_pod_log.return_value = logs
        return MagicMock(), core

    def test_completed_job_is_recorded_then_deleted(self, _ticket):
        make_rows([CONT_A, CONT_B])
        batch, core = self.clients(global_container_log())
        process_jobs.process_job(fake_job('Complete'), batch, core)
        self.assertEqual({r.status for r in rows_by_name().values()}, {'success'})
        batch.delete_namespaced_job.assert_called_once()

    def test_job_is_kept_if_recording_fails(self, _ticket):
        make_rows([CONT_A])
        batch, core = self.clients(global_container_log())
        with patch('fi_manager.process_jobs.update_database', side_effect=RuntimeError('db down')):
            with self.assertRaises(RuntimeError):
                process_jobs.process_job(fake_job('Complete'), batch, core)
        batch.delete_namespaced_job.assert_not_called()

    def test_dry_run_is_read_from_job_args(self, _ticket):
        self.assertTrue(process_jobs.job_is_dry_run(fake_job('Complete', dry_run=True)))
        self.assertFalse(process_jobs.job_is_dry_run(fake_job('Complete', dry_run=False)))

    def test_failed_job_marks_rows_failed_and_keeps_job(self, ticket):
        make_rows([CONT_A])
        batch, core = self.clients(tool_log('Traceback (most recent call last):', 'OOMKilled'))
        process_jobs.process_job(fake_job('Failed'), batch, core)
        row = rows_by_name()[CONT_A]
        self.assertEqual(row.status, 'failed')
        self.assertIn('Kubernetes job file-invalidation-job-abcd1234 failed', row.logs)
        batch.delete_namespaced_job.assert_not_called()
        ticket.assert_called_once()
        # A second pass over the same failed job must not touch the rows or Jira again.
        process_jobs.process_job(fake_job('Failed'), batch, core)
        ticket.assert_called_once()
