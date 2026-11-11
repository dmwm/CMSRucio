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


@override_settings(AUTO_APPROVE_USERS=['cmsunified'], AUTO_APPROVE_MAX_ENTRIES=3)
@patch('fi_manager.views.send_approval_alert')
@patch('fi_manager.views.create_ticket_for_invalidation')
@patch('fi_manager.views.process_invalidation', return_value='started')
class AutoApproveTests(TestCase):

    def submit(self, user, n_files):
        client = APIClient()
        files = '\n'.join(f'/store/test/{uuid.uuid4().hex}.root' for _ in range(n_files))
        return client.post('/api/upload/', {'reason': 'test', 'file_content': files, 'mode': 'global'},
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
