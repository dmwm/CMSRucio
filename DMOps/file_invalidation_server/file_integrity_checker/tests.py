from unittest.mock import patch
from django.test import TestCase
from .models import FileIntegrityRequest, FileReplica
from .tasks import process_integrity_check, split_scope, FileIntegrityRequest


class SplitScopeTest(TestCase):

    def test_with_scope(self):
        self.assertEqual(
            split_scope('cms:/store/data/file.root'),
            ('cms', '/store/data/file.root')
        )

    def test_without_scope(self):
        self.assertEqual(
            split_scope('/store/data/file.root'),
            ('cms', '/store/data/file.root')
        )

    def test_non_cms_scope(self):
        self.assertEqual(
            split_scope('T2_CH_CERN:/store/file.root'),
            ('T2_CH_CERN', '/store/file.root')
        )


class ProcessIntegrityCheckTest(TestCase):

    def setUp(self):
        self.request = FileIntegrityRequest.objects.create(
            requested_by='testuser',
            rse_expression=None,
            full_scan=False,
            status=FileIntegrityRequest.Status.SUBMITTED
        )

    def test_creates_replica_rows(self):
        raw_lfns = [
            'cms:/store/data/Run2024/file1.root',
            '/store/data/Run2024/file2.root',
        ]
        process_integrity_check(self.request, raw_lfns)
        self.assertEqual(self.request.replicas.count(), 2)

    def test_replica_scope_defaults_to_cms(self):
        process_integrity_check(self.request, ['/store/data/file.root'])
        replica = self.request.replicas.first()
        self.assertEqual(replica.scope, 'cms')
        self.assertEqual(replica.lfn, '/store/data/file.root')

    def test_replica_initial_status_is_pending(self):
        process_integrity_check(self.request, ['cms:/store/data/file.root'])
        replica = self.request.replicas.first()
        self.assertEqual(replica.status, 'pending')

    def test_request_status_updated_after_trigger(self):
        # Locally K8s is not available so trigger_job returns FAILED.
        # What we test here is that the status is always updated to
        # whatever trigger_job returns — never left as SUBMITTED.
        process_integrity_check(self.request, ['cms:/store/data/file.root'])
        self.request.refresh_from_db()
        self.assertNotEqual(
            self.request.status,
            FileIntegrityRequest.Status.SUBMITTED
        )

    def test_job_id_always_set(self):
        # job_id is generated before K8s submission so it is always
        # set regardless of whether the job creation succeeds or fails
        process_integrity_check(self.request, ['cms:/store/data/file.root'])
        self.request.refresh_from_db()
        self.assertIsNotNone(self.request.job_id)
        self.assertEqual(len(self.request.job_id), 8)

    def test_too_many_lfns_raises(self):
        lfns = [f'/store/data/file{i}.root' for i in range(21)]
        with self.assertRaises(ValueError):
            process_integrity_check(self.request, lfns)

    def test_empty_lfns_raises(self):
        with self.assertRaises(ValueError):
            process_integrity_check(self.request, [])
            

class TriggerJobArgsTest(TestCase):

    def setUp(self):
        self.request_with_rse = FileIntegrityRequest.objects.create(
            requested_by='testuser',
            rse_expression='T2_CH_CERN',
            full_scan=False,
            status=FileIntegrityRequest.Status.SUBMITTED
        )
        self.request_full_scan = FileIntegrityRequest.objects.create(
            requested_by='testuser',
            rse_expression=None,
            full_scan=True,
            status=FileIntegrityRequest.Status.SUBMITTED
        )

    @patch('file_integrity_checker.tasks.trigger_job')
    def test_rse_expression_stored_on_request(self, mock_trigger):
        mock_trigger.return_value = ('abc12345', FileIntegrityRequest.Status.IN_PROGRESS)
        process_integrity_check(self.request_with_rse, ['cms:/store/data/file.root'])
        self.request_with_rse.refresh_from_db()
        self.assertEqual(self.request_with_rse.rse_expression, 'T2_CH_CERN')

    @patch('file_integrity_checker.tasks.trigger_job')
    def test_status_is_in_progress_when_job_succeeds(self, mock_trigger):
        # By mocking trigger_job we can test the IN_PROGRESS path
        # that is unreachable locally without K8s
        mock_trigger.return_value = ('abc12345', FileIntegrityRequest.Status.IN_PROGRESS)
        process_integrity_check(self.request_with_rse, ['cms:/store/data/file.root'])
        self.request_with_rse.refresh_from_db()
        self.assertEqual(
            self.request_with_rse.status,
            FileIntegrityRequest.Status.IN_PROGRESS
        )

    @patch('file_integrity_checker.tasks.trigger_job')
    def test_full_scan_stored_on_request(self, mock_trigger):
        mock_trigger.return_value = ('abc12345', FileIntegrityRequest.Status.IN_PROGRESS)
        process_integrity_check(self.request_full_scan, ['cms:/store/data/file.root'])
        self.request_full_scan.refresh_from_db()
        self.assertTrue(self.request_full_scan.full_scan)