from django.test import TestCase
from .models import FileIntegrityRequest, FileReplica
from .tasks import process_integrity_check, split_scope, MAX_LFNS_PER_REQUEST

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
        """Creates a fresh request before each test."""
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

    def test_request_status_updated_to_in_progress(self):
        process_integrity_check(self.request, ['cms:/store/data/file.root'])
        self.request.refresh_from_db()
        self.assertEqual(self.request.status, FileIntegrityRequest.Status.IN_PROGRESS)

    def test_job_id_set_on_request(self):
        process_integrity_check(self.request, ['cms:/store/data/file.root'])
        self.request.refresh_from_db()
        self.assertIsNotNone(self.request.job_id)
        self.assertEqual(len(self.request.job_id), 8)

    def test_too_many_lfns_raises(self):
        lfns = [f'/store/data/file{i}.root' for i in range(MAX_LFNS_PER_REQUEST + 1)]
        with self.assertRaises(ValueError):
            process_integrity_check(self.request, lfns)

    def test_empty_lfns_raises(self):
        with self.assertRaises(ValueError):
            process_integrity_check(self.request, [])