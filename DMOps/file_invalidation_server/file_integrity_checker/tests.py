import json
import uuid
from unittest.mock import patch
from django.test import TestCase, Client
from django.urls import reverse
from .models import FileIntegrityRequest, FileReplica
from .tasks import process_integrity_check, split_scope, FileIntegrityRequest
from .views import derive_file_status, build_request_summary, _format_lfns_per_rse
from file_integrity_checker.process_jobs import parse_tool_output, update_replicas


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


class ParseToolOutputTest(TestCase):

    def test_parses_json_from_logs_with_preceding_output(self):
        # Mirrors real pod logs — JSON on last line, logging lines before it
        logs = (
            "INFO:file_ops:Copying file...\n"
            "INFO:check_decompression:Integrity check PASSED\n"
            '[{"filename": "cms:/store/data/file.root", "replicas": '
            '[{"rse": "T1_US_FNAL_Disk", "pfn": "davs://...", "status": "OK"}]}]'
        )
        results = parse_tool_output(logs)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["filename"], "cms:/store/data/file.root")
        self.assertEqual(results[0]["replicas"][0]["status"], "OK")

    def test_raises_if_no_json_line(self):
        logs = "INFO:something\nINFO:something else\n"
        with self.assertRaises(ValueError):
            parse_tool_output(logs)

    def test_raises_if_not_a_list(self):
        logs = 'INFO:something\n{"filename": "cms:/store/data/file.root"}'
        with self.assertRaises(ValueError):
            parse_tool_output(logs)


class UpdateReplicasTest(TestCase):

    def setUp(self):
        self.integrity_request = FileIntegrityRequest.objects.create(
            requested_by='testuser',
            rse_expression=None,
            full_scan=False,
            status=FileIntegrityRequest.Status.IN_PROGRESS
        )
        # Create placeholder rows as trigger_job would have done
        FileReplica.objects.create(
            request=self.integrity_request,
            scope='cms',
            lfn='/store/mc/RunIISummer16NanoAODv3/file1.root',
            status='pending'
        )
        FileReplica.objects.create(
            request=self.integrity_request,
            scope='cms',
            lfn='/store/mc/RunIISummer16NanoAODv5/file2.root',
            status='pending'
        )

    def _real_results(self):
        # Extracted directly from your real test run output
        return [
            {
                "filename": "cms:/store/mc/RunIISummer16NanoAODv3/file1.root",
                "replicas": [
                    {
                        "rse": "T1_US_FNAL_Disk",
                        "pfn": "davs://cmsdcadisk.fnal.gov:2880/file1.root",
                        "status": "OK"
                    },
                    {
                        "rse": "T0_CH_CERN_Tape",
                        "pfn": "davs://eosctacms.cern.ch:8444/file1.root",
                        "status": "ERROR"
                    }
                ]
            },
            {
                "filename": "/store/mc/RunIISummer16NanoAODv5/file2.root",
                "replicas": [
                    {
                        "rse": "T1_US_FNAL_Disk",
                        "pfn": "davs://cmsdcadisk.fnal.gov:2880/file2.root",
                        "status": "OK"
                    },
                    {
                        "rse": "T1_FR_CCIN2P3_Disk",
                        "pfn": "davs://ccdavcms.in2p3.fr:2880/file2.root",
                        "status": "OK"
                    }
                ]
            }
        ]

    def test_placeholder_rows_deleted(self):
        update_replicas(self.integrity_request, self._real_results())
        pending = FileReplica.objects.filter(
            request=self.integrity_request,
            status='pending'
        )
        self.assertEqual(pending.count(), 0)

    def test_correct_number_of_replica_rows_created(self):
        # 2 replicas for file1 + 2 replicas for file2 = 4 total
        update_replicas(self.integrity_request, self._real_results())
        self.assertEqual(
            FileReplica.objects.filter(request=self.integrity_request).count(),
            4
        )

    def test_replica_statuses_correct(self):
        update_replicas(self.integrity_request, self._real_results())
        ok_count = FileReplica.objects.filter(
            request=self.integrity_request,
            status='OK'
        ).count()
        error_count = FileReplica.objects.filter(
            request=self.integrity_request,
            status='ERROR'
        ).count()
        self.assertEqual(ok_count, 3)
        self.assertEqual(error_count, 1)

    def test_scope_defaults_to_cms_for_lfn_without_scope(self):
        # file2 has no scope prefix in filename — should default to cms
        update_replicas(self.integrity_request, self._real_results())
        replicas_file2 = FileReplica.objects.filter(
            request=self.integrity_request,
            lfn='/store/mc/RunIISummer16NanoAODv5/file2.root'
        )
        for r in replicas_file2:
            self.assertEqual(r.scope, 'cms')

    def test_idempotent_when_called_twice(self):
        # Simulates process_jobs.py running twice before job is deleted
        update_replicas(self.integrity_request, self._real_results())
        update_replicas(self.integrity_request, self._real_results())
        # Should still be 4 rows, not 8
        self.assertEqual(
            FileReplica.objects.filter(request=self.integrity_request).count(),
            4
        )

    def test_rucio_level_error_creates_single_error_row(self):
        results_with_error = [
            {
                "filename": "cms:/store/mc/RunIISummer16NanoAODv3/file1.root",
                "error": "No replicas found in Rucio for this LFN",
                "replicas": []
            }
        ]
        update_replicas(self.integrity_request, results_with_error)
        replicas = FileReplica.objects.filter(
            request=self.integrity_request,
            lfn='/store/mc/RunIISummer16NanoAODv3/file1.root'
        )
        self.assertEqual(replicas.count(), 1)
        self.assertEqual(replicas.first().status, 'ERROR')
        self.assertIsNone(replicas.first().rse)

    def test_pfn_stored_correctly(self):
        update_replicas(self.integrity_request, self._real_results())
        replica = FileReplica.objects.get(
            request=self.integrity_request,
            rse='T1_US_FNAL_Disk',
            lfn='/store/mc/RunIISummer16NanoAODv3/file1.root'
        )
        self.assertEqual(
            replica.pfn,
            'davs://cmsdcadisk.fnal.gov:2880/file1.root'
        )


class DeriveFileStatusTest(TestCase):

    def test_all_corrupted(self):
        self.assertEqual(
            derive_file_status(['CORRUPTED', 'CORRUPTED']),
            'FULLY_CORRUPTED'
        )

    def test_some_corrupted(self):
        self.assertEqual(
            derive_file_status(['CORRUPTED', 'OK']),
            'PARTIALLY_CORRUPTED'
        )

    def test_all_ok(self):
        self.assertEqual(
            derive_file_status(['OK', 'OK']),
            'FULLY_OK'
        )

    def test_some_ok(self):
        self.assertEqual(
            derive_file_status(['OK', 'ERROR']),
            'OK'
        )

    def test_pending(self):
        self.assertEqual(
            derive_file_status(['pending', 'pending']),
            'PENDING'
        )

    def test_all_error(self):
        self.assertEqual(
            derive_file_status(['ERROR', 'ERROR']),
            'ERROR'
        )

    def test_empty(self):
        self.assertEqual(derive_file_status([]), 'UNKNOWN')


class FormatLfnsPerRseTest(TestCase):

    def test_groups_by_rse(self):
        # Build minimal replica-like objects
        class FakeReplica:
            def __init__(self, lfn, rse):
                self.lfn = lfn
                self.rse = rse

        replicas = [
            FakeReplica('/store/file1.root', 'T1_US_FNAL_Disk'),
            FakeReplica('/store/file2.root', 'T1_US_FNAL_Disk'),
            FakeReplica('/store/file1.root', 'T1_UK_RAL_Disk'),
        ]
        output = _format_lfns_per_rse(replicas)
        self.assertIn('T1_US_FNAL_Disk', output)
        self.assertIn('T1_UK_RAL_Disk', output)
        # RSEs are separated by blank line
        self.assertIn('\n\n', output)

    def test_no_duplicate_lfns_per_rse(self):
        class FakeReplica:
            def __init__(self, lfn, rse):
                self.lfn = lfn
                self.rse = rse

        # Same LFN appears twice for same RSE — should only appear once
        replicas = [
            FakeReplica('/store/file1.root', 'T1_US_FNAL_Disk'),
            FakeReplica('/store/file1.root', 'T1_US_FNAL_Disk'),
        ]
        output = _format_lfns_per_rse(replicas)
        self.assertEqual(output.count('/store/file1.root'), 1)


class ViewEndpointTest(TestCase):

    def setUp(self):
        self.client = Client(HTTP_ACCEPT='application/json')
        self.request = FileIntegrityRequest.objects.create(
            requested_by='testuser',
            status=FileIntegrityRequest.Status.COMPLETED,
            job_id='aabb1122'
        )
        FileReplica.objects.create(
            request=self.request, scope='cms',
            lfn='/store/data/file1.root',
            rse='T1_US_FNAL_Disk', status='OK'
        )
        FileReplica.objects.create(
            request=self.request, scope='cms',
            lfn='/store/data/file1.root',
            rse='T1_UK_RAL_Disk', status='CORRUPTED'
        )
        FileReplica.objects.create(
            request=self.request, scope='cms',
            lfn='/store/data/file2.root',
            rse='T1_US_FNAL_Disk', status='CORRUPTED'
        )

    def test_query_list_returns_200(self):
        r = self.client.get('/api/integrity/query/')
        self.assertEqual(r.status_code, 200)

    def test_query_detail_returns_200(self):
        r = self.client.get(
            f'/api/integrity/query/?request_id={self.request.request_id}'
        )
        self.assertEqual(r.status_code, 200)

    def test_query_detail_missing_returns_404(self):
        r = self.client.get(
            f'/api/integrity/query/?request_id={uuid.uuid4()}'
        )
        self.assertEqual(r.status_code, 404)

    def test_files_requires_request_id(self):
        r = self.client.get('/api/integrity/files/')
        self.assertEqual(r.status_code, 400)

    def test_files_returns_grouped_by_lfn(self):
        r = self.client.get(
            f'/api/integrity/files/?request_id={self.request.request_id}'
        )
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(len(data), 2)  # 2 distinct LFNs

    def test_files_file_status_filter(self):
        r = self.client.get(
            f'/api/integrity/files/?request_id={self.request.request_id}'
            f'&file_status=PARTIALLY_CORRUPTED'
        )
        self.assertEqual(r.status_code, 200)
        data = r.json()
        # file1 has one OK and one CORRUPTED → PARTIALLY_CORRUPTED
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['lfn'], '/store/data/file1.root')

    def test_files_output_lfns_is_plain_text(self):
        r = self.client.get(
            f'/api/integrity/files/?request_id={self.request.request_id}'
            f'&output=lfns'
        )
        self.assertEqual(r.status_code, 200)
        self.assertIn('text/plain', r['Content-Type'])
        lines = r.content.decode().strip().splitlines()
        self.assertEqual(len(lines), 2)

    def test_replicas_requires_request_id(self):
        r = self.client.get('/api/integrity/replicas/')
        self.assertEqual(r.status_code, 400)

    def test_replicas_status_filter(self):
        r = self.client.get(
            f'/api/integrity/replicas/?request_id={self.request.request_id}'
            f'&replica_status=CORRUPTED'
        )
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(len(data), 2)
        self.assertTrue(all(rep['status'] == 'CORRUPTED' for rep in data))

    def test_replicas_lfn_filter(self):
        r = self.client.get(
            f'/api/integrity/replicas/?request_id={self.request.request_id}'
            f'&lfn=/store/data/file1.root'
        )
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(len(data), 2)

    def test_replicas_output_lfns_per_rse_is_plain_text(self):
        r = self.client.get(
            f'/api/integrity/replicas/?request_id={self.request.request_id}'
            f'&output=lfns_per_rse'
        )
        self.assertEqual(r.status_code, 200)
        self.assertIn('text/plain', r['Content-Type'])
        self.assertIn('T1_US_FNAL_Disk', r.content.decode())