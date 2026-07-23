"""
Grid-free unit tests for run_check.

run_check pulls in the grid stack at import time (gfal2 via file_ops, uproot via
check_decompression, rucio). We stub those modules in sys.modules before import
so the tests run anywhere; the per-RSE logic takes its copy/checksum/content
operations as injectable callables, so the real grid functions are never called.

Run from this directory:
    python3 -m unittest test_run_check -v
"""
import sys
import unittest
from unittest import mock

for _mod in ("gfal2", "uproot", "rucio", "rucio.client"):
    sys.modules.setdefault(_mod, mock.MagicMock())

import run_check                       # noqa: E402
from utils import ValidationStatus     # noqa: E402


# (success_bool, ValidationStatus) tuples as returned by the real check functions
OK        = (True,  ValidationStatus.OK)
CORRUPTED = (False, ValidationStatus.CORRUPTED)
ERROR     = (False, ValidationStatus.ERROR)


def make_copy(reachable):
    """copy_fn fake: returns a fake local path for reachable PFNs, else None."""
    def _copy(pfn, workdir):
        return f"/fake/{pfn}.root" if reachable.get(pfn) else None
    return _copy


class CheckRseTest(unittest.TestCase):

    def test_single_pfn_ok(self):
        result = run_check.check_rse(
            "T1_X_Disk", ["pfnA"], "adler", "/tmp",
            copy_fn=make_copy({"pfnA": True}),
            checksum_fn=mock.Mock(return_value=OK),
            content_fn=mock.Mock(return_value=OK),
        )
        self.assertEqual(result, {"rse": "T1_X_Disk", "pfn": "pfnA", "status": "OK"})

    def test_first_pfn_fails_then_second_ok(self):
        # The regression this fix targets: a transient copy failure on pfnA must
        # not mask the healthy copy on pfnB. Exactly one row, OK, pfn=pfnB.
        content_fn = mock.Mock(return_value=OK)
        result = run_check.check_rse(
            "T1_X_Disk", ["pfnA", "pfnB"], "adler", "/tmp",
            copy_fn=make_copy({"pfnA": False, "pfnB": True}),
            checksum_fn=mock.Mock(return_value=OK),
            content_fn=content_fn,
        )
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["pfn"], "pfnB")
        content_fn.assert_called_once()        # validated once, only on pfnB

    def test_all_pfns_fail_single_error(self):
        result = run_check.check_rse(
            "T1_X_Disk", ["pfnA", "pfnB"], "adler", "/tmp",
            copy_fn=make_copy({"pfnA": False, "pfnB": False}),
            checksum_fn=mock.Mock(return_value=OK),
            content_fn=mock.Mock(return_value=OK),
        )
        self.assertEqual(result["status"], "ERROR")
        self.assertEqual(result["pfn"], "pfnB")   # last attempted PFN, for context

    def test_checksum_corrupted_skips_content(self):
        content_fn = mock.Mock(return_value=OK)
        result = run_check.check_rse(
            "T1_X_Disk", ["pfnA"], "adler", "/tmp",
            copy_fn=make_copy({"pfnA": True}),
            checksum_fn=mock.Mock(return_value=CORRUPTED),
            content_fn=content_fn,
        )
        self.assertEqual(result["status"], "CORRUPTED")
        content_fn.assert_not_called()

    def test_checksum_ok_content_corrupted(self):
        result = run_check.check_rse(
            "T1_X_Disk", ["pfnA"], "adler", "/tmp",
            copy_fn=make_copy({"pfnA": True}),
            checksum_fn=mock.Mock(return_value=OK),
            content_fn=mock.Mock(return_value=CORRUPTED),
        )
        self.assertEqual(result["status"], "CORRUPTED")


class CheckFilesTest(unittest.TestCase):

    def _replicas(self):
        return [{
            "adler32": "abc",
            "rses": {"T1_X_Disk": ["pfnA", "pfnB"], "T2_Y_Disk": ["pfnC"]},
            "states": {"T1_X_Disk": "AVAILABLE", "T2_Y_Disk": "UNAVAILABLE"},
        }]

    def test_one_row_per_available_rse(self):
        fake_client = mock.MagicMock()
        fake_client.list_replicas.return_value = self._replicas()

        calls = []

        def fake_check_rse(rse, pfns, adler, workdir, **kwargs):
            calls.append(rse)
            return {"rse": rse, "pfn": pfns[0], "status": "OK"}

        with mock.patch.object(run_check, "RucioClient", return_value=fake_client), \
             mock.patch.object(run_check, "check_rse", side_effect=fake_check_rse):
            results = run_check.check_files(["cms:/store/x.root"], "/tmp")

        # Only the AVAILABLE RSE is checked → one replica row, one check_rse call
        self.assertEqual(calls, ["T1_X_Disk"])
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]["replicas"]), 1)
        self.assertEqual(results[0]["replicas"][0]["rse"], "T1_X_Disk")

    def test_non_root_lfn_rejected(self):
        results = run_check.check_files(["cms:/store/dataset"], "/tmp")
        self.assertEqual(results[0]["error"], "The LFN provided is not a .root file.")


if __name__ == "__main__":
    unittest.main()
