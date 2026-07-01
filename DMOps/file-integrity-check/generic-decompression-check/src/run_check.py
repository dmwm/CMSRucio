
import os
import json
import logging
import argparse
from typing import List, Dict

from rucio.client import Client as RucioClient

from file_ops import copy_file_locally
from file_ops import validate_checksum as checksum_check
from check_decompression import integrity_check as content_check
from utils import ValidationStatus, setup_logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

def check_rse(
    rse,
    pfns,
    adler,
    workdir,
    full_scan=False,
    timeout_seconds=900,
    copy_fn=copy_file_locally,
    checksum_fn=checksum_check,
    content_fn=content_check,
):
    """
    Validate a file at a single RSE and return exactly one result dict.

    PFNs of an RSE point at the same physical bytes via different access paths,
    so we try them in order and validate the first one that copies — there is no
    value in re-checking the same file through another path. Every PFN attempt
    is logged. Only if *all* PFNs fail to copy do we report a single ERROR, so a
    transient access failure on one PFN can never mask a healthy copy on another.

    The copy/checksum/content callables are injectable to allow unit testing
    without the grid stack.

    Returns: {"rse": str, "pfn": str|None, "status": "OK"|"CORRUPTED"|"ERROR"}
    """
    last_pfn = None
    for pfn in pfns:
        last_pfn = pfn
        local_path = None
        try:
            local_path = copy_fn(pfn, workdir)
            if not local_path:
                logger.error(f"PFN '{pfn}' on RSE '{rse}' -> copy failed; trying next PFN if available.")
                continue

            _, status = checksum_fn(local_path, adler)
            if status != ValidationStatus.CORRUPTED:
                _, status = content_fn(local_path, full_scan=full_scan, timeout_seconds=timeout_seconds)

            logger.info(f"PFN '{pfn}' on RSE '{rse}' -> {status.value}")
            return {"rse": rse, "pfn": pfn, "status": status.value}

        finally:
            if local_path and os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    logger.warning(f"Could not delete local file '{local_path}'. Please check manually.")

    logger.warning(f"All PFNs failed to copy for RSE '{rse}' -> ERROR")
    return {"rse": rse, "pfn": last_pfn, "status": ValidationStatus.ERROR.value}


def check_files(
    lfns: List[str],
    workdir: str,
    rse_expression: str = None,
    full_scan: bool = False,
    timeout_seconds: int = 900
) -> List[Dict]:
    """
    Check integrity of files based on their LFNs by copying them locally, validating checksums and performing content checks by decompression.
    
    Args:
        lfns (List[str]): List of files in the format '<scope>:<lfn>'. Example: 'cms:/store/data/...'. 
                          If no scope is provided, 'cms' is used as the default.
        workdir (str): Local directory to use for copying files.
        rse_expression (str, optional): RSE expression to filter replicas. (default: None - check all replicas).
        full_scan (bool, optional): Perform a full scan by reading every basket (more time-consuming). (default: False)
        timeout_seconds (int, optional): Timeout in seconds for the integrity check of each file. (default: 900s)
    
    Returns:
        List[Dict]: A list of results for each LFN, including replica information and validation status.
    """

    client = RucioClient()
    results = []

    for lfn_with_scope in lfns:
        # Parse scope:lfn format, default to 'cms' if no scope provided
        if ':' in lfn_with_scope:
            scope, lfn = lfn_with_scope.split(':', 1)
        else:
            scope = 'cms'
            lfn = lfn_with_scope
            logger.warning(f"No scope provided for '{lfn}'. Using default scope 'cms'.")

        if not lfn.endswith('.root'):
            logger.error(f"The LFN provided is not a .root file: '{lfn_with_scope}'")
            results.append({"filename": lfn_with_scope, "error": "The LFN provided is not a .root file."})
            continue

        file_result = {"filename": lfn_with_scope, "replicas": []}

        try:
            replicas = client.list_replicas(
                [{'scope': scope, 'name': lfn}], rse_expression=rse_expression
            )

            for replica in replicas:
                adler = replica['adler32']

                for rse in replica['rses'].keys():
                    replica_state = replica['states'].get(rse, 'UNKNOWN')

                    if replica_state != 'AVAILABLE':
                        logger.info(f"LFN '{lfn}' skipped on RSE '{rse}' (state: {replica_state})")
                        continue

                    # One result per RSE — see check_rse for the PFN handling.
                    file_result["replicas"].append(
                        check_rse(
                            rse,
                            replica['rses'][rse],
                            adler,
                            workdir,
                            full_scan=full_scan,
                            timeout_seconds=timeout_seconds,
                        )
                    )

        except Exception as e:
            file_result["error"] = str(e)

        results.append(file_result)

    return results

if __name__ == "__main__":
        
    parser = argparse.ArgumentParser(
        description="Check integrity of files based on their LFNs by copying them locally, validating checksums and performing content checks by decompression.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
returns:
  A list of results for each LFN, including replica information and validation status."""
    )
    parser.add_argument("lfns", nargs="+", help="Files to check in the format '<scope>:<lfn>' (e.g., 'cms:/store/data/...'). A .txt file with a list of LFNs in this format is also accepted. If no scope is provided, 'cms' is used as default.")
    parser.add_argument("workdir", help="Local directory to use for copying files.")
    parser.add_argument("--rse-expression", default=None, help="RSE expression to filter replicas. (default: None - check all replicas).")
    parser.add_argument("--full-scan", action="store_true", help="Perform a full scan by reading every basket (more time-consuming). (default: False)")
    parser.add_argument("--timeout", type=int, default=900, help="Timeout in seconds for the integrity check of each file. (default: 900s)")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity: -v (Warning), -vv (Info), -vvv (Debug). Default is Error.")
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    lfns_list = []
    for arg in args.lfns:
        if arg.endswith(".txt") and os.path.exists(arg):
            with open(arg, 'r') as f:
                lfns_list.extend([line.strip() for line in f if line.strip()])
        else:
            lfns_list.append(arg)
    
    results = check_files(
        lfns=lfns_list,
        workdir=args.workdir,
        rse_expression=args.rse_expression,
        full_scan=args.full_scan,
        timeout_seconds=args.timeout
    )
    print(json.dumps(results))