
import os
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

                    for pfn in replica['rses'][rse]:
                        local_path = None
                        replica_result = {"rse": rse,  "pfn": pfn, "status": None}
                        
                        try:
                            local_path = copy_file_locally(pfn, workdir)
                            if not local_path:
                                replica_result["status"] = ValidationStatus.ERROR.value
                                file_result["replicas"].append(replica_result)
                                logger.error(f"Failed to copy '{pfn}' locally from RSE '{rse}'. Skipping integrity checks.")
                                continue
                            
                            _, replica_result["status"] = checksum_check(local_path, adler)
                            if replica_result["status"] != ValidationStatus.CORRUPTED:
                                _, replica_result["status"] = content_check(local_path, full_scan=full_scan, timeout_seconds=timeout_seconds)
                            replica_result["status"] = replica_result["status"].value
                            
                        finally:
                            
                            if local_path and os.path.exists(local_path):
                                try:
                                    os.remove(local_path)
                                except OSError:
                                    logger.warning(f"Could not delete local file '{local_path}'. Please check manually.")

                        file_result["replicas"].append(replica_result)

                        break  # only one PFN per RSE is necessary for the check
                    
                    else:
                        logger.warning(f"All PFNs failed for LFN '{lfn}' on RSE '{rse}'")

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
    print(results)