import sys
import time
import uproot
import logging
import argparse
from definitions import ValidationStatus

logger = logging.getLogger(__name__)

def integrity_check(
    file_path: str, 
    full_scan: bool = False, 
    timeout_seconds: int=900
) -> (bool, ValidationStatus):
    """
    Check the integrity of a ROOT file by attempting to read its contents. It tries to read all objects, and if it encounters a TTree, it attempts to read all baskets of its branches. The function can operate in two modes: "Fail-Fast" (default) where it stops at the first error, and "Full Scan" where it continues to check all items and reports the total number of passed, failed, and skipped items.
    
    Args:
        file_path (str): Local path to the ROOT file to check.
        full_scan (bool, optional): Perform a full scan by reading every basket (more time-consuming). Defaults to False (Fail-Fast mode).
        timeout_seconds (int, optional): Timeout in seconds for the integrity check. Defaults to 900 seconds (15 minutes).
        
    Returns:
        Tuple[bool, ValidationStatus]: A tuple where the first element indicates overall success and the second element is a ValidationStatus indicating OK, CORRUPTED, or ERROR.
    """
    start_time = time.time()
    logger.info(f"Mode: {'Full Scan' if full_scan else 'Fail-Fast'}")
    
    stats = {'passed': 0, 'skipped': 0, 'failed': 0}
    stats_msg = lambda: f"Checked: {stats['passed']}, Failed: {stats['failed']}, Skipped: {stats['skipped']}"
    
    current_status = ValidationStatus.OK
    
    try:
        with uproot.open(file_path) as f:
            logger.info(f"Successfully opened: {file_path}")
            
            for name, classname in f.classnames().items():
                
                if time.time() - start_time > timeout_seconds:
                    current_status = ValidationStatus.ERROR
                    raise TimeoutError(f"Timeout reached ({timeout_seconds}s).")
                
                if "TTree" in classname:
                    tree = f[name]
                    for branch in tree.branches:
                        try:
                            for i in range(branch.num_baskets):
                                if i % 100 == 0 and (time.time() - start_time > timeout_seconds):
                                    current_status = ValidationStatus.ERROR
                                    raise TimeoutError(f"Timeout during basket decompression ({timeout_seconds}s).")
                                branch.basket(i)
                                stats['passed'] += 1
                        except Exception as e:
                            stats['failed'] += 1
                            logger.error(f"Tree '{name}', Branch '{branch.name}' is corrupt. ({type(e).__name__})")
                            if not full_scan:
                                current_status = ValidationStatus.CORRUPTED
                                raise e
                            
                else:
                    
                    try:
                        f[name].get()
                        stats['passed'] += 1
                    except Exception as e:
                        logger.warning(f"Object '{name}' skipped. ({type(e).__name__})")
                        stats['skipped'] += 1
                        
        if full_scan and stats['failed'] > 0:
            current_status = ValidationStatus.CORRUPTED
            logger.info(f"Run 'Fail-Fast' mode to see more details of failed items.")
            raise RuntimeError(f"{stats['failed']} failed item{'s' if stats['failed'] > 1 else ''} detected.")

        logger.info(f"Integrity check PASSED. ({stats_msg()})")
        return True, current_status

    except Exception as e:
        logger.error(e)
        logger.error(f"Integrity check FAILED. ({stats_msg()})")
        if current_status == ValidationStatus.OK:
            current_status = ValidationStatus.ERROR
        return False, current_status

if __name__ == "__main__":
    
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(funcName)s] - %(message)s'
    )
    
    parser = argparse.ArgumentParser(
        description="Check ROOT file integrity.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
returns:
  A tuple where the first element indicates overall success and the second element is a ValidationStatus indicating OK, CORRUPTED, or ERROR."""
    )
    parser.add_argument("file", help="Local path to the ROOT file to check.")
    parser.add_argument("--full-scan", action="store_true", help="Perform a full scan by reading every basket (more time-consuming). Defaults to False (Fail-Fast mode)")
    parser.add_argument("--timeout", type=int, default=900, help="Timeout in seconds for the integrity check. Defaults to 900 seconds (15 minutes)")
    
    args = parser.parse_args()
        
    success, status = integrity_check(args.file, full_scan=args.full_scan, timeout_seconds=args.timeout)
    logger.info(f"Final Result: {status}")
    sys.exit(0 if success else 1)