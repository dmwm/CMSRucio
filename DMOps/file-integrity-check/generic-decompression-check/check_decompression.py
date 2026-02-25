import sys
import time
import uproot
import logging
import argparse
from definitions import ValidationStatus

logger = logging.getLogger(__name__)

def integrity_check(file_path, full_scan=False, timeout_seconds=900) -> (bool, ValidationStatus):
    start_time = time.time()
    logging.info(f"Mode: {'Full Scan' if full_scan else 'Fail-Fast'}")
    
    stats = {'passed': 0, 'skipped': 0, 'failed': 0}
    stats_msg = lambda: f"Checked: {stats['passed']}, Failed: {stats['failed']}, Skipped: {stats['skipped']}"
    
    current_status = ValidationStatus.OK
    
    try:
        with uproot.open(file_path) as f:
            logging.info(f"Successfully opened: {file_path}")
            
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
                            logging.error(f"Tree '{name}', Branch '{branch.name}' is corrupt. ({type(e).__name__})")
                            if not full_scan:
                                current_status = ValidationStatus.CORRUPTED
                                raise e
                            
                else:
                    
                    try:
                        f[name].get()
                        stats['passed'] += 1
                    except Exception as e:
                        logging.warning(f"Object '{name}' skipped. ({type(e).__name__})")
                        stats['skipped'] += 1
                        
        if full_scan and stats['failed'] > 0:
            current_status = ValidationStatus.CORRUPTED
            logging.info(f"Run 'Fail-Fast' mode to see more details of failed items.")
            raise RuntimeError(f"{stats['failed']} failed item{'s' if stats['failed'] > 1 else ''} detected.")

        logging.info(f"Integrity check PASSED. ({stats_msg()})")
        return True, current_status

    except Exception as e:
        logging.error(e)
        logging.error(f"Integrity check FAILED. ({stats_msg()})")
        if current_status == ValidationStatus.OK:
            current_status = ValidationStatus.ERROR
        return False, current_status

if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
    
    parser = argparse.ArgumentParser(description="Check ROOT file integrity.")
    parser.add_argument("file", help="Local .root file path")
    parser.add_argument("--full-scan", action="store_true", help="Read every basket")
    parser.add_argument("--timeout", type=int, default=900, help="Timeout in seconds")
    
    args = parser.parse_args()
        
    success, status = integrity_check(args.file, full_scan=args.full_scan, timeout_seconds=args.timeout)
    logger.info(f"Final Result: {status}")
    sys.exit(0 if success else 1)