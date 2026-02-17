import os
import sys
import uproot
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def integrity_check(file_path, full_scan=False):
    
    logging.info(f"Mode: {'Full Scan' if full_scan else 'Fail-Fast'}")
    
    stats = {'passed': 0, 'skipped': 0, 'failed': 0}
    stats_msg = lambda: f"Checked: {stats['passed']}, Failed: {stats['failed']}, Skipped: {stats['skipped']}"
    
    try:
        with uproot.open(file_path) as f:
            logging.info(f"Successfully opened: {file_path}")
            
            for name, classname in f.classnames().items():
                if "TTree" in classname:
                    tree = f[name]
                    for branch in tree.branches:
                        try:
                            for i in range(branch.num_baskets):
                                branch.basket(i)
                                stats['passed'] += 1
                        except Exception as e:
                            stats['failed'] += 1
                            logging.error(f"Tree '{name}', Branch '{branch.name}' is corrupt. ({type(e).__name__})")
                            if not full_scan:
                                raise e
                            
                else:
                    
                    try:
                        f[name].get()
                        stats['passed'] += 1
                    except Exception as e:
                        logging.warning(f"Object '{name}' skipped. ({type(e).__name__})")
                        stats['skipped'] += 1
                        
        if full_scan and stats['failed'] > 0:
            logging.info(f"Run 'Fail-Fast' mode to see more details of failed items.")
            raise RuntimeError(f"{stats['failed']} failed item{'s' if stats['failed'] > 1 else ''} detected.")

        logging.info(f"Integrity check PASSED. ({stats_msg()})")
        return True

    except Exception as e:
        logging.error(e)
        logging.error(f"Integrity check FAILED. ({stats_msg()})")
        return False

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Check ROOT file integrity.")
    parser.add_argument("file", help="Local .root file path")
    parser.add_argument("--full-scan", action="store_true", help="Read every basket")
    
    args = parser.parse_args()
        
    success = integrity_check(args.file, full_scan=args.full_scan)
    sys.exit(0 if success else 1)