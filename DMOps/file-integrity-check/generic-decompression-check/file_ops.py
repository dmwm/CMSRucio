import os
import gfal2
import random
import logging
import contextlib
from pathlib import Path
from definitions import ValidationStatus

logger = logging.getLogger(__name__)

@contextlib.contextmanager
def suppress_logs(logger_name):
    logger = logging.getLogger(logger_name)
    current_level = logger.level
    logger.setLevel(logging.ERROR)
    try:
        yield
    finally:
        logger.setLevel(current_level)

def copy_file_locally(pfn: str, local_dir: str) -> str:
    
    local_filename = f"file-integrity-check-file-to-check-{random.randint(100000, 999999)}.root"
    local_path = Path(local_dir).resolve() / local_filename
    logger.info(f"Copy '{pfn}' to '{local_path}'")

    try:
        os.makedirs(local_dir, exist_ok=True)

        ctx = gfal2.creat_context()
        with suppress_logs('gfal2'):
            ctx.filecopy(pfn, f"file://{local_path}")
            
        logger.info(f"Copy of '{pfn}' succeeded.")
        return str(local_path)

    except Exception as e:
        logger.error(f"Copy of '{pfn}' failed: {e}")
        return None

def validate_checksum(local_path: str, adler: str) -> (bool, ValidationStatus):

    try:
        ctx = gfal2.creat_context()
        checksum_ok = ctx.checksum(f"file://{local_path}", "adler32") == adler

        if checksum_ok:
            logging.info(f"Checksum validation passed for {local_path}")
            return True, ValidationStatus.OK
        else:
            logging.warning(f"Checksum validation failed for {local_path}")
            return False, ValidationStatus.CORRUPTED

    except Exception as e:
        logging.error(f"Checksum validation failed for {local_path}: {e}")
        return False, ValidationStatus.ERROR
    
if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
    
    test_local_dir = "./tmp"
    
    print("\n1.1. Testing copy_file_locally - valid PFN.")
    test_pfn1 = "davs://hip-cms-se.csc.fi:2880/store/user/nbinnorj/RAWToPFNANO_v0p1/CRABOUTPUT/DYto2Mu-4Jets_Bin-MLL-50_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAOD_HCalPFCuts_eta2829depth1_thresh4p0_RAWToPFNANO_v0p1/260224_232450/0001/NANOAODSIM_1721.root"
    local_file1 = copy_file_locally(test_pfn1, test_local_dir)
    print(f"Test 1.1. completed. local_file1 = '{local_file1}'")
    
    print("\n1.2. Testing copy_file_locally - invalid PFN.")
    test_pfn2 = "davs://hip-cms-se.csc.fi:2880/store/user/nbinnorj/RAWToPFNANO_v0p1/CRABOUTPUT/DYto2Mu-4Jets_Bin-MLL-50_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAOD_HCalPFCuts_eta2829depth1_thresh4p0_RAWToPFNANO_v0p1/260224_232450/0001/NANOAODSIM_1721_invalid.root"
    local_file2 = copy_file_locally(test_pfn2, test_local_dir)
    print(f"Test 1.2. completed. local_file2 = '{local_file2}'")

    print("\n2.1. Testing validate_checksum - valid checksum.")
    true_adler = "d34b712d"
    is_valid, status = validate_checksum(local_file1, true_adler)
    print(f"Test 2.1 completed. Validation result: {is_valid}, Status: {status}")
    
    print("\n2.2. Testing validate_checksum - invalid checksum.")
    false_adler = "00000000"
    is_valid, status = validate_checksum(local_file1, false_adler)
    print(f"Test 2.2 completed. Validation result: {is_valid}, Status: {status}")