import os
import gfal2
import random
import logging
import contextlib
from pathlib import Path
from utils import ValidationStatus

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

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
    logger.info(f"Coping '{pfn}'.")

    try:
        os.makedirs(local_dir, exist_ok=True)

        ctx = gfal2.creat_context()
        with suppress_logs('gfal2'):
            ctx.filecopy(pfn, f"file://{local_path}")
            
        logger.info(f"File '{pfn}' copied successfully to '{local_path}'.")
        return str(local_path)

    except Exception as e:
        logger.error(f"Copy of '{pfn}' failed: {e}")
        return None

def validate_checksum(local_path: str, adler: str) -> (bool, ValidationStatus):

    try:
        ctx = gfal2.creat_context()
        checksum_ok = ctx.checksum(f"file://{local_path}", "adler32") == adler

        if checksum_ok:
            logger.info(f"Checksum validation passed for {local_path}")
            return True, ValidationStatus.OK
        else:
            logger.warning(f"Checksum validation failed for {local_path}")
            return False, ValidationStatus.CORRUPTED

    except Exception as e:
        logger.error(f"Checksum validation failed for {local_path}: {e}")
        return False, ValidationStatus.ERROR