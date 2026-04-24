import sys
import logging
from enum import Enum

class ValidationStatus(str, Enum):
    OK = "OK"
    CORRUPTED = "CORRUPTED"
    ERROR = "ERROR"
    
def setup_logging(verbosity_level: int):
    """
    Configures the root logger based on a verbosity count (0-3).
    This should only be called by the 'entry point' script (the one the user runs).
    """
    log_levels = {
        0: logging.ERROR,
        1: logging.WARNING,
        2: logging.INFO,
        3: logging.DEBUG
    }
    
    level = log_levels.get(min(verbosity_level, 3), logging.DEBUG)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(funcName)s] - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )