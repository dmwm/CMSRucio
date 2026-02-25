from enum import Enum

class ValidationStatus(str, Enum):
    OK = "OK"
    CORRUPTED = "CORRUPTED"
    ERROR = "ERROR"