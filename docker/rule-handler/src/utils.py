import enum
import json
import logging
import os
from typing import List, Optional

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format='%(levelname)s [%(name)s] %(asctime)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

@enum.unique
class Mode(enum.Enum):
    LIST_GENERATION = 'list-generation' # Generate a list of stuck and locks for further processing
    PNR_INVALIDATION = 'threshold-invalidation' # This tackles automatic pnr invalidations
    FILE_EXISTS = 'file-exists' # This tackles .*Destination file exists.* errors by checking if there are other replicas available and then rm from tape. If there's only one available and the others are unavailable, treat it as possibly corrupt.
    POSSIBLY_CORRUPT = 'possibly-corrupt' # For errors .*CHECKSUM MISMATCH.* run corrupt_file.py handler and invalidate?
    POSSIBLY_MISSING = 'possibly-missing' #For unavailable replicas 
    FORCE_RETRY = 'force-retry' # Every period of time (how much? or based on suspended size threshold?) update all SUSPENDED rules to STUCK status
    OVERVIEW = 'overview' # Every period of time (how much? or based on suspended size threshold?) update all SUSPENDED rules to STUCK status

@enum.unique
class MissingErrors(enum.Enum):
    NO_SOURCES = 'RequestErrMsg.NO_SOURCES'
    FILE_NOT_FOUND = 'File not found  after 1 attempts'

@enum.unique
class FileExistsError(enum.Enum):
    FILE_EXISTS_OVERWRITE_DISABLED = 'Destination file exists and overwrite is not enabled'
    FILE_EXISTS_ON_TAPE = 'Destination file exists and is on tape (overwrite-when-only-on-disk requested)'

@enum.unique
class InvalidationExcludeErrors(enum.Enum):
    RSE_DISABLED = 'RSE excluded; not available for writing'

def map_error_to_mode(error: str):
    mode = 'threshold-invalidation'
    mode = 'possibly-missing' if any([e.value in error for e in MissingErrors]) else mode
    mode = 'possibly-corrupt' if 'checksum' in error.lower() else mode
    mode = 'file-exists' if any([e.value in error for e in FileExistsError]) else mode
    mode = None if any([e.value.lower() in error.lower() for e in InvalidationExcludeErrors]) else mode
    return mode

def get_stuck_locks_overview(stuck_locks: pd.DataFrame, rse: str = None, output_json: str = None) -> str:
    pd.set_option("display.max_colwidth", None)
    pd.set_option("display.max_columns", None)

    group_cols = ["rse", "error", "rule_id"] if rse is None else ["error", "rule_id"]
    final_group_cols = ["rse", "error"] if rse is None else ["error"]

    overview = (
        stuck_locks.groupby(group_cols)
        .aggregate({"file_name": "nunique", "file_size": "sum", "rule_size": "first"})
        .sort_values(by="rule_size", ascending=False)
        .reset_index()
    )
    overview = (
        overview.groupby(final_group_cols, as_index=False)
        .agg({"rule_id": "count", "file_name": "sum", "file_size": "sum", "rule_size": "sum"})
        .sort_values(by="rule_size", ascending=False)
    )

    overview.rename(columns={"rule_id": "total_rules", "file_name": "total_files"}, inplace=True)
    overview["file_size"] = overview["file_size"] / 1e15
    overview["rule_size"] = overview["rule_size"] / 1e15

    total_rule_size = overview["rule_size"].sum()
    label = f"RSE {rse}" if rse else "all RSEs"

    if output_json is not None:
        overview['error_desc'] = overview['error']
        overview['error'] = overview['error'].apply(map_error_to_mode)
        overview = overview.drop_duplicates(subset=["rse","error"]).reset_index(drop=True)
        failures = [
            {"rse": row["rse"], "error": row["error"],"total_rules":row["total_rules"],"total_files":row["total_files"],"file_size":row["file_size"],"rule_size":row["rule_size"]}
            for _, row in overview.iterrows() if row["error"] is not None
        ]
        with open(output_json, "w") as f:
            json.dump({"total_rule_size_pb": total_rule_size, "failures": failures}, f)
        
        overview.to_json(output_json.replace('.json','_complete.json'),orient='records',indent=2)

    if rse is None:
        overview = overview.head(10)

    return (
        f"Total suspended rule size: {total_rule_size:.2f} PB\n"
        f"Overview of stuck locks at {label}:\n{overview}"
    )

class FileInvalidationClient():

    """
        _FileInvalidationClient_
        General API for interacting with the CERN File Invalidation service
        
        This class handles both the low-level API communication with the File Invalidation service
        and provides higher-level methods for file invalidation operations.
    """

    def __init__(self, **kwargs):
        """Initialize the File Invalidation client with proper cookie authentication"""
        try:

            self.base_url = "https://file-invalidation.app.cern.ch"
            self.upload_endpoint = "/api/upload/"
            
            
            # Ensure cookie exists and is valid
            self._get_token()
            self._set_headers()

        except Exception as error:
            raise Exception(f"Error initializing FileInvalidationClient\n{str(error)}")
    

    def _get_token(self):
        """
            Get token using post requests to CERN auth api access endpoint. Requires CLIENT_ID and CLIENT_SECRET properly set up.
        """

        try:
            resp = requests.post(
                "https://auth.cern.ch/auth/realms/cern/api-access/token",
                headers={"Content-Type":"application/x-www-form-urlencoded"},
                data={
                    "grant_type": "client_credentials",
                    "client_id": os.environ["CLIENT_ID"],
                    "client_secret": os.environ["CLIENT_SECRET"],
                    "audience": "webframeworks-paas-file-invalidation-tool",
                }
            )
            resp.raise_for_status()

            self.token = resp.json()["access_token"]
        except Exception as e:
            logging.error(f"Error creating token: {str(e)}", exc_info=True)
            raise


    def _set_headers(self):
            """Set the headers for API requests"""
            self.headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
            }

    def upload_invalidation_request(self,reason: str, files: List[str], dry_run: bool = True, mode: str = "global", rse: Optional[str]= None):
        """
            Upload a file invalidation request to the API.
            
            Args:
                reason: The reason for the file invalidation request
                files: A list of file paths to be invalidated
                dry_run: Whether this is a dry run
                mode: Invalidation mode, either "global" or "local"
                rse: RSE specification, required only for local mode
        """

        self._get_token()
        self._set_headers()

        payload = {
            "reason": reason,
            "file_content": "\n".join(files),
            "dry_run": dry_run,
            "mode": mode,
            "global_invalidate_last_replicas": False
        }

        if mode=="local" and rse:
            payload["rse"] = rse

        try:
            url = self.base_url+self.upload_endpoint
            response = requests.post(url,json=payload, headers=self.headers)

            if response.status_code == 401 or response.status_code == 403:
                raise Exception(f"Authentication failed with response {response.json()}")
            elif response.status_code == 400:
                raise Exception(f"Bad request for URL {url} with payload: {payload}")

            response.raise_for_status()
            print(f"Status: {response.status_code}")
            print(f"Raw: {repr(response.text)}")
            data = json.loads(response.text)
            return f"Request id [{data['request_id']}|https://file-invalidation.app.cern.ch/api/query/{data['request_id']}]"
        except requests.exceptions.RequestException as e:
            error_message = f"Error calling file invalidation API: {str(e)}"
            if hasattr(e, "response") and e.response:
                try:
                    error_details = e.response.json()
                    error_message += f" - Details: {json.dumps(error_details)}"
                except:
                    error_message += f" - Status code: {e.response.status_code}"
            
            logging.error(error_message)
            raise Exception(error_message)