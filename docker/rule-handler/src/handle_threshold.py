import pandas as pd

pd.set_option("future.no_silent_downcasting", True)
import json
import logging
import os
from datetime import datetime

import requests
import send_os
from utils import FileInvalidationClient

logger = logging.getLogger(__name__)

PB_DENOMINATOR = 1e15
TB_DENOMINATOR = 1e12
MAX_ITER = 100
URL = "https://unified-api.app.cern.ch/pre-invalidation/pre-invalidation/bulk-validate"

class HandleBulkInvalidate:

    def __init__(self, stuck_locks: pd.DataFrame, state: str, account: str, rse: str, quiet=True, dry_run=True):

        self.possibly_invalidated = self._prepare_dataframe(stuck_locks)
        self.token = None
        self.dry_run = dry_run
        self.found = 0
        self.rse = rse

        i = 0
        while (self.possibly_invalidated.can_invalidate.isna().any()) and (i<MAX_ITER):
            unknown_datasets = self.possibly_invalidated[self.possibly_invalidated.can_invalidate.isna()]
            logger.info(f"Iteration #{i}: {unknown_datasets.shape[0]} datasets with unknown invalidation status")
            chunk_size = max(300,unknown_datasets.n_files.min())
            chunk = self._prepare_chunk(chunk_size)
            payload = self._prepare_payload(chunk)
            bulk_validation = self._send_bulk_validation_request(payload)
            if "results" in bulk_validation.keys():
                self._parse_bulk_validation_results(bulk_validation["results"])
            else:
                logger.warning(f"Response does not contain results, skipping. Response\n{bulk_validation}")
            i += 1

        invalid_replicas = self.possibly_invalidated.loc[self.possibly_invalidated.can_invalidate].explode(column='dataset_filenames').reset_index().rename(columns={'dataset_filenames':'file_name'})
        self.found = len(invalid_replicas)

        api_response = ""
        if not invalid_replicas.empty:
            output_file = f"./df_bulk_invalidate_files.csv"
            invalid_replicas.to_csv(output_file, index=False)

            if not self.dry_run:
                logger.info(f"Sending invalidation")
                api_response = self.invalidate_bulk_files(invalid_replicas, state)
                logger.info(api_response)

            if not quiet:
                send_os.post_logs(df=invalid_replicas.merge(stuck_locks[['file_name', 'rule_id']],on='file_name',how='left'),
                                rse=rse,
                                action="pending_invalidation" if dry_run else "requested_invalidation",
                                mode="threshold-invalidation",
                                state=state,
                                account=account)

        description = f"Invalidated {self.found} files {api_response}" if self.found>0 else f"Found no files to invalidate ({self.possibly_invalidated.shape[0]} datasets were investigated)"
        description = "Dry run "+description if self.dry_run else description
        logger.info(description)

    def _prepare_dataframe(self, stuck_locks: pd.DataFrame):
        stuck_locks = stuck_locks.groupby('dataset').agg({'file_name':list})
        stuck_locks["n_files"] = stuck_locks["file_name"].apply(len)
        stuck_locks["can_invalidate"] = stuck_locks["file_name"].apply(lambda files: False if any("/RAW" in f for f in files) or any("data/" in f for f in files) else pd.NA)
        stuck_locks = stuck_locks.sort_values(by="n_files", ascending=True)
        return stuck_locks.rename(columns={'file_name':'dataset_filenames'})

    def _prepare_chunk(self, chunk_size):
        unknown_invalidation = self.possibly_invalidated[self.possibly_invalidated.can_invalidate.isna()]
        unknown_invalidation = unknown_invalidation.sort_values(by="n_files", ascending=True)
        cumsum = unknown_invalidation["n_files"].cumsum()
        if chunk_size is None:
            return unknown_invalidation

        mask = cumsum<=chunk_size
        
        if not mask.any():
            mask.iloc[0] = True

        return unknown_invalidation[mask]

    def _prepare_payload(self, df_chunk):
        payload_dict = df_chunk.drop(columns=["n_files"]).to_dict()
        payload_dict['tolerance'] = 5
        return payload_dict

    def _send_bulk_validation_request(self, payload):
        if self.token is None:
            self._get_token()
        
        try:
            response = requests.post(
                URL,
                data=json.dumps(payload),
                headers=self.headers
            )

            response.raise_for_status()
            logger.debug(f"Results: {response.text}")
            
            return response.json()
        except Exception as e:
            logger.warning(f"Error on chunk  {e}")
            import traceback
            traceback.print_exc()

    def _parse_bulk_validation_results(self, results):
        logger.info(results)
        df_results = pd.DataFrame(results).T['can_invalidate']
        logger.info(df_results)
        self.possibly_invalidated = self.possibly_invalidated.fillna({'can_invalidate':df_results}).infer_objects(copy=False)

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
                    "client_id": os.environ['PNR_API_CLIENT_ID'],
                    "client_secret": os.environ['PNR_API_CLIENT_SECRET'], 
                    "audience": "cms-pnr-api",
                }
            )
            resp.raise_for_status()

            self.token = resp.json()["access_token"]
            self._set_headers()
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

    def invalidate_bulk_files(self,df_invalid_replicas:pd.DataFrame, state: str):
        inv_client = FileInvalidationClient()

        file_list = df_invalid_replicas["file_name"].sort_values().values
        reason = f"Stuck Handler Tool {datetime.today().strftime('%Y-%m-%d')}: Threshold invalidation of files keeping {state} rules" + f" at {self.rse}" if self.rse else ""
        mode = "global"


        response = inv_client.upload_invalidation_request(reason=reason,
                                                files = file_list,
                                                dry_run=self.dry_run,
                                                mode = mode,
                                                rse = None)

        return response 