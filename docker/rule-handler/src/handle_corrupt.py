import pandas as pd
from collections import Counter
from rucio.client import Client
import gfal2
import subprocess
import logging
import os
import send_os
from datetime import datetime
from utils import FileInvalidationClient

rucio_client = Client()
ctx = gfal2.creat_context()

logger = logging.getLogger(__name__)
gfal2.set_verbose(gfal2.verbose_level.warning)

copy_path = os.getenv("TEMP_PATH")

class HandleCorrupt:

    def __init__(self, possibly_corrupt: pd.DataFrame, rse, state, account, quiet: bool = False, dry_run: bool = False):
        self.possibly_corrupt = possibly_corrupt.drop_duplicates(subset="file_name").reset_index(drop=True)
        num_files = possibly_corrupt.shape[0]

        self.get_locks_information()
        self.rse = rse
        self.dry_run = dry_run
        corrupt_replicas = self.analyze_stuck_locks()
        

        api_response = ""
        if corrupt_replicas is not None:
            output_file = f"./df_corrupt_{self.rse or ''}.csv"
            corrupt_replicas.to_csv(output_file, index=False)

            if not self.dry_run:
                api_response = self.invalidate_corrupt_files(corrupt_replicas, state)
                logger.info(api_response)
            
            if not quiet:
                send_os.post_logs(df=corrupt_replicas,
                                rse=rse,
                                action="pending_invalidation" if dry_run else "requested_invalidation",
                                mode="found-corrupt",
                                state=state,
                                account=account)
        self.found = len(corrupt_replicas) if corrupt_replicas is not None else 0
        description = f"Invalidated {self.found} files {api_response}" if self.found>0 else f"Found no corrupt files ({num_files} were investigated)"
        description = "Dry run "+description if self.dry_run else description 
        logger.info(description)


    @classmethod
    def from_stuck_locks(cls, all_stuck_locks: pd.DataFrame, rse, state, account, quiet=False, dry_run=True):

        possibly_corrupt = all_stuck_locks[all_stuck_locks["error"].str.lower().str.contains('checksum')]
        return cls(possibly_corrupt,rse,state,account,quiet,dry_run)

    def is_size_corrupt(self, pfns_dicts, states_dicts, rucio_size):
        if Counter(states_dicts.values())['AVAILABLE'] != 1:
            raise ValueError('There is more than one AVAILABLE replica, this case should only act on locks with 1 available replica.')

        available_site = [site for site in states_dicts if states_dicts[site] == 'AVAILABLE'][0]
        pfn = pfns_dicts[available_site][0]

        result = ctx.stat(pfn)
        return result.st_size != rucio_size

    def is_checksum_corrupt(self, pfns_dicts, states_dicts, rucio_checksum, deep=False, copy_path=None):
        if Counter(states_dicts.values())['AVAILABLE'] != 1:
            raise ValueError('There is more than one AVAILABLE replica, this case should only act on locks with 1 available replica.')

        if deep and copy_path is None:
            raise ValueError('If deep is True, copy_path must be provided to store the file locally for checksum verification.')

        available_site = [site for site in states_dicts if states_dicts[site] == 'AVAILABLE'][0]
        pfn = pfns_dicts[available_site][0]

        checksum_corrupt = None

        if deep:
            file_name = pfn.split('/')[-1]  # Ensure the pfn is just the file name
            try:
                ctx.filecopy(pfn, "file://" + copy_path + file_name)
                deep_checksum = ctx.checksum("file://" + copy_path + file_name, 'adler32')
                logger.info(f'{pfn},{deep_checksum}')
                checksum_corrupt = deep_checksum != rucio_checksum
                removeOutput = subprocess.run(['rm', copy_path + file_name])
                logger.info(f"Removed local copy of {file_name} after checksum verification. {removeOutput.stdout}")
            except Exception as e:
                logger.warning('Error during file copy or checksum verification:', e)
            return checksum_corrupt
        else:
            checksum = ctx.checksum(pfn, 'adler32')
            return checksum != rucio_checksum

    def get_locks_information(self):
        self.possibly_corrupt['info'] = self.possibly_corrupt.file_name.apply(lambda s: list(rucio_client.list_replicas(dids=[{'scope': 'cms', 'name': s}], all_states=True))[0])
        self.possibly_corrupt[['rses', 'states', 'adler32', 'bytes']] = pd.DataFrame(self.possibly_corrupt['info'].tolist())[['rses', 'states', 'adler32', 'bytes']]
        self.possibly_corrupt['state_count'] = self.possibly_corrupt['states'].apply(lambda x: dict(Counter(x.values())))

    def clear_temp_directory(self):
        if len(os.listdir(copy_path)) > 0:
            subprocess.run(['rm', copy_path + "*.root"])

    def analyze_stuck_locks(self):

        # Checks only cases with one available replica
        df_one_available_replica = self.possibly_corrupt[self.possibly_corrupt['state_count'].apply(lambda d: d['AVAILABLE'] == 1 if 'AVAILABLE' in d.keys() else False)].reset_index(drop=True)

        df_corrupt_replicas = pd.DataFrame(columns=['file_name', 'rule_id'])

        if len(df_one_available_replica) > 0:
            n_total = len(df_one_available_replica)
            logger.info(f"Running corruption checks on {n_total} files with one available replica...")

            logger.info("Checking size corruption...")
            df_one_available_replica.loc[:, 'is_size_corrupt'] = df_one_available_replica.apply(lambda row: self.is_size_corrupt(row.rses, row.states, row.bytes), axis=1)
            n_size_corrupt = df_one_available_replica['is_size_corrupt'].sum()
            logger.info(f"Size corruption: {n_size_corrupt}/{n_total} files flagged ({n_size_corrupt/n_total:.1%})")

            logger.info("Checking rucio checksum corruption...")
            df_one_available_replica.loc[df_one_available_replica.is_size_corrupt == False, 'is_checksum_corrupt'] = df_one_available_replica.loc[df_one_available_replica.is_size_corrupt == False, :].apply(lambda row: self.is_checksum_corrupt(row.rses, row.states, row.adler32), axis=1)
            n_checksum_candidates = (df_one_available_replica['is_size_corrupt'] == False).sum()
            n_checksum_corrupt = df_one_available_replica['is_checksum_corrupt'].sum()
            logger.info(f"Checksum corruption: {n_checksum_corrupt}/{n_checksum_candidates} candidates flagged ({n_checksum_corrupt/n_checksum_candidates:.1%} of checked [not size corrupt])")

            self.clear_temp_directory()

            logger.info("Checking deep checksum corruption...")
            df_one_available_replica.loc[df_one_available_replica.is_checksum_corrupt == False, 'is_checksum_corrupt_deep'] = df_one_available_replica.loc[df_one_available_replica.is_checksum_corrupt == False, :].apply(lambda row: self.is_checksum_corrupt(row.rses, row.states, row.adler32, deep=True, copy_path=copy_path), axis=1)
            n_deep_candidates = (df_one_available_replica['is_checksum_corrupt'] == False).sum()
            n_deep_corrupt = df_one_available_replica['is_checksum_corrupt_deep'].sum()
            logger.info(f"Deep checksum corruption: {n_deep_corrupt}/{n_deep_candidates} candidates flagged ({n_deep_corrupt/n_deep_candidates:.1%} of checked [not rucio checksum corrupt])")

            df_corrupt_replicas = df_one_available_replica[df_one_available_replica.is_size_corrupt | df_one_available_replica.is_checksum_corrupt | df_one_available_replica.is_checksum_corrupt_deep].reset_index(drop=True)
            logger.info(f"Total corrupt files found: {len(df_corrupt_replicas)}/{n_total} ({len(df_corrupt_replicas)/n_total:.1%})")
        
        return df_corrupt_replicas if not df_corrupt_replicas.empty else None

    def invalidate_corrupt_files(self, df_corrupt_to_invalidate: pd.DataFrame, state: str):
        inv_client = FileInvalidationClient()

        file_list = df_corrupt_to_invalidate["file_name"].sort_values().values
        reason = f"Stuck Handler Tool {datetime.today().strftime('%Y-%m-%d')}: Corrupt files keeping {state} rules at {self.rse}"
        mode = "global"

        response = inv_client.upload_invalidation_request(reason=reason,
                                                          files=file_list,
                                                          dry_run=self.dry_run,
                                                          mode=mode,
                                                          rse=None)

        return response