import pandas as pd
from collections import Counter
from rucio.client import Client
from utils import MissingErrors, FileInvalidationClient
import gfal2
import logging
import send_os
from datetime import datetime

rucio_client = Client() 
ctx = gfal2.creat_context()

logger = logging.getLogger(__name__)
gfal2.set_verbose(gfal2.verbose_level.warning)

class HandleMissing:

    def __init__(self, possibly_missing: pd.DataFrame, rse: str, state: str, account: str, quiet=True, dry_run=True,ignore_src_rses = ["T2_IT_Pisa", "T2_IN_TIFR"]):

        possibly_missing = possibly_missing.drop_duplicates(subset="file_name").reset_index(drop=True)
        num_files = possibly_missing.shape[0]
        num_rules = len(possibly_missing.rule_id.unique()) if 'rule_id' in possibly_missing.columns else None

        logger.info(f"There are {num_files:,} stuck files possibly missing, blocking {num_rules:,} rules at RSE {rse}.")

        self.found = 0
        if len(possibly_missing)>0:
            self.possibly_missing = possibly_missing
            self.rse = rse
            self.ignore_src_rses = ignore_src_rses
            self.dry_run = dry_run

            self.get_locks_information()
            lost_replicas = self.analyze_stuck_locks()
            api_response=""
            if lost_replicas is not None:
                output_file = f"./df_missing_files_{self.rse or ''}.csv"
                lost_replicas.to_csv(output_file, index=False)

                if not self.dry_run:
                    api_response = self.invalidate_lost_files(lost_replicas, state) 
                    logger.info(api_response)
                
                if not quiet:
                    send_os.post_logs(df=lost_replicas,
                                    rse=rse,
                                    action="pending_invalidation" if dry_run else "requested_invalidation",
                                    mode="found-lost",
                                    state=state,
                                    account=account)
            self.found = len(lost_replicas) if lost_replicas is not None else 0
            description = f"Invalidated {self.found} files {api_response}" if self.found>0 else f"Found no lost files ({num_files} were investigated)"
            description = "Dry run "+description if self.dry_run else description
            logger.info(description)

    @classmethod
    def from_stuck_locks(cls, all_stuck_locks: pd.DataFrame, rse, state, account, quiet, dry_run):

        pattern = "|".join(e.value for e in MissingErrors)
        possibly_missing = all_stuck_locks[all_stuck_locks["error"].str.contains(pattern)]

        return cls(possibly_missing,rse,state,account,quiet,dry_run)



    def is_available_replica_missing(self,pfns_dicts,states_dicts):
        if Counter(states_dicts.values())['AVAILABLE'] != 1:
            raise ValueError('There is more than one AVAILABLE replica, this case should only act on locks with 1 available replica.')
        
        available_site = [site for site in states_dicts if states_dicts[site]=='AVAILABLE'][0]
        if (self.ignore_src_rses is not None) and (available_site in self.ignore_src_rses):
            logger.info(f'The replica at site {available_site} is ignored; no conclusion will be made of its availability.')
            return False
        pfn = pfns_dicts[available_site][0]

        try:
            ctx.stat(pfn)
        except Exception as e:
            if 'File not found' in str(e):
                return True
        
        return False

    def get_locks_information(self):
        self.possibly_missing['info'] = self.possibly_missing.file_name.apply(lambda s: list(rucio_client.list_replicas(dids=[{'scope':'cms','name':s}],all_states=True))[0])
        self.possibly_missing[['rses','states']] = pd.DataFrame(self.possibly_missing['info'].tolist())[['rses','states']]
        self.possibly_missing['state_count'] = self.possibly_missing['states'].apply(lambda x: dict(Counter(x.values())))

    def analyze_single_replica_locks(self,df_only_one_replica: pd.DataFrame):

        if len(df_only_one_replica)>0:
            logger.info(f'Total single replica stuck locks {f"at {self.rse}" if self.rse is not None else ""}: {df_only_one_replica.shape[0]}')

        df_only_one_replica_unavailable = df_only_one_replica[df_only_one_replica['state_count'].apply(lambda d: 'UNAVAILABLE' in d.keys())]

        if df_only_one_replica.shape[0]>0:
            df_only_one_replica_other = df_only_one_replica[df_only_one_replica['state_count'].apply(lambda d: not 'UNAVAILABLE' in d.keys())]
            if df_only_one_replica_other.shape[0]>0:
                logger.info(f'Single replica stuck locks not UNAVAILABLE (ignored): {df_only_one_replica_other.shape[0]} with the following states: {df_only_one_replica_other["state_count"].drop_duplicates().values}.')

        return df_only_one_replica_unavailable if not df_only_one_replica_unavailable.empty else None

    def analyze_multiple_replica_locks(self, df_multiple_replicas: pd.DataFrame):
        if df_multiple_replicas.shape[0]>0:
            logger.info(f'Total multiple replica stuck locks{f" (one of which is at {self.rse})" if self.rse is not None else ""}: {df_multiple_replicas.shape[0]}')

        df_only_one_available = df_multiple_replicas[df_multiple_replicas['state_count'].apply(lambda d: d['AVAILABLE']==1 if 'AVAILABLE' in d.keys() else False)].reset_index(drop=True)
        if df_only_one_available.shape[0]>0:
            df_only_one_available['lost'] = df_only_one_available.apply(lambda row: self.is_available_replica_missing(row.rses, row.states), axis=1)
            n_possibly_missing = len(df_only_one_available)
            n_lost_files = df_only_one_available["lost"].sum()
            n_ignore = (~df_only_one_available["lost"]).sum()
            logger.info(f'Total multiple replica stuck locks with single available replicas: {n_possibly_missing} [possibly missing]')
            if n_lost_files>0:
                logger.info(f'Total multiple replica stuck locks with single available replicas found to be lost: {n_lost_files}')
            if n_ignore>0:
                logger.info(f'Total multiple replica stuck locks with single available replicas NOT found to be lost: {n_ignore}')
            if df_multiple_replicas.shape[0]-df_only_one_available.shape[0]>0:
                logger.info(f'Total multiple replica stuck locks with multiple available replicas (ignored): {df_multiple_replicas.shape[0]-df_only_one_available.shape[0]}')

            return df_only_one_available.loc[df_only_one_available["lost"]] if not df_only_one_available.loc[df_only_one_available["lost"]].empty else None

        return None

    def analyze_stuck_locks(self):

        state_counts = self.possibly_missing['states'].str.len()

        # Locks that have a single replica
        df_single_replica = self.possibly_missing[state_counts == 1].reset_index(drop=True)
        # Locks that have multiple replicas
        df_multiple_replicas = self.possibly_missing[state_counts > 1].reset_index(drop=True)

        # Locks with only one replica which is unavailable (lost)
        lost_files_single_replica = self.analyze_single_replica_locks(df_single_replica)
    
        # Locks with multiple replicas that are confirmed to be lost
        lost_files_multiple_replicas = self.analyze_multiple_replica_locks(df_multiple_replicas)
        
        df_lost_replicas = pd.DataFrame(columns=["file_name","rule_id"])

        if lost_files_single_replica is not None:
            logger.info(f'Total single replica locks confirmed lost: {len(lost_files_single_replica)}')
            df_lost_replicas = pd.concat([df_lost_replicas,lost_files_single_replica[["file_name","rule_id"]]]).reset_index(drop=True)

        if lost_files_multiple_replicas is not None:
            logger.info(f'There are {len(lost_files_multiple_replicas)} stuck locks with multiple replicas which are confirmed to be permanently lost.')
            df_lost_replicas = pd.concat([df_lost_replicas,lost_files_multiple_replicas[["file_name","rule_id"]]]).reset_index(drop=True)

        return df_lost_replicas if not df_lost_replicas.empty else None

    def invalidate_lost_files(self,df_lost_replicas:pd.DataFrame, state: str):
        inv_client = FileInvalidationClient()

        file_list = df_lost_replicas["file_name"].sort_values().values
        reason = f"Stuck Handler Tool {datetime.today().strftime('%Y-%m-%d')}: Lost files keeping {state} rules" + f" at {self.rse}" if self.rse else ""
        mode = "global"

        response = inv_client.upload_invalidation_request(reason=reason,
                                                files = file_list,
                                                dry_run=self.dry_run,
                                                mode = mode,
                                                rse = None)

        return response 