import pandas as pd
from collections import Counter
from rucio.client import Client
import gfal2
import logging
import subprocess
import handle_force_retry
import re
import send_os
from utils import FileExistsError

rucio_client = Client() 
ctx = gfal2.creat_context()

logger = logging.getLogger(__name__)
gfal2.set_verbose(gfal2.verbose_level.warning)

class HandleFileExists:

    def __init__(self, stuck_locks: pd.DataFrame, rse: str, state: str, account: str, dry_run = True, quiet=True):
        
        pattern = "|".join(re.escape(e.value) for e in FileExistsError)
        stuck_locks = stuck_locks[stuck_locks.error.str.contains(pattern)]
        possibly_stuck_on_buffer = stuck_locks.drop_duplicates(subset="file_name").reset_index(drop=True)

        num_files = possibly_stuck_on_buffer.shape[0]
        num_rules = len(possibly_stuck_on_buffer.rule_id.unique())
        logger.info(f"There are {num_files:,} stuck files possibly in buffer, blocking {num_rules:,} rules at RSE {rse}.")

        self.possibly_stuck_on_buffer = possibly_stuck_on_buffer
        self.rse = rse

        self.get_locks_information()
        on_buffer_replicas = self.analyze_stuck_locks()
        if on_buffer_replicas is not None:
            output_file = f"./df_incorrect_on_buffer_{self.rse or ''}.csv"
            on_buffer_replicas.to_csv(output_file, index=False)
            
            if not dry_run:
                self.remove_files_from_buffer(on_buffer_replicas)
                n_rule, n_locks = handle_force_retry.update_stuck_rules(rule_list=stuck_locks.rule_id.unique(),n_locks=stuck_locks.shape[0],)
                logger.info(f"Updated {n_rule} rules to STUCK state, blocking {n_locks} locks.")

            if not quiet:
                send_os.post_logs(df=on_buffer_replicas,
                                rse=rse,
                                action="to_remove_on_buffer" if dry_run else "removed_on_buffer",
                                mode="file-exists",
                                state=state,
                                account=account)
        description = f"Dry-run removed  {len(on_buffer_replicas)} replicas from buffer." if on_buffer_replicas is not None else f"Did not find file exists buffer replicas ({num_files} were investigated)"
        logger.info(description)

    def is_file_incorrectly_on_tape(self,pfns_dicts,rucio_size, rucio_checksum):
        """
        Check if the file is incorrectly on tape.
        """

        pfn = pfns_dicts[self.rse][0]
        incorrect_on_tape = False

        try:
        # Size incorrect
            size_result = ctx.stat(pfn)
            if (size_result.st_size==0) or (size_result.st_size != rucio_size):
                incorrect_on_tape = True

            # Checksum incorrect

            if not incorrect_on_tape:
                checksum = ctx.checksum(pfn,'adler32')
                if checksum != rucio_checksum:
                    incorrect_on_tape = True
        except Exception as e:
            if 'File not found' in str(e):
                rucio_client.declare_bad_file_replicas(replicas=[pfn],reason='File not found while handling file exists error')
        
        # On buffer
        if not incorrect_on_tape:
            try:
                tape_status = ctx.getxattr(pfn,'user.status')
                if tape_status=='ONLINE':
                    incorrect_on_tape = True
            except Exception as e:
                if 'Locality attribute missing' in str(e) or 'File locality reported as UNAVAILABLE' in str(e):
                    incorrect_on_tape = True
                else:
                    logging.error(f'Tape status could not be found {e}')
        
        return incorrect_on_tape

    def get_locks_information(self):
        self.possibly_stuck_on_buffer['info'] = self.possibly_stuck_on_buffer.file_name.apply(lambda s: list(rucio_client.list_replicas(dids=[{'scope':'cms','name':s}],all_states=True))[0])
        self.possibly_stuck_on_buffer[['rses','states','bytes','adler32']] = pd.DataFrame(self.possibly_stuck_on_buffer['info'].tolist())[['rses','states','bytes','adler32']]
        self.possibly_stuck_on_buffer['state_count'] = self.possibly_stuck_on_buffer['states'].apply(lambda x: dict(Counter(x.values())))

    def analyze_stuck_locks(self):
        
        df_unavailable_at_rse = self.possibly_stuck_on_buffer[self.possibly_stuck_on_buffer.apply(lambda row: 'AVAILABLE' in row['state_count'].keys() and row['states'][self.rse] == 'UNAVAILABLE', axis=1)].copy()

        if len(df_unavailable_at_rse)>0:
            df_unavailable_at_rse['incorrect_on_tape'] = df_unavailable_at_rse.apply(lambda row: self.is_file_incorrectly_on_tape(pfns_dicts=row.rses,rucio_size=row.bytes,rucio_checksum=row.adler32),axis=1)
            df_unavailable_at_rse = df_unavailable_at_rse.loc[df_unavailable_at_rse['incorrect_on_tape'],['rule_id','rse','file_name','rses']]
            df_unavailable_at_rse[f'pfn_at_{self.rse}'] = df_unavailable_at_rse['rses'].apply(lambda pfns: pfns[self.rse][0])
            df_unavailable_at_rse.reset_index(drop=True,inplace=True)

        return df_unavailable_at_rse if not df_unavailable_at_rse.empty else None

    def remove_files_from_buffer(self, df_to_invalidate_locally):

        logging.info(f'{df_to_invalidate_locally.shape[0]} files will be removed from buffer at {self.rse}.')

        for pfn in df_to_invalidate_locally[f'pfn_at_{self.rse}']:
            removeOutput = subprocess.run(['gfal-rm', pfn])
            logging.info(removeOutput)
