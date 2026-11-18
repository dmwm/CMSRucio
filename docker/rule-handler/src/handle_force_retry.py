from rucio.client import Client
import logging
from utils import Mode, MissingErrors

rucio_client = Client() 

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')


def update_stuck_locks_rules(stuck_locks, error: str = None, mode: Mode = None):

    if mode:
        if mode==Mode.POSSIBLY_MISSING:
            pattern = "|".join(e.value for e in MissingErrors)
            stuck_locks = stuck_locks[stuck_locks.error.str.contains(pattern)]
        elif mode==Mode.POSSIBLY_CORRUPT:
            stuck_locks = stuck_locks[stuck_locks.error.str.lower().str.contains('checksum')]
        else:
            raise NotImplementedError("Handled force retry by mode has not been implemented for {mode}")


    if error:
        stuck_locks = stuck_locks[stuck_locks.error.str.lower().str.contains(error.lower(), regex=False)]

    n_rule = 0
    n_locks = 0
    for rule_id in stuck_locks['rule_id'].unique():
        if rucio_client.get_replication_rule(rule_id=rule_id)['state']=='SUSPENDED':
            rucio_client.update_replication_rule(rule_id=rule_id,options={'state':'stuck'})
            logging.info(f'{rule_id} was SUSPENDED, updating to STUCK')
            n_rule += 1
            n_locks += stuck_locks[stuck_locks['rule_id']==rule_id].shape[0]

    return n_rule, n_locks

def update_stuck_rules(rule_list, n_locks):

    n_rule = 0
    for rule_id in rule_list:
        if rucio_client.get_replication_rule(rule_id=rule_id)['state']=='SUSPENDED':
            rucio_client.update_replication_rule(rule_id=rule_id,options={'state':'stuck'})
            logging.info(f'{rule_id} was SUSPENDED, updating to STUCK')
            n_rule += 1

    return n_rule, n_locks