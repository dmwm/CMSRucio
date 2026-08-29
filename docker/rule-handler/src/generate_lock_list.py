import pandas as pd
from rucio.client import Client
from dbs.apis.dbsClient import DbsApi
import logging

rucio_client = Client()
dbs_api = DbsApi(url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader")

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')


def df_stuck_locks(suspended=True, account='wmcore_output',rse=None):
    if suspended:
        logging.info('Generating list of stuck locks for SUSPENDED rules.')
        rule_list = list(rucio_client.list_replication_rules(filters={'account':account,'state':'U'}))
    else:
        logging.info('Generating list of stuck locks for STUCK rules.')
        rule_list = list(rucio_client.list_replication_rules(filters={'account':account,'state':'S'}))

    logging.info(f'Found {len(rule_list)} {"suspended" if suspended else "stuck"} rules for account {account}.')

    df_stuck_locks = pd.DataFrame()
    for rule in rule_list:
        rule_id = rule['id']
        rule_error = rule['error']
        dataset_name = rule['name']
        try:
            dataset_size = dbs_api.listBlockSummaries(dataset=dataset_name)[0]['file_size']
        except Exception as e:
            logging.info(f'Error getting dataset size for {dataset_name}: {e}')
            dataset_size = None
        try:
            df_stuck_locks_rule = pd.DataFrame(list(rucio_client.list_replica_locks(rule_id=rule_id)))
            df_stuck_locks_rule = df_stuck_locks_rule.loc[df_stuck_locks_rule.state=='STUCK',['rule_id','rse','name']]
            df_stuck_locks_rule['dataset'] = dataset_name
            df_stuck_locks_rule['rule_size'] = dataset_size
            df_stuck_locks_rule['error'] = rule_error
            if df_stuck_locks_rule.shape[0]==0 and suspended:
                logging.info(f'Rule {rule_id} has no stuck locks. It will be updated from suspended to stuck state.')
                rucio_client.update_replication_rule(rule_id=rule_id,options={'state':'stuck'})
            else:
                df_stuck_locks = pd.concat([df_stuck_locks,df_stuck_locks_rule]).reset_index(drop=True)
        except Exception as e:
            logging.info(f'Error getting stuck locks for rule {rule_id}: {e}')
            continue

    logging.info(f'Found {df_stuck_locks.shape[0]:,} stuck locks for {len(rule_list)} {"SUSPENDED" if suspended else "STUCK"} rules.')
    logging.info('Generating file size for each lock.')
    if df_stuck_locks.shape[0]>0:
        df_stuck_locks['file_size']=df_stuck_locks.name.apply(lambda lfn: dbs_api.listFileArray(logical_file_name=lfn,detail=True)[0]['file_size'] if len(dbs_api.listFileArray(logical_file_name=lfn,detail=True))>0 else None)
        df_stuck_locks.rename(columns={'name':'file_name'},inplace=True)
        if not rse is None:
            df_stuck_locks = df_stuck_locks[df_stuck_locks['rse']==rse]

    return df_stuck_locks
