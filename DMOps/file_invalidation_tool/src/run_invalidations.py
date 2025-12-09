import logging
import argparse
import subprocess
import asyncio
from glob import glob
from enum import Enum
import pandas as pd
from invalidate_dbs import invalidate_datasets as invalidate_dbs_datasets
from invalidate_dbs import invalidate_files as invalidate_dbs_files
from integrity_validation import checksum_invalidate_dids
from invalidate_rucio import main as invalidate_rucio
import os

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

class RunningMode(Enum):
    GLOBAL = 'global'
    ONLY_DBS = 'only-dbs'
    ONLY_RUCIO = 'only-rucio'
    SITE_INVALIDATION = 'site-invalidation'
    INTEGRITY_VALIDATION = 'integrity-validation'

def check_arguments():
    """
    Parse and check the arguments provided, and perform various validations based on the running mode.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('running_mode', type=RunningMode, help='Running mode: --global, --only-dbs, --only-rucio, --site-invalidation, checksum-validation ')
    parser.add_argument('--dry-run', action='store_true', help='Test the script without deleting anything')
    parser.add_argument('--erase-mode', action='store_true', help='Erase empty datasets and containers')
    parser.add_argument('--rucio-mode', action='store_true', help='Invalidate files using rucio instead of spark')
    parser.add_argument('--global-invalidate-last-replicas', action='store_true', help='When using site-invalidation, if the replica is the last available one globally, also invalidate the file in DBS')
    parser.add_argument('--rse', type=str, help='Site name on which to perform the invalidations')
    parser.add_argument('--reason', type=str, help='Comment for the deletion')

    args = parser.parse_args()

    # The --global-invalidate-last-replicas option is only valid for site-invalidation mode
    if getattr(args, 'global_invalidate_last_replicas', False) and args.running_mode != RunningMode.SITE_INVALIDATION:
        raise ValueError('--global-invalidate-last-replicas can only be used with site-invalidation mode')

    if not args.running_mode == RunningMode.INTEGRITY_VALIDATION and not args.reason:
        raise ValueError('Reason is required.')

    # Integrity invalidation mode
    if args.running_mode == RunningMode.INTEGRITY_VALIDATION:
        files = glob('/input/*.csv')
        did_level = None
        if len(files) != 1:
            raise ValueError('Only one csv file is expected as /input/ parameter')
        replicas_df = pd.read_csv(files[0])
        if not ('FILENAME' in replicas_df.columns and 'RSE_EXPRESSION' in replicas_df.columns):
            raise ValueError('File must contain columns FILENAME and RSE_EXPRESSION')
        if len(replicas_df) == 0:
            raise ValueError("Files list can't be empty")
        for did in replicas_df['FILENAME'].values:
            current_level = None
            if '.' in did:
                current_level = 'file'
            elif '#' in did:
                current_level = 'dataset'
            else:
                current_level = 'container'
            if did_level is None:
                did_level = current_level
            if did_level != current_level:
                raise ValueError('All DIDs must be only at file level')
        if did_level != 'file':
            raise ValueError('checksum-validation mode can only be used with file level DIDs')
        if args.erase_mode:
            raise ValueError('Erase mode is not allowed when using checksum-validation mode. Only [--reason,--dry-run] are allowed when using checksum-validation mode')
        return args.running_mode,replicas_df, args.dry_run, args.rucio_mode

    files = glob('/input/*.txt')
    if len(files) != 1:
        raise ValueError('Only one txt file is expected as /input/ parameter')

    dids = []
    with open(files[0],'r') as f:
        dids = f.readlines()
    dids = [did.strip().replace('\n','') for did in dids]

    did_level = None
    for did in dids:
        current_level = None
        if '.' in did:
            current_level = 'file'
        elif '#' in did:
            current_level = 'dataset'
        else:
            current_level = 'container'
        if did_level is None:
            did_level = current_level
        if did_level != current_level:
            raise ValueError('All DIDs must be in the same level')

    # Site invalidation mode
    if args.running_mode == RunningMode.SITE_INVALIDATION:
        if not args.rse:
            raise ValueError('RSE is required when using site-invalidation mode')
        if args.erase_mode:
            raise ValueError('Erase mode is not allowed when using site-invalidation mode. Only [--reason,--rse,--dry-run] are allowed when using site-invalidation mode')
        return args.running_mode, files[0].split('/')[-1],did_level, dids, args.rse, args.reason, args.dry_run, args.rucio_mode, args.global_invalidate_last_replicas

    # Global, DBS or Rucio invalidation mode
    if args.rse:
        raise ValueError('RSE is used only when using site-invalidation mode')
    return args.running_mode,files[0].split('/')[-1], did_level, dids, args.reason,args.dry_run, args.erase_mode, args.rucio_mode

def init_proxy():
    """
    Validate the existence and names the two PEM files in the /certs/ directory and init proxy with them.
    """
    #Validate certs
    certs = glob('/certs/*.pem')
    if len(certs) != 2:
        raise ValueError('Only two pem files are expected')
    certs = [file.split('/')[1] for file in certs]
    if 'userkey.pem' in certs and 'usercert.pem' in certs:
        raise ValueError('Only usercert.pem and userkey.pem are expected')

    cert_path = '/certs/usercert.pem'
    key_path = '/certs/userkey.pem'

    try:
        subprocess.run(['chmod', '400', cert_path], check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        if e.returncode == 1:
            logging.info(f"Failed to set permissions for {cert_path}. File is likely read-only.")
        else:
            raise Exception(f"Error setting permissions for {cert_path}: {e.stderr}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}")
    
    try:
        subprocess.run(['chmod', '400', key_path], check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        if e.returncode == 1:
            logging.info(f"Failed to set permissions for {key_path}. File is likely read-only.")
        else:
            raise Exception(f"Error setting permissions for {key_path}: {e.stderr}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}")
    
    
    try:
        subprocess.run(['voms-proxy-init', '-voms', 'cms', '-rfc', '-valid', '192:00', '--cert', '/certs/usercert.pem', '--key', '/certs/userkey.pem'], check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        if "Created proxy" not in e.stdout:
            logging.error(f"Command '{e.cmd}' returned non-zero exit status {e.returncode}.")
            logging.error("Error message:", e.stderr)
            raise ValueError(e.stderr)

def submit_list_generation_job(did_level, input_file,rse=None,rucio_mode=False):
    
    if rse is None:
        logging.info(f'>RSE is none, submitting invalidation, rucio mode {rucio_mode}')
        if rucio_mode:
            logging.info('Starting rucio list generation')
            result = subprocess.run(['/src/submit_invalidation_rucio.sh', did_level, input_file], check=True, capture_output=True, text=True)
            logging.info('Invalidation lists with rucio generated successfully')
        else:
            logging.info('Starting spark job')
            result = subprocess.run(['/src/submit_invalidation.sh', did_level, input_file], check=True, capture_output=True, text=True)
        logging.info(f'>Result is {result}')
    else:
        logging.info(f'>Submitting invalidation, rucio mode {rucio_mode}')
        if rucio_mode:
            logging.info('Starting rucio list generation')
            result = subprocess.run(['/src/submit_invalidation_rucio.sh', did_level, input_file,'--rse' ,rse], check=True, capture_output=True, text=True)
        else:
            logging.info('Starting spark job list generation')
            result = subprocess.run(['/src/submit_invalidation.sh', did_level, input_file,'--rse' ,rse], check=True, capture_output=True, text=True)
    if result.returncode != 0:
        logging.info('>result.returncode is not 0')
        raise ValueError(result.stderr)
    if rucio_mode:
        logging.info('Finished rucio job list generation')
    else:
        logging.info('Finished spark job list generation')
    logging.info('--------------------------------------------')

def dbs_invalidation(did_level, dids, dry_run=False):
    logging.info('Starting DBS invalidation')
    if did_level == 'file':
        invalidate_dbs_files(dids, test=dry_run)
    else:
        files = []
        with open('/input/dbs_files_inv.txt', 'r') as f:
            files = f.readlines()
        files = [x.strip().replace('\n', '') for x in files]
        if did_level == 'dataset':
            invalidate_dbs_files(files, test=dry_run)
        else:
            invalidate_dbs_datasets(files, dids, test=dry_run)
    logging.info('Finished DBS invalidation')
    logging.info('--------------------------------------------')

def rucio_invalidation(did_level, dids, reason, dry_run=False, erase_mode=False, global_invalidate_last_replicas=False):
    logging.info('Starting Rucio invalidation')
    replicas_df = pd.read_csv('/input/rucio_replicas_inv.csv')
    datasets = []
    containers = []
    rules_delete_df = pd.read_csv('/input/rucio_rules_delete.csv')
    rules_stuck = []

    if did_level == 'file':
        with open('/input/rucio_rules_stuck.txt', 'r') as f:
            rules_stuck = f.readlines()
            rules_stuck = [x.strip() for x in rules_stuck]
    elif did_level == 'dataset':
        datasets = dids
    elif did_level == 'container':
        containers = dids
        with open('/input/datasets_inv.txt', 'r') as f:
            datasets = f.readlines()
            datasets = [x.strip() for x in datasets]
    logging.info('Found %d replicas to invalidate' % len(replicas_df))
    logging.info('Found %d rules to delete' % len(rules_delete_df))
    logging.info('Found %d rules to stuck' % len(rules_stuck))
    if erase_mode:
        logging.info('Found %d datasets to erase' % len(datasets))
        logging.info('Found %d containers to erase' % len(containers))

    loop = asyncio.get_event_loop()
    reason = "File Invalidation Tool - " + reason
    if erase_mode:
        loop.run_until_complete(invalidate_rucio(replicas_df, reason, rules_stuck, rules_delete_df, datasets, containers, dry_run, global_invalidate_last_replicas))
    else:
        loop.run_until_complete(invalidate_rucio(replicas_df, reason, rules_stuck, dry_run=dry_run, global_invalidate_last_replicas=global_invalidate_last_replicas))
    logging.info('Finished Rucio invalidation')

if __name__ == '__main__':
    args = check_arguments()
    init_proxy()
    # Checksum validation
    if args[0] == RunningMode.INTEGRITY_VALIDATION:
        _, dids_df, dry_run,rucio_mode = args
        checksum_invalidate_dids(dids_df, dry_run)
    # Local invalidation
    elif args[0] == RunningMode.SITE_INVALIDATION:
        _, input_file, did_level, dids, rse, reason, dry_run, rucio_mode, global_invalidate_last_replicas = args
        try:
            submit_list_generation_job(did_level, input_file, rse=rse,rucio_mode=rucio_mode)
            rucio_invalidation(did_level, dids, reason, dry_run=dry_run, global_invalidate_last_replicas=global_invalidate_last_replicas)
        except subprocess.CalledProcessError as e:
            logging.error("Error running shell script:")
            logging.error(e.stderr)
    # Global, Rucio or DBS invalidation
    else:
        running_mode,input_file, did_level, dids, reason, dry_run, erase_mode,rucio_mode = args
        if running_mode == RunningMode.ONLY_DBS:
            #Not necessary when the level is file
            if did_level != 'file':
                submit_list_generation_job(did_level, input_file,rucio_mode=rucio_mode)
            dbs_invalidation(did_level, dids, dry_run)
        elif running_mode == RunningMode.ONLY_RUCIO:
            try:
                submit_list_generation_job(did_level, input_file,rucio_mode=rucio_mode)
                rucio_invalidation(did_level, dids, reason, dry_run=dry_run, erase_mode=erase_mode)
            except subprocess.CalledProcessError as e:
                logging.error("Error running shell script:")
                logging.error(e.stderr)
        else:
            try:
                submit_list_generation_job(did_level, input_file,rucio_mode=rucio_mode)
                logging.info("Submitted lists correctly")
                dbs_invalidation(did_level, dids, dry_run=dry_run)
                logging.info("DBS invalidation was performed correctly")
                rucio_invalidation(did_level, dids, reason, dry_run=dry_run, erase_mode=erase_mode)
                logging.info("Rucio invalidation was performed correctly")
            except subprocess.CalledProcessError as e:
                logging.info("Error running shell script:")
                logging.info(e.stderr)
            except Exception as e:
                logging.info("Error running invalidation script:")
                logging.info(e)