"""
File        : invalidate_rucio.py
Author      : Andres Manrique <andres.manrique AT cern [DOT] ch>
Description : Declare replicas as bad to force declaring them as lost (Rucio invalidation for invalid DBS files which are valid on Rucio)
              Erase empty datasets and containers from Rucio
              Delete rules of empty datasets or empty containers.
              Stuck suspended rules of invalid DBS files that are valid on Rucio (force rule re-evaluation)

"""

import asyncio
import os
import logging
import pandas as pd
import argparse
from rucio.client import Client

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

def validate_file_exists(file_path):
    """
    Validate if a file exists at the given file path.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The valid file path.

    Raises:
        argparse.ArgumentTypeError: If the file does not exist.

    """
    if not os.path.exists(file_path):
        raise argparse.ArgumentTypeError(f"The file '{file_path}' does not exist.")
    return file_path

def validate_arguments():
    """
    Validate the command line arguments and parse them to obtain the required files and parameters.

    Parameters:
        None

    Returns:
        replicas (pandas.DataFrame): A DataFrame containing the replicas data.
        datasets (list): A list of datasets to be deleted.
        containers (list): A list of containers to be deleted.
        rules (list): A list of rules to be deleted.
        dry_run (bool): A flag indicating if the script should be run in dry-run mode.

    Raises:
        ValueError: If the datasets list is empty when attempting to delete containers.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('replicas', type=validate_file_exists, help='File containing the replicas of files invalid on DBS and valid on Rucio.  File with CSVformat: FILENAME,RSE',
                        metavar='REPLICAS_FILE')
    parser.add_argument('--datasets', type=validate_file_exists, help='File containing the list of empty datasets (no files associated to them)')
    parser.add_argument('--containers', type=validate_file_exists, help='File containing the list of empty containers (no files associated to them)')
    parser.add_argument('--rules-delete', type=validate_file_exists, help='Rules associated to the empty datasets, empty containers or lost files. Avoid [0/0/0] rules')
    parser.add_argument('--rules-stuck', type=validate_file_exists, help='Suspended Rules associated with lost files that requires to be re-evaluated') #Used for rules with lost files, but  all the rule files
    parser.add_argument('--dry-run', action='store_true', help='Test the script without deleting anything')
    parser.add_argument('--erase-mode', action='store_true', help='Erase empty datasets and containers')
    parser.add_argument('--reason', type=str, help='Comment for the deletion')

    args = parser.parse_args()
    replicas_df = pd.DataFrame()
    datasets = []
    containers = []
    rules_delete_df = pd.DataFrame()
    rules_stuck = []

    #Replicas to declare as bad
    if args.replicas:
        replicas_df = pd.read_csv(args.replicas)
        if not set(['FILENAME', 'RSES']).issubset(replicas_df.columns):
            raise ValueError('File csv file must have the following columns: FILENAME, RSES')

    #Check the reason exists
    if args.reason is None or len(args.reason) == 0:
        raise ValueError("Reason for deletion is required.")

    if  args.erase_mode:
        #Empty to datasets to erase from Rucio. They are deleted to avoid create rules [0/0/0]
        if args.datasets:
            with open(args.datasets,'r') as f:
                datasets = f.readlines()
                datasets = [x.strip() for x in datasets]

        #Empty to containers to erase from Rucio. It shouldn't be possible to delete containers without datasets (prevent ghosts). They are deleted to avoid create rules [0/0/0]
        if args.containers:
            with open(args.containers,'r') as f:
                containers = f.readlines()
                containers = [x.strip() for x in containers]
            if len(datasets) == 0 and len(containers) > 0:
                raise ValueError('Datasets list cannot be empty deleting containers')

    #Check if the file with the rules to delete exists. If empty nothing will happen
    #Empty rules are deleted to avoid empty rules [0/0/0]
    if args.rules_delete:
        rules_delete_df = pd.read_csv(args.rules_delete)
        if not set(['RULE_ID','RSE']).issubset(rules_delete_df.columns):
            raise ValueError('File csv file must have the following columns: RULE_ID,RSE')

    #Check if the file with the rules to stuck exists. If empty nothing will happen
    if args.rules_stuck:
        with open(args.rules_stuck,'r') as f:
            rules_stuck = f.readlines()
            rules_stuck = [x.strip() for x in rules_stuck]

    return replicas_df, datasets, containers, rules_delete_df, rules_stuck, args.reason,args.dry_run

async def replicas_worker(queue, reason, dry_run):
    """
    Asynchronously processes files from a queue and declares them as bad if necessary.

    Args:
        queue (asyncio.Queue): The queue containing files to be processed.
        dry_run (bool): Whether to perform a dry run or not.

    Returns:
        None
    """
    client = Client()

    while True:
        record = await queue.get()
        file = record[0]
        rses = record[1].split(';')
        dids = []
        for rse in rses:
            dids.append({'scope':'cms', 'name':file, 'rse': rse})
        try:
            if not dry_run:
                # Force in case replicas was already declared as bad in the past
                client.declare_bad_file_replicas(dids, reason=reason, force=True)
                logging.info("Declared file %s as bad at %s" % (file, rses))
            else:
                logging.info("Would declare file %s as bad at %s" % (file, rses))
        except Exception as e:
            logging.error("Error declaring file %s as bad at %s: %s" % (file, rses, e))
        queue.task_done()

async def stuck_rules_worker(queue, reason, dry_run):
    """
    Asynchronously unlocks and stuck rules from a queue. This is used for the suspended rules

    Args:
        queue (asyncio.Queue): The queue containing the stuck rules.
        dry_run (bool): Whether to perform a dry run or not.

    Returns:
        None
    """

    client = Client()
    while True:
        rule = await queue.get()
        try:
            if not dry_run:
                client.update_replication_rule(rule, {'state':'STUCK', 'comment': reason})
                logging.info("Re-stuck Rule %s" % (rule))
            else:
                logging.info("Would re-stuck Rule %s" % (rule))
        except Exception as e:
            logging.error("Error set rule to stuck %s: %s" % (rule, e))
        queue.task_done()

async def delete_rules_worker(queue, reason, dry_run):
    """
    Asynchronously deletes replication rules from the queue.

    Args:
        queue (asyncio.Queue): The queue containing the replication rules to be deleted.
        dry_run (bool): If True, the rules will not actually be deleted, but the deletion will be logged.

    Returns:
        None
    """

    client = Client()
    while True:
        record = await queue.get()
        rule = record[0]
        rse_expression = record[1]
        try:
            if not dry_run:
                client.update_replication_rule(rule_id=rule, options={'locked': False, 'comment': reason})
                purge_replicas = 'Tape' in rse_expression
                client.delete_replication_rule(rule_id=rule,purge_replicas=purge_replicas)
                logging.info("Deleted rule %s" % (rule))
            else:
                logging.info("Would Delete rule %s" % (rule))
        except Exception as e:
            logging.error("Error updating rule %s: %s" % (rule, e))
        queue.task_done()

async def erase_dids_worker(queue, dry_run):
    """
    Erases emptyDIDs from the queue.

    Args:
        queue (asyncio.Queue): The queue containing the DIDs to be erased.
        dry_run (bool): If True, the DIDs will not be erased. Instead, information about the DIDs that would be deleted is logged.

    Returns:
        None
    """
    client = Client()
    while True:
        did = await queue.get()
        try:
            if not dry_run:
                #Equivalent to rucio erase
                client.set_metadata(scope='cms', name=did, key='lifetime', value=86400)
                logging.info("Deleted did %s" % (did))
            else:
                logging.info("Would delete did %s" % (did))
        except Exception as e:
            logging.error("Error deleting did %s: %s" % (did, e))
        queue.task_done()

async def main(replicas_df,reason,rules_stuck=[], rules_delete=pd.DataFrame(),datasets=[],containers=[],dry_run=False):
    """
    Asynchronously executes tasks to process replicas, datasets, and containers.

    :param replicas_df: The DataFrame containing replica information.
    :param datasets: A list of datasets to process.
    :param containers: A list of containers to process.
    :param rules_delete: A list of rules to delete.
    :param rules_stuck: A list of stuck rules.
    :param dry_run: A flag indicating whether to perform a dry run.
    :return: None
    """
    loop = asyncio.get_event_loop()

    #Start delete rules worker
    if len(rules_delete) > 0:
        num_workers_rules = 10
        queue_rules = asyncio.Queue()
        workers_rules_delete = [loop.create_task(delete_rules_worker(queue_rules, reason, dry_run)) for _ in range(num_workers_rules)]
        for rule_id,rse_expression in rules_delete[['RULE_ID','RSE']].values:
            await queue_rules.put((rule_id,rse_expression))

        await queue_rules.join()
        for worker_task in workers_rules_delete:
            worker_task.cancel()
    # Start bad replicas workers
    if len(replicas_df) > 0:
        num_workers_replicas = 30
        queue_replicas = asyncio.Queue()
        workers_files = [loop.create_task(replicas_worker(queue_replicas,reason, dry_run)) for _ in range(num_workers_replicas)]
        for file, rse in replicas_df[['FILENAME','RSES']].values:
            await queue_replicas.put((file, rse))

        await queue_replicas.join()
        for worker_task in workers_files:
            worker_task.cancel()


    #Start delete dids worker
    if len(containers) > 0 or len(datasets) > 0:
        queue_dids = asyncio.Queue()
        num_workers_dids = 20
        workers_dids = [loop.create_task(erase_dids_worker(queue_dids,dry_run)) for _ in range(num_workers_dids)]
        for dataset in datasets:
            await queue_dids.put(dataset)
        for container in containers:
            await queue_dids.put(container)

        await queue_dids.join()
        for worker_task in workers_dids:
            worker_task.cancel()

    #Start stuck rules worker
    if len(rules_stuck) > 0:
        num_workers_stuck_rules = 10
        queue_stuck_rules = asyncio.Queue()
        queue_rules_stuck = [loop.create_task(stuck_rules_worker(queue_stuck_rules, reason, dry_run)) for _ in range(num_workers_stuck_rules)]
        for rule in rules_stuck:
            await queue_stuck_rules.put(rule)

        await queue_stuck_rules.join()
        for worker_task in queue_rules_stuck:
            worker_task.cancel()


if __name__ == '__main__':
    replicas_df, datasets, containers, rules_delete_df, rules_stuck, reason ,dry_run = validate_arguments()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(replicas_df,reason,rules_delete_df, rules_stuck, datasets, containers,dry_run))