
import os
import sys
import json
import time
from rucio.client import Client
from argparse import ArgumentParser

from src.utils.colors import bcolors, printC
from src.utils.prepare_transfer import get_src_and_dst_list
from src.utils.fts_transfer import get_context, check_status, do_transfer
from src.utils.register_files import register_temp_replicas
from src.utils.register_blocks_and_containers import create_block_file_map, register_blocks, register_dataset
from src.utils.create_rule import create_rule


FTS_ENDPOINT = "https://fts3-cms.cern.ch:8446/"

user_proxy = os.environ["X509_USER_PROXY"]

if __name__ == "__main__":

    parser = ArgumentParser(prog='Rucio user file registration')
    parser.add_argument("upload_filename", help="json file with dataset name and filename map")
    parser.add_argument("options_filename", help="json file containing options for the upload operation")
    parser.add_argument("-o", "--overwrite", action='store_true')
    parser.add_argument("-d", "--dry-run", action='store_true')
    args = parser.parse_args()

    dry_run = args.dry_run
    overwrite = args.overwrite

    # TODO: do we need to validate the json schema?
    try:
        with open(args.options_filename, "r") as f:
            options = json.load(f)

        account_options = options["account"]
        upload_options = options["upload"]
        rule_options = options["rule"]
    except KeyError as e:
        print(f"Missing either of account, upload or rule options in the provided {args.options_file_name} file.")
    except Exception as e:
        print(e)

    with open(args.upload_filename, "r") as f:
        dataset_files_map = json.load(f)

    # Transfer files from source to temp rses

    for dataset, files in dataset_files_map.items():
        lfn_src_dst = get_src_and_dst_list(files, upload_options, account_options)

        src_dst = [(src, dst) for lfn, src, dst in lfn_src_dst]
        fts_context = get_context(FTS_ENDPOINT, user_proxy)
        job_id = do_transfer(fts_context, src_dst, account_options, dry_run=dry_run)
        if not dry_run:
            job_state = check_status(fts_context, job_id)["job_state"]
            print("Transferring files ....", end='')
            while job_state not in ["FINISHED", "FINISHEDDIRTY", "FAILED"]:
                time.sleep(60)
                job_status = check_status(fts_context, job_id)
                job_state = job_status["job_state"]
                print('.', end='')

            print(f"\n Tranfer Job {job_state}")

        raccount = account_options["username"]
        rscope = f'{account_options["account_type"]}.{account_options["username"]}'
        rclient = Client(account=raccount)
        files_per_block = upload_options["files_per_block"]
        temp_rse = upload_options["dest_temp_rse"]

        lfns = register_temp_replicas(client=rclient, file_src_dst_list=lfn_src_dst,
                                      scope=rscope, rse=temp_rse,
                                      dry_run=dry_run)

        # 3. Create blocks for registered files and attach them

        block_file_map = create_block_file_map(dataset, lfns, files_per_block)

        register_blocks(client=rclient, scope=rscope, block_file_map=block_file_map, dry_run=dry_run)
        register_dataset(client=rclient, scope=rscope, name=dataset, blocks=block_file_map.keys(), dry_run=dry_run)

        # 4. Create Rules

        rule_did = {"scope": rscope, "name": dataset}
        rule_id = create_rule(dids=[rule_did], options=rule_options, dry_run=dry_run)
        rule_id = rule_id[0]
        printC(f"Rule created for dataset: {dataset}", bcolors.OKGREEN)
        printC(f"https://cms-rucio-webui.cern.ch/rule?rule_id={rule_id}", bcolors.OKBLUE)
