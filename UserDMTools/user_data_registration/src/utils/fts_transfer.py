import os
import argparse
import logging
import json
import fts3.rest.client.easy as fts3

FTS_MONITORING = "https://fts3-cms.cern.ch:8449/"


def do_transfer(context, src_dst_list, account_options, dry_run=False):

    username = account_options["username"]
    account_type = account_options["account_type"]
    # Build the job
    transfers = []
    for src, dst in src_dst_list:
        transfer = fts3.new_transfer(src, dst, metadata=f"User data subscription transfer by {account_type} {username}")
        transfers.append(transfer)

    job = fts3.new_job(
        transfers, verify_checksum=True, overwrite=True,
        metadata=f"User data subscription transfer Job by {account_type} {username}", retry=3, priority=3)

    # Submit or just print
    if dry_run:
        print("Dry Run: ", dry_run)
        # print(json.dumps(job, indent=2))
        job_id = "dry_job"
    else:
        print("Submitting job ....")
        job_id = fts3.submit(context, job)
        print(f"{FTS_MONITORING}/fts3/ftsmon/#/job/{job_id}")

    return job_id


def check_status(context, job_id):
    return fts3.get_job_status(context, job_id)


def get_context(FTS_ENDPOINT, user_proxy):
    return fts3.Context(FTS_ENDPOINT, user_proxy, user_proxy, verify=True)


def run():
    context = get_context()
    do_transfer(context, src_dst_list, dry_run)


if __name__ == "__main__":

    logging.getLogger('fts3.rest.client').setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        prog='FTS Bulk Transfer Builder',
        description='Submits fts jobs for a list of files',
        epilog='Hooorayy!!!')

    parser.add_argument('source')
    parser.add_argument('destination')
    parser.add_argument('-s', '--endpoint', dest='endpoint', default='https://fts3-pilot.cern.ch:8446')
    parser.add_argument('--dry-run', dest='dry_run', default=False, action='store_true')
    parser.add_argument('--multifile-test', default=False, action='store_true')

    args = parser.parse_args()
    source = args.source
    destination = args.destination
    dry_run = args.dry_run

    src_dst_list = [(source, destination)]

    if args.multifile_test:
        src_dst_list = [
            ('davs://deepthought.crc.nd.edu:1094/store/mc/Phase2HLTTDRSummer20ReRECOMiniAOD/TTToSemiLepton_TuneCP5_14TeV-powheg-pythia8/FEVT/PU200_111X_mcRun4_realistic_T15_v1-v1/120000/A0037171-FD6D-8B4E-A8D2-71CB77893C4A.root',
                'davs://deepthought.crc.nd.edu:1094/store/temp/user/rucio/rchauhan/mc/Phase2HLTTDRSummer20ReRECOMiniAOD/TTToSemiLepton_TuneCP5_14TeV-powheg-pythia8/FEVT/PU200_111X_mcRun4_realistic_T15_v1-v1/120000/A0037171-FD6D-8B4E-A8D2-71CB77893C4A.root'),
            ('davs://deepthought.crc.nd.edu:1094/store/mc/RunIISummer20UL17NanoAODv9/GluGluToContinToZZTo2e2tau_TuneCP5_13TeV-mcfm701-pythia8/NANOAODSIM/106X_mc2017_realistic_v9-v2/270000/701A10AD-FC1D-E145-A402-D0209482A5A6.root',
                'davs://deepthought.crc.nd.edu:1094/store/temp/user/rucio/rchauhan/mc/RunIISummer20UL17NanoAODv9/GluGluToContinToZZTo2e2tau_TuneCP5_13TeV-mcfm701-pythia8/NANOAODSIM/106X_mc2017_realistic_v9-v2/270000/701A10AD-FC1D-E145-A402-D0209482A5A6.root')
        ]

    run()
