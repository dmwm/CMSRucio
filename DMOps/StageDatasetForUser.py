#! /usr/bin/env python

from __future__ import print_function

import argparse

from rucio.client import Client
from rucio.common.exception import DuplicateRule

# Fix Python 2.x.
try:
    input = raw_input
except NameError:
    pass

RSE_EXPRESSION = 'ddm_quota>0&tier=2&rse_type=DISK'
WEIGHT = 'ddm_quota'

rucio = Client()

parser = argparse.ArgumentParser(description='Stage a dataset for CRAB user')
parser.add_argument('container', action='store',
                    help='the containers (CMS datasets) to stage - comma separated')
parser.add_argument('user', action='store',
                    help='the user name or email (used in the comment)')

args = parser.parse_args()

containers = args.container.split(',')

for container in containers:

    blocks = rucio.list_content(scope='cms', name=container)

    bytes = 0
    on_disk_bytes = 0
    for block in blocks:

        block_replicas = rucio.list_dataset_replicas(scope=block['scope'], name=block['name'], deep=True)
        block_bytes = 0
        disk_block_bytes = 0
        for replica in block_replicas:
            block_bytes = replica['bytes']
            if replica['state'] == 'AVAILABLE' and '_Tape' not in replica['rse']:
                disk_block_bytes = replica['available_bytes']

        bytes += block_bytes
        on_disk_bytes += disk_block_bytes

    print('Dataset is %9.3f TB with %9.3f TB not on disk.' % (bytes / 1e12, (bytes - on_disk_bytes) / 1e12))

    yes_no = input('Would you like to make the rule? ')

    if yes_no not in ['y', 'Y']:
        continue

    if bytes / 1.e12 > 30.: # switch from dataset level to block level
        grouping = 'DATASET'
    else:
        grouping = 'ALL'

        
    dids = [{'scope': 'cms', 'name': container}]

    days = 14 * 24 * 3600

    try:
        rules = rucio.add_replication_rule(dids=dids, copies=1, rse_expression=RSE_EXPRESSION, weight=WEIGHT, grouping=grouping,
                                           lifetime=days, account='crab_tape_recall', activity='Analysis Input',
                                           comment='Staged from tape for %s' % args.user, ask_approval=False,
                                           asynchronous=True,
                                           )

        rule = rules[0]

        print("Rule %s has been created for %s" % (rule, args.user))
        print("This rule can be monitored through DAS by checking the dataset or directly through Rucio")
        print("with 'rucio rule-info %s' " % rule)

        rule_info = rucio.get_replication_rule(rule)

        print('This rule expires at %s after which the data may be removed if not used occassionally'
              % rule_info['expires_at'])
    except DuplicateRule:
        print('An exact match for the rule on %s already exists. No new rule made.' % container)
