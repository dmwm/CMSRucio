#! /usr/bin/env python


"""
A bit of code to check on the progress of the million file test
"""

from __future__ import division, print_function

from rucio.client.client import Client
from requests.exceptions import ChunkedEncodingError
import pdb

client = Client()
client.whoami()

ACCOUNT = 'transfer_ops'
SUBSCRIPTION = 'Placement_NanoAODv4'
# rucio list-rules --subscription transfer_ops Placement_NanoAODv4

rules = client.list_subscription_rules(account=ACCOUNT, name=SUBSCRIPTION)

for rule in rules:
    rule_id = rule['id']
    dataset = rule['name']
    expression = rule['res_expression']

    print('Cleanup up rule %s (%s) on %s' % (rule_id, expression, dataset))
    try:
        client.delete_replication_rule(rule_id=rule_id)
    except ChunkedEncodingError:
        print(' Got a ChunkedEncodingError exception')
        