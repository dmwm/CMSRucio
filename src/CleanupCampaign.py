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

rule_gen = client.list_subscription_rules(account=ACCOUNT, name=SUBSCRIPTION)

rules = [rule for rule in rule_gen]  # Getting chunking errors if we wait 

for rule in rules:
    rule_id = rule['id']
    dataset = rule['name']
    expression = rule['rse_expression']

    print('Cleanup up rule %s (%s) on %s' % (rule_id, expression, dataset))
    client.delete_replication_rule(rule_id=rule_id)
