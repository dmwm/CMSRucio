#! /usr/bin/env python


"""
A bit of code to check on the progress of the million file test
"""

from __future__ import division, print_function

from rucio.client.client import Client
import pdb

client = Client()
client.whoami()

ACCOUNT='transfer_ops'
SUBSCRIPTION =  'Placement_NanoAODv4'
#rucio list-rules --subscription transfer_ops Placement_NanoAODv4

rules = client.list_subscription_rules(account=ACCOUNT, name=SUBSCRIPTION)


pdb.set_trace()
