#! /usr/bin/env python


"""
approve/deny rule
usage: python update_rule.py --action allow --id 98770dd6e72142d1b728cd0f95ac54e3 
"""

from __future__ import division, print_function
from rucio.client.client import Client
from rucio.common.exception import RuleNotFound
import argparse

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='update_rule_status')
        self.parser.add_argument("--action", help="specify action approve/deny", required=True, action="store", dest="action")
        self.parser.add_argument("--id", help="provide rule id", required=True, action="store", dest="id")

def update_rule(args):
    print(args)
    try:
        if args.action == "allow":
            approve_rule = client.approve_replication_rule(rule_id=args.id)
            print(approve_rule)
        elif args.action == "deny":
            deny_rule = client.deny_replication_rule(rule_id=args.id)  
            print(deny_rule) 
    except RuleNotFound as err:
        print(err)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    update_rule(args)
