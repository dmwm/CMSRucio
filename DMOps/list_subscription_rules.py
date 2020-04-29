#! /usr/bin/env python


"""
returns list of subscription rules with status
usage: python list_subscription_rules.py --account transfer_ops --name Placement_NanoAODv5 --rse MIT
"""

from __future__ import division, print_function
from rucio.client.client import Client
from rucio.common.exception import RucioException
import argparse

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='list_subscription_rules')
        self.parser.add_argument("--account", help="Specify account type", default="transfer_ops", action="store", dest="account")        
        self.parser.add_argument("--name", help="Subscription name required", action="store", dest="name", required=True)
        self.parser.add_argument("--rse", action="store", dest="rse")

def get_subscription_rules(args):
    try:
        rules_list = client.list_subscription_rules(account=args.account, name=args.name)
        rules = [rule for rule in rules_list]
        print('Cleaning up %s rules' % len(rules))
        for rule in rules:
            rule_status = ('{"id" : %s ,  "rse_expression" : %s , "name" : %s , "state" : %s , "stuck_at" : %s}' %(rule['id'], rule['rse_expression'], rule['name'], rule['state'], rule['stuck_at']))
            if args.rse:
                if args.rse in rule_status:
                    print(rule_status)
            else:
                print(rule_status)
    except RucioException as err:
        print(err)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    get_subscription_rules(args)
