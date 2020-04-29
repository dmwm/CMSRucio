#! /usr/bin/env python


"""
returns list of stuck replicas
"""

from __future__ import division, print_function
from rucio.client.client import Client
from rucio.common.exception import AccountNotFound,RSENotFound
import argparse
import datetime
import time

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='list_stuck_rules')
        self.parser.add_argument("--rse", action="store", dest="rse")
        self.parser.add_argument("--past_days",action="store",dest="days",default=7,type=int)

def get_rules_by_account(acc, rse, delta_t):
    acc_rules = client.list_account_rules(acc) 
    for rule in acc_rules:
        if (time.mktime(rule['created_at'].timetuple()) < delta_t):
            if rule['state'] != "OK" and rule['rse_expression'] == rse:
                print(rule['id'], rule['name'], rule['error'], rule['created_at'])
            elif rule['state'] != "OK":
                print(rule['id'], rule['name'], rule['error'], rule['created_at'])

def get_stuck_rules(args):
    current = time.time()
    past_n_days = args.days
    delta_t = current - past_n_days*60*60*24
    try:
        if args.rse:
            rse = client.get_rse(args.rse)
        #accounts_list = client.list_accounts(account_type=None, identity=None, filters=None)
        get_rules_by_account("transfer_ops", args.rse, delta_t)
        get_rules_by_account("root", args.rse, delta_t)
    except (AccountNotFound,RSENotFound) as err:
        print(err)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    get_stuck_rules(args)
