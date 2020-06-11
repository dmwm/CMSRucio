#! /usr/bin/env python


"""
usage: python get_stuck_rules.py --state ALL --past_days 8 --rse PIC
rule states:
    REPLICATING = 'R', 'REPLICATING'
    OK = 'O', 'OK'
    STUCK = 'S', 'STUCK'
    SUSPENDED = 'U', 'SUSPENDED'
    WAITING_APPROVAL = 'W', 'WAITING_APPROVAL'
    INJECT = 'I', 'INJECT'
    ALL = 'ALL', ALL STUCK RULES
"""

from __future__ import division, print_function
from rucio.client.client import Client
from rucio.common.exception import RuleNotFound
import argparse
import datetime
import time

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='update_rule_status')
        self.parser.add_argument("--rse", help="specify RSE name", action="store", dest="rse")
        self.parser.add_argument("--state", help="specify rule state", required=True,action="store", dest="state")
        self.parser.add_argument("--past_days",action="store",dest="days",default=7,type=int) 

def list_rules(rse,rules,delta_t):
    if rse:
        for rule in rules:
            if (time.mktime(rule['created_at'].timetuple()) < delta_t):
                if rse in rule['rse_expression']:
                    print(rule['id'], rule['account'] ,rule['name'], rule['updated_at'], rule['rse_expression'])
    else:
        for rule in rules:
            if (time.mktime(rule['created_at'].timetuple()) < delta_t):
                print(rule['id'], rule['account'] ,rule['name'], rule['updated_at'], rule['rse_expression'])

def get_stuck_rules(args):
    current = time.time()
    past_n_days = args.days
    delta_t = current - past_n_days*60*60*24
    try:
        states = ['R','S','U', 'I', 'W']
        if args.state == 'ALL':
            for state in states:
                stuck_rules = client.list_replication_rules(filters={'state': state})
                list_rules(args.rse,stuck_rules,delta_t)
        else:
            stuck_rules = client.list_replication_rules(filters={'state': args.state})
            list_rules(args.rse,stuck_rules,delta_t)
    except RuleNotFound as err:
        print(err)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    get_stuck_rules(args)
