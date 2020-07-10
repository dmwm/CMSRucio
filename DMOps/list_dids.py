#! /usr/bin/env python


"""
returns DIDs list
usage: python list_dids.py --did /*/Run2016B-02Apr2020-v*/NANOAOD --type container       
List all data identifiers in a scope which match a given pattern.
:param type: The type of the did: 'all'(container, dataset or file)|'collection'(dataset or container)|'dataset'|'container'|'file' 
"""

from __future__ import division, print_function
from rucio.client.client import Client
from rucio.common.exception import RucioException
import argparse
import sys

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='list_did_content')
        self.parser.add_argument("--did", help="provide did", action="store", dest="did")
        self.parser.add_argument("--type", help="provide did type", action="store", dest="type")
        self.parser.add_argument("--recursive", help="True/False", action="store", dest="recursive", type=bool)

def get_replicas(args):
    try:
        did_list = client.list_dids(scope="cms", recursive=args.recursive ,type=args.type, filters={'name': args.did})
        for did in did_list:
            print("cms:" + did) 
    except RucioException as err:
        print(err)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    get_replicas(args)

