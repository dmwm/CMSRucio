#! /usr/bin/env python


"""
returns replicas list
usage: python list_replicas.py --dataset /Neutrino_E-10_gun/RunIISummer19ULPrePremix-UL18_106X_upgrade2018_realistic_v11_L1v1-v2/PREMIX#0c6e569f-28a9-4a3e-9fa7-cc87af997fbb 
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
        self.parser = argparse.ArgumentParser(prog='list_replicas')
        self.parser.add_argument("--lfn", help="provide LFN", action="store", dest="lfn")
        #self.parser.add_option("--container", help="provide container")
        self.parser.add_argument("--dataset", help="provide dataset", action="store", dest="dataset")

def get_list_of_rses(replica,rses):
    rse_with_state = replica['rse'] + " " + replica['state']
    rses.append(rse_with_state)

def get_replicas(args):
    SCOPE = 'cms'
    if args.lfn:
        name = args.lfn
        did = [{'scope': SCOPE, 'name': name}]
    elif args.dataset:
        name = args.dataset
    try:
        if args.lfn:
            replicas = client.list_replicas(dids=did, all_states=True)
            for replica in replicas:
                print("lfn: ", name, replica['states'])
        if args.dataset:
            rses = []
            replicas = client.list_dataset_replicas(scope=SCOPE, name=name)
            for replica in replicas:
                get_list_of_rses(replica, rses)
            print("dataset:", name, rses)
        #if args.container:
        #    print("container:", name)
                
    except RucioException as err:
        print(err)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    if args.lfn == None and args.dataset == None:
        print('Please specify dataset with --dataset or container with --container or lfn with --lfn')
        sys.exit(2)
    get_replicas(args)

