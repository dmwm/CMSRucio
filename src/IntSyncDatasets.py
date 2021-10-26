#! /usr/bin/env python3
import json
import logging

from rucio.client import Client
from rucio.common.exception import (DataIdentifierAlreadyExists, DuplicateContent, DuplicateRule, FileAlreadyExists,
                                    RSENotFound, DataIdentifierNotFound)

INPUT_RSE = '%s_Input'
INT_RSE = '%s'
SCOPE = 'cms'


def sync_block(rcp, rci, name, destinations=None):
    destinations = destinations or []

    block_dids = [{'scope': 'cms', 'name': name}]
    replicas = rcp.list_replicas(dids=block_dids)
    for replica in replicas:
        adler32 = replica['adler32']
        lfn = replica['name']
        file_size = replica['bytes']
        all_rses = replica['rses'].keys()
        states = replica['states']
        rses = [(INPUT_RSE % rse) for rse in all_rses if states[rse] == 'AVAILABLE']
        file_dids = [{'scope': 'cms', 'name': lfn}]
        # rci.add_did(scope=SCOPE, name=lfn, type='FILE')
        # try:
        #     rci.attach_dids(scope=SCOPE, name=name, dids=file_dids)
        # except DuplicateContent:
        #     logging.debug('File already attached')
        new_replicas = [{'scope': SCOPE, 'name': lfn, 'adler32': adler32, 'bytes': file_size, 'state': 'A'}]
        for rse in rses:
            # pdb.set_trace()
            try:
                result = rci.add_replicas(rse=rse, files=new_replicas)
                print(rse, new_replicas, result)
            except RSENotFound:
                print('Source RSE %s not found. No replica made.' % rse)
        try:
            rci.add_files_to_dataset(scope=SCOPE, name=name, files=file_dids)
        except FileAlreadyExists:
            print('%s already existed' % lfn)
        except DataIdentifierNotFound:
            print('%s not found' % lfn)



if __name__ == '__main__':
    rci = Client(rucio_host='http://cms-rucio-int.cern.ch', auth_host='https://cms-rucio-auth-int.cern.ch',
                 account='root')
    rcp = Client(rucio_host='http://cms-rucio.cern.ch', auth_host='https://cms-rucio-auth.cern.ch',
                 account='ewv')

    with open('int_wmcore_datasets.json', 'r') as wmcore_file:
        containers = json.load(wmcore_file)

    for container in containers:
        name = container['name']
        if '#' in name:  # This is already a block, make the container
            block_names = [container['name']]
            container_name, _ = container['name'].split('#')
            try:
                rci.add_did(scope=SCOPE, name=container_name, type='CONTAINER')
            except DataIdentifierAlreadyExists:
                logging.debug('Container already existed')
        else:
            blocks = rcp.list_content(scope=SCOPE, name=name)
            try:
                rci.add_did(scope=SCOPE, name=name, type='CONTAINER')
            except DataIdentifierAlreadyExists:
                logging.debug('Container already existed')
            block_names = sorted([block['name'] for block in blocks])
            if 'nblocks' in container:
                block_names = block_names[:container['nblocks']]
        for block_name in block_names:
            block_dids = [{'scope': 'cms', 'name': block_name}]
            try:
                rci.add_did(scope=SCOPE, name=block_name, type='DATASET')
            except DataIdentifierAlreadyExists:
                logging.debug('Block already existed')
            try:
                rci.attach_dids(scope=SCOPE, name=name, dids=block_dids)
            except DuplicateContent:
                logging.debug('Block already attached')
            except DataIdentifierNotFound:
                print('DID not found %s' % block_name)
            sync_block(rcp=rcp, rci=rci, name=block_name)  # destinations=container['destinations']
            for destination in container.get('destinations', ['T1_US_FNAL_Disk', 'T2_CH_CERN']):
                dest_rse = INT_RSE % destination
                try:
                    rci.add_replication_rule(block_dids, 1, dest_rse, account='transfer_ops')
                except DuplicateRule:
                    print('Rule already made for %s at %s' % (name, dest_rse))
