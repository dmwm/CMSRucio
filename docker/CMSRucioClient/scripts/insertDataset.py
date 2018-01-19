#! /bin/env python

from __future__ import absolute_import, division, print_function

import json
import subprocess
import uuid

from rucio.client.accountclient import AccountClient
from rucio.client.didclient import DIDClient
from rucio.client.lifetimeclient import LifetimeClient
from rucio.client.replicaclient import ReplicaClient
from rucio.client.ruleclient import RuleClient
from rucio.common.exception import DataIdentifierAlreadyExists, RucioException

DUMMY_RSE = 'T2_US_UCSD'

DAYS_TO_LIVE = 300

PATTERN = 'd'  # This replaces slashes to allow us to pass schema rules, but also allows for a new attempt at something

DATASET = '/SingleMuon/Run2017A-PromptReco-v2/MINIAOD'

DAS = ['dasgoclient', '-json']
DAS_BLOCK = '-query=block dataset=%s'
DAS_FILE_DBS = '-query=file block=%s detail=true'
DAS_FILE_PHEDEX = '-query=file block=%s system=phedex'

COMMAND = ['dasgoclient', '-query=file dataset=%s system=phedex' % DATASET, '-json']

RUCIO_DS = DATASET.replace('/', '', 1).replace('/', PATTERN) + '_DS'  # Remove /
RUCIO_CONTAINER = DATASET.replace('/', '', 1).replace('/', PATTERN)  # Remove /
RUCIO_DS = DATASET + '_DS'  # Remove /
RUCIO_CONTAINER = DATASET


def dbs_info_for_file(filename='', dbs_files=None):
    if dbs_files is None:
        dbs_files = []

    size = 0
    adler32 = None
    n_events = 0
    for dbs_file in dbs_files:
        if dbs_file['file'][0]['name'] == filename:
            size = dbs_file['file'][0]['size']
            adler32 = dbs_file['file'][0]['adler32']
            n_events = dbs_file['file'][0]['nevents']
            break

    return size, adler32, n_events


if __name__ == '__main__':

    aClient = AccountClient(account='ewv', auth_type='x509_proxy')
    dClient = DIDClient(account='ewv', auth_type='x509_proxy')
    rClient = ReplicaClient(account='ewv', auth_type='x509_proxy')
    lClient = LifetimeClient(account='ewv', auth_type='x509_proxy')
    ruleClient = RuleClient(account='ewv', auth_type='x509_proxy')

    print("Connected to rucio as %s" % aClient.whoami()['account'])

    # Make Rucio container and dataset to correspond to CMS dataset

    try:
        status = dClient.add_container(scope='user.ewv', name=RUCIO_CONTAINER, lifetime=DAYS_TO_LIVE*24*3600)
        print('Status for add_container', status)
    except DataIdentifierAlreadyExists:
        print('Container already exists')

    try:
        status = dClient.add_dataset(scope='user.ewv', name=RUCIO_DS, lifetime=DAYS_TO_LIVE*24*3600)
        print('Status for add_dataset', status)
    except DataIdentifierAlreadyExists:
        print('Dataset already exists')

    try:
        dasOutput = subprocess.check_output(DAS + [DAS_BLOCK % DATASET])
    except subprocess.CalledProcessError as ex:
        print(ex.output)
    blocks = json.loads(dasOutput)

    block_datasets = []
    for blockObj in blocks:
        block = blockObj['block'][0]['name']

        # Make Rucio dataset to correspond to CMS blocks. Attach this dataset to the container representing CMS dataset
        rucio_block_ds = block.replace('/', '', 1).replace('/', PATTERN)
        rucio_block_ds = block
        block_datasets.append({'scope':'user.ewv', 'name':rucio_block_ds})
        try:
            status = dClient.add_dataset(scope='user.ewv', name=rucio_block_ds, lifetime=DAYS_TO_LIVE*24*3600)
            print('Status for add_dataset', status)
        except DataIdentifierAlreadyExists:
            print('Dataset already exists')

        try:
            status = dClient.attach_dids(scope='user.ewv', name=RUCIO_CONTAINER,
                                         dids=[{'scope': 'user.ewv', 'name': rucio_block_ds}])
            print('Status for attach dataset', status)
        except RucioException:
            print("Attach faild, probabably already done.")

        print('Creating files for block %s' % block)

        phedex_files = json.loads(subprocess.check_output(DAS + [DAS_FILE_PHEDEX % block]))
        dbs_files = json.loads(subprocess.check_output(DAS + [DAS_FILE_DBS % block]).strip())

        replicas = []
        for fileDict in phedex_files:

            phedex_bytes = fileDict['file'][0]['bytes']
            adler32 = None
            for checksum in fileDict['file'][0]['checksum'].split(','):
                kind, value = checksum.split(':')
                if kind.lower() == 'adler32':
                    adler32 = value

            dbs_bytes, dbs_adler32, dbs_events = dbs_info_for_file(filename=fileDict['file'][0]['name'],
                                                                   dbs_files=dbs_files)

            if adler32 != dbs_adler32 or phedex_bytes != dbs_bytes:
                raise RuntimeError('Checksums or size do not match')

            replica = {'scope': 'user.ewv',
                       'name': fileDict['file'][0]['name'],
                       'bytes': phedex_bytes,
                       'meta': {'guid': str(uuid.uuid4()).upper(),
                                'events': dbs_events,
                                },  # Should be able to remove later (client cares now)
                       'adler32': adler32,
                       }

            replicas.append(replica)

        # Mark the replicas as at a site
        status = rClient.add_replicas(rse=DUMMY_RSE, files=replicas)
        print('Status for add_replicas', status)

        # Attach the files to the dataset representing the block and the single dataset
        try:
            status = dClient.attach_dids(scope='user.ewv', name=rucio_block_ds, dids=replicas)
            status = dClient.attach_dids(scope='user.ewv', name=RUCIO_DS, dids=replicas)
            print('Status for attach', status)
        except RucioException:
            print("Attach failed, probabably already done.")

    status = ruleClient.add_replication_rule(dids=block_datasets, copies=2, rse_expression='tier=2',
                                             lifetime=DAYS_TO_LIVE//2, account='ewv')

