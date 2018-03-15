#! /bin/env python

from __future__ import absolute_import, division, print_function

import json
import math
from itertools import islice
from subprocess import PIPE, Popen

from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.common.exception import DataIdentifierAlreadyExists, FileAlreadyExists, RucioException


def das_go_client(query):
    """
    just wrapping the dasgoclient command line
    """

    DEBUG_FLAG = False  # FIXME: Remove

    proc = Popen(['dasgoclient', '-query=%s' % query, '-json'], stdout=PIPE)
    output = proc.communicate()[0]
    if DEBUG_FLAG:
        print('DEBUG:' + output)
    return json.loads(output)


def grouper(iterable, n):  # FIXME: Pull this from WMCore/Utils/IteratorTools when we migrate
    """
    :param iterable: List of other iterable to slice
    :type: iterable
    :param n: Chunk size for resulting lists
    :type: int
    :return: iterator of the sliced list
    Source: http://stackoverflow.com/questions/3992735/python-generator-that-groups-another-iterable-into-groups-of-n
    """
    iterable = iter(iterable)
    return iter(lambda: list(islice(iterable, n)), [])


def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


def convert_size_si(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1000)))
    p = math.pow(1000, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


class CMSRucio(object):
    """
    Interface for Rucio with the CMS data model

    CMS         Rucio
    File/LFN    File
    Block       Dataset
    Dataset     Container

    We try to use the correct terminology on for variable and parameter names where the CMS facing code uses
    File/Block/Dataset and the Rucio facing code uses File/Dataset/Container
    """

    def __init__(self, account, auth_type, scope='cms', dry_run=False):
        self.account = account
        self.auth_type = auth_type
        self.scope = scope
        self.dry_run = dry_run

        self.didc = DIDClient(account=self.account, auth_type=self.auth_type)
        self.rc = ReplicaClient(account=self.account, auth_type=self.auth_type)

        pass

    def cmsBlocksInContainer(self, container, scope='cms'):

        block_names = []
        response = self.didc.get_did(scope=scope, name=container)
        if response['type'].upper() != 'CONTAINER':
            return block_names

        response = self.didc.list_content(scope=scope, name=container)
        for item in response:
            if item['type'].upper() == 'DATASET':
                block_names.append(item['name'])

        return block_names

    def getReplicaInfoForBlocks(self, scope='cms', dataset=None, block=None, node=None):  # Mirroring PhEDEx service

        """
        This mimics the API of a CMS PhEDEx function. Be careful changing it

        _blockreplicas_
        Get replicas for given blocks

        dataset        dataset name, can be multiple (*)
        block          block name, can be multiple (*)
        node           node name, can be multiple (*)
        se             storage element name, can be multiple (*)
        update_since  unix timestamp, only return replicas updated since this
                time
        create_since   unix timestamp, only return replicas created since this
                time
        complete       y or n, whether or not to require complete or incomplete
                blocks. Default is to return either
        subscribed     y or n, filter for subscription. default is to return either.
        custodial      y or n. filter for custodial responsibility.  default is
                to return either.
        group          group name.  default is to return replicas for any group.
        """

        block_names = []
        result = {'block': []}

        if isinstance(block, (list, set)):
            block_names = block
        elif block:
            block_names = [block]

        if isinstance(dataset, (list, set)):
            for dataset_name in dataset:
                block_names.extend(self.cmsBlocksInContainer(dataset_name, scope=scope))
        elif dataset:
            block_names.extend(self.cmsBlocksInContainer(dataset, scope=scope))

        for block_name in block_names:
            dids = [{'scope': scope, 'name': block_name} for block_name in block_names]

            response = self.rc.list_replicas(dids=dids)
            nodes = set()
            for item in response:
                for node, state in item['states'].items():
                    if state.upper() == 'AVAILABLE':
                        nodes.add(node)
            result['block'].append({block_name: list(nodes)})
        return result

    def dataset_summary(self, scope='cms', dataset=None):
        response = self.didc.list_files(scope=scope, name=dataset)
        summary = {'files': {}, 'dataset': dataset}
        dataset_bytes = 0
        dataset_events = 0
        dataset_files = 0
        files = []
        for fileobj in response:
            dataset_files += 1
            summary['files'].update({fileobj['name']: {
                'bytes': fileobj['bytes'],
                'events': fileobj['events'],
            }})
            files.append({'scope': scope, 'name': fileobj['name']})
            if fileobj['bytes']:
                dataset_bytes += fileobj['bytes']

            if fileobj['events']:
                dataset_events += fileobj['events']
        summary.update({'bytes': dataset_bytes, 'events': dataset_events, 'file_count': dataset_files})
        summary.update({'size': convert_size_si(dataset_bytes)})

        site_summary = {}

        for chunk in grouper(files, 1000):
            response = self.rc.list_replicas(dids=chunk)
            for item in response:
                lfn = item['name']
                for node, state in item['states'].items():
                    if state.upper() == 'AVAILABLE':
                        if node not in site_summary:
                            site_summary[node] = {'file_count': 0, 'bytes': 0, 'events': 0}
                        site_summary[node]['file_count'] += 1
                        if summary['files'][lfn]['bytes']:
                            site_summary[node]['bytes'] += summary['files'][lfn]['bytes']
                        if summary['files'][lfn]['events']:
                            site_summary[node]['events'] += summary['files'][lfn]['events']

        for node in site_summary:
            site_summary[node]['size'] = convert_size_si(site_summary[node]['bytes'])

        summary['sites'] = site_summary

        return summary

    def register_replicas(self, rse, replicas):
        """
        Register file replicas
        """

        if not replicas:
            return
        if self.dry_run:
            print(' Dry run only. Not registering files.')
            return

        if self.check:
            for filemd in replicas:
                self.check_storage(filemd)

        self.rc.add_replicas(rse=rse, files=[{'scope': self.scope, 'name': filemd['name'],
                                              'adler32': filemd['checksum'], 'bytes': filemd['size'],
                                              } for filemd in replicas])

    def register_dataset(self, block, dataset, lifetime=None):
        """
        Create the rucio dataset corresponding to a CMS block and attach it to the container (CMS dataset)
        """

        if self.dry_run:
            print(' Dry run only. Not creating dataset (CMS block %s).' % block)
            return

        try:
            self.didc.add_dataset(scope=self.scope, name=block, lifetime=lifetime)
        except DataIdentifierAlreadyExists:
            pass

        try:
            self.didc.attach_dids(scope=self.scope, name=dataset, dids=[{'scope': self.scope, 'name': block}])
        except RucioException:
            pass

    def register_container(self, dataset, lifetime):
        """
        Create a container (CMS Dataset)
        """

        if self.dry_run:
            print(' Dry run only. Not creating container (CMS dataset %s).' % dataset)
            return

        try:
            self.didc.add_container(scope=self.scope, name=dataset, lifetime=lifetime)
        except DataIdentifierAlreadyExists:
            pass

    def attach_files(self, lfns, block):
        """
        Attach the file to the container
        """
        if not lfns:
            return

        if self.dry_run:
            print(' Dry run only. Not attaching files to %s.' % block)
            return

        try:
            self.didc.attach_dids(scope=self.scope, name=block,
                                  dids=[{'scope': self.scope, 'name': lfn} for lfn in lfns])
        except FileAlreadyExists:
            pass
