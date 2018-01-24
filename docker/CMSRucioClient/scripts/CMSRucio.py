#! /bin/env python

from __future__ import absolute_import, division, print_function

from pprint import pprint

from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient


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

    def __init__(self, account, auth_type):
        self.account = account
        self.auth_type = auth_type
        pass

    def cmsBlocksInContainer(self, container, scope='cms'):

        didClient = DIDClient(account=self.account, auth_type=self.auth_type)

        block_names = []
        response = didClient.get_did(scope=scope, name=container)
        if response['type'].upper() != 'CONTAINER':
            return block_names

        response = didClient.list_content(scope=scope, name=container)
        for item in response:
            if item['type'].upper() == 'DATASET':
                block_names.append(item['name'])

        return block_names

    def getReplicaInfoForBlocks(self, scope='cms', dataset=None, block=None, node=None):  # Mirroring PhEDEx service

        """
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

        rc = ReplicaClient(account=self.account, auth_type=self.auth_type)

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
            dids = [{'scope': scope, 'name': block_name}]

            response = rc.list_replicas(dids=dids)
            nodes = set()
            for item in response:
                for node, state in item['states'].items():
                    if state.upper() == 'AVAILABLE':
                        nodes.add(node)
            result['block'].append({block_name: list(nodes)})
        return result


if __name__ == '__main__':
    cms_rucio = CMSRucio(account='ewv', auth_type='x509_proxy')
    result = cms_rucio.getReplicaInfoForBlocks(scope='user.ewv',
                                               block='/SingleMuon/Run2017A-PromptReco-v2/MINIAOD#7ca0e53a-4c6a-11e7-a64a-001e67ac06a0')
    print("Result from one block:")
    pprint(result)

    result = cms_rucio.getReplicaInfoForBlocks(scope='user.ewv',
                                               block=[
                                                   '/SingleMuon/Run2017A-PromptReco-v2/MINIAOD#7ca0e53a-4c6a-11e7-a64a-001e67ac06a0',
                                                   '/SingleMuon/Run2017A-PromptReco-v2/MINIAOD#3492a052-4c84-11e7-a64a-001e67ac06a0'])
    print("Result from list of blocks:")
    pprint(result)

    result = cms_rucio.getReplicaInfoForBlocks(scope='user.ewv',
                                               dataset='/SingleMuon/Run2017A-PromptReco-v2/MINIAOD')
    print("Result from one dataset:")
    pprint(result)

    result = cms_rucio.getReplicaInfoForBlocks(scope='user.ewv',
                                               dataset=['/SingleMuon/Run2017A-PromptReco-v2/MINIAOD'])
    print("Result from list of datasets:")
    pprint(result)

