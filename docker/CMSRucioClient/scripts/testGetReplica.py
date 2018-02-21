#! /bin/env python

from __future__ import absolute_import, division, print_function

from pprint import pprint

from CMSRucio import CMSRucio

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

    result = cms_rucio.dataset_summary(scope='user.ewv',
                                       dataset='/SingleMuon/Run2017A-PromptReco-v2/MINIAOD')
    print("Result from one dataset:")
    del result['files']

    pprint(result)

    result = cms_rucio.dataset_summary(scope='cms',
                                       dataset='/Neutrino_E-10_gun/RunIISummer17PrePremix-MCv2_correctPU_94X_mc2017_realistic_v9-v1/GEN-SIM-DIGI-RAW')

    del result['files']

    pprint(result)
