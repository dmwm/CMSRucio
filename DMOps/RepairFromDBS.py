#! /usr/bin/env python3


import json
import pdb

from rucio.client import Client

from subprocess import PIPE, Popen

# import requests
# from requests.exceptions import ReadTimeout
#
# from gfal2 import GError, Gfal2Context
#
# import rucio.rse.rsemanager as rsemgr
# from rucio.client.client import Client
# from rucio.common.exception import (DataIdentifierAlreadyExists, FileAlreadyExists, RucioException,
#                                     AccessDenied)
DEBUG_FLAG = False
DEFAULT_DASGOCLIENT = '/cvmfs/cms.cern.ch/common/dasgoclient'

_ = pdb.__name__

client = Client()


def das_go_client(query, dasgoclient=DEFAULT_DASGOCLIENT, debug=DEBUG_FLAG):
    """
    just wrapping the dasgoclient command line
    """
    proc = Popen([dasgoclient, '-query=%s' % query, '-json'], stdout=PIPE)
    output = proc.communicate()[0]
    if debug:
        print('DEBUG:' + output)
    return json.loads(output)


INCOMPLETE_BLOCKS = [
#('/SingleNeutrino/RunIISummer19UL16RECOAPV-FlatPU0to75_106X_mcRun2_asymptotic_preVFP_v8_ext2-v2/AODSIM#63fea451-5a5c-4fbf-8c8b-c0e476716355', 'T1_US_FNAL_Tape'),
#('/VBFH_HToSSTo4Tau_MH-125_TuneCUETP8M1_13TeV-powheg-pythia8/RunIISummer16DR80Premix-PUMoriond17_rp_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/GEN-SIM-RECO#1a7b8a08-4ddb-4661-90bd-b1f875ae2691', 'T1_US_FNAL_Tape'),
('/ZeroBias1/Commissioning2018-26Apr2018-v1/MINIAOD#da3b6a3b-c0db-47c7-8948-ef5bc0c0e887', 'T1_US_FNAL_Tape'),
]


# BLOCK = "/StreamExpress/Run2018A-PromptCalibProdSiStripGains-Express-v1/ALCAPROMPT#50d78a18-38ef-4cd4-8721-a617c441aa5b"
# RSE = 'T2_CH_CERN'


def files_in_block(block):
    result = das_go_client(query='file block=%s' % block)
    files = []
    for record in result:
        files.append(record['file'][0]['name'])
    return files


def dbs_file_info(filename):
    result = das_go_client(query='file file=%s' % filename)

    n_bytes = result[0]['file'][0]['size']
    adler32 = result[0]['file'][0]['adler32']

    return n_bytes, adler32


def files_in_rucio_ds(block):
    files = []
    for record in client.list_content(scope='cms', name=block):
        files.append(record['name'])
    return files


if __name__ == '__main__':
    """
    Sync site data manager roles to RSE attributes
    """

    for block, rse in INCOMPLETE_BLOCKS:
        print('Fixing %s at %s' % (block, rse))
        true_files = files_in_block(block=block)
        rucio_files = files_in_rucio_ds(block=block)

        print('%s files in DBS vs %s files in Rucio' % (len(true_files), len(rucio_files)))

        missing_files = set(true_files) - set(rucio_files)

        for m_file in missing_files:
            n_bytes, adler32 = dbs_file_info(filename=m_file)
            try:
                result = client.add_replica(rse=rse, scope='cms', name=m_file, bytes=n_bytes, adler32=adler32)
                print('Added file (%s) %s with %s bytses and %s' % (result, m_file, n_bytes, adler32))
            except: 
                print('Problem adding file %s' % m_file)
            try:
                result = client.attach_dids(scope='cms', name=block, dids=[{'scope': 'cms', 'name': m_file}])
            except Exception as ex:
                print('Problem attaching file %s' % ex)
                
