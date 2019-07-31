#! /usr/bin/env python

from rucio.client.client import Client
from pprint import pprint

cl = Client()
cl.whoami()

filename='/store/mc/RunIIFall18wmLHEGS/SUSYGluGluToBBHToBB_M-600_TuneCP5_13TeV-amcatnlo-pythia8/GEN-SIM/102X_upgrade2018_realistic_v11-v1/280000/A61D92B2-C74A-6045-8325-869194181F9E.root'

pprint(cl.list_request_by_did(filename, 'T3_CH_CERN_EOS_Test', scope='cms'))

