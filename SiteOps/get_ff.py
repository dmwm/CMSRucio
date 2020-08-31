#!/usr/bin/env python

# get_ff.py - get the path and tape family for the incoming files in the dataset
#
# Usage:
#
#    get_ff.py data [data ...]
#
#    data could be a dataset name or a block name
#
#    the format of data is like the followings
#
#    dataset name is a "/" delimited path, the last term is the type
#    block name is a dataset name with #<block>
#

from __future__ import division, print_function
import sys
import os

from rucio.client.didclient import DIDClient
from rucio.client.ruleclient import RuleClient

def usage():
    print("usage:")
    print()
    print("%s [-h] <id/name> [<id/name>>] ..."%(os.path.basename(sys.argv[0])))
    print()
    print("    <id/name> rule id or name")
    print()
    print("    -h this message")

# Category - here is a static mapping of known categories

category = {
    1: ['RAW'],
    2: ['ALCARECO'],
    3: ['AOD',
        'MINIAOD',
	'NANOAOD',
	'RAW-RECO',
	'USER',
	'DQMIO' ],
    4: ['GEN-SIM-RAW ',
        'GEN-SIM' ],
    5: ['LHE',
        'GEN-SIM-DIGI-RAW',
	'GEN-SIM-RECO',
	'NANOAODSIM',
	'PREMIX',
	'GEN-SIM-DIGI',
	'MINIAODSIM',
	'AODSIM',
	'GEN',
	'SIM' ]
}

# rules for tape_family assignment
#
#   Category:
#       3 or 5: inherit
#       1     : FF+RAW
#       2     : FF+Prompt
#       4     : FF+GenSimRaw
#
#  default: inherit
#
def tape_family(tag, label):
    if tag in category[3] or tag in category[5]:
        return "inherit"
    if tag in category[1]:
        return label+'RAW'
    if tag in category[2]:
        return label+'Prompt'
    if tag in category[4]:
        return label+'GenSimRaw'
    # default to be "inherit"
    return "inherit"

# default scop
scope = 'cms'

didclient = DIDClient()
ruleclient = RuleClient()

# get_ff(scope, name) -- returns path, ff from scope, name
#
#   name is either a dataset name or a block name
#   find lfns of the files and determin the common path
#   aassign ff (tape family) according to the third term in lfn and the last term in dataset name
def get_ff(scope, name):
    # take the last term of dataset name as type (tag)
    tag = name.split('/')[-1].split('#')[0]
    ff = {}
    # get files from the DID
    res = didclient.list_files(scope, name)
    for i in res:
        # print(i['name'])
        t = i['name'].split('/')
	for j in range(len(t)):
	    if t[j] == tag:
	        break
	path = os.path.join('/', *t[:j+2])
	ff[path] = tape_family(tag, t[3])
    return ff

# get_rule(id) - returns scope, name from rule id
def get_rule(id):
    r = ruleclient.get_replication_rule(id)
    return r['scope'], r['name']

if __name__ == '__main__':
    if len(sys.argv) == 1 or sys.argv[1] == "-h":
        usage()
	sys.exit(0)
    for i in sys.argv[1:]:
        # check if it is a rule id or a dataset name
	if i.find('/') == -1:    # rule id
            scope, name = get_rule(i)
        else:
	    name = i
    	res = get_ff(scope, name)
	for path, ff in res.items():
	    print(path, ff)

