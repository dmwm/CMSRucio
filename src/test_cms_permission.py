# Copyright 2012-2019 CERN for the benefit of the ATLAS collaboration.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
# - Vincent Garonne <vgaronne@gmail.com>, 2012-2013
# - Mario Lassnig <mario.lassnig@cern.ch>, 2012-2019
# - Cedric Serfon <cedric.serfon@cern.ch>, 2013
# - Andrew Lister <andrew.lister@stfc.ac.uk>, 2019

"""
Test the Permission Core and API
"""
from __future__ import print_function

CAN_ADD_ANY_RULE = ['sync_', 'wma_']
MANAGED_DATATIERS = ['USER', 'NANOAOD', 'NANOAODSIM']


def cms_rule_logic(rses, kwargs):
    """
    A test function to execute the same logic we have in the temporary version of permissions/cms.py
    """

    # Are all the RSEs OK?
    rses_ok = True
    for rse in rses:
        rse_type = rse.get('cms_type', None)
        if rse_type not in ['test', 'rw']:
            rses_ok = False

    # Is the account clear?
    account_ok = False
    for prefix in CAN_ADD_ANY_RULE:
        if kwargs['account'].startswith(prefix):
            account_ok = True

    # Is the data tier clear
    tier_ok = True
    for did in kwargs['dids']:
        this_did_ok = False
        for tier in MANAGED_DATATIERS:
            check_tier = '/' + tier
            if check_tier in did['name']:
                this_did_ok = True
        if not this_did_ok:
            tier_ok = False

    if not rses_ok and not account_ok and not tier_ok:
        return False
    return True


scope = 'cms'

nano_did = {'scope': scope, 'name': '/blah/blah/NANOAODSIM#1234-5678-90'}
non_did = {'scope': scope, 'name': '/blah/blah/GENSIM#1234-5678-90'}

test_rse = {'name': 'T2_US_UCSD_Test', 'cms_type': 'test'}
real_rse = {'name': 'T2_US_UCSD'}

user_account = 'ewv'
sync_account = 'sync_T2_US_UCSD'

print('User can put nano at test:', cms_rule_logic([test_rse], {'dids': [nano_did], 'account': user_account}))
print('User can put nano at real:', cms_rule_logic([real_rse], {'dids': [nano_did], 'account': user_account}))
print('User can put rand at real:', cms_rule_logic([real_rse], {'dids': [non_did], 'account': user_account}))
print('User can put many at real:', cms_rule_logic([real_rse], {'dids': [non_did, nano_did], 'account': user_account}))
print('Sync can put many at real:', cms_rule_logic([real_rse], {'dids': [non_did, nano_did], 'account': sync_account}))
print('Sync can put nano at real:', cms_rule_logic([real_rse], {'dids': [nano_did], 'account': sync_account}))
print('Sync can put nano at test:', cms_rule_logic([test_rse], {'dids': [nano_did], 'account': sync_account}))
print('User can put many at test:', cms_rule_logic([test_rse], {'dids': [non_did, nano_did], 'account': user_account}))
print('User can put nano at many:', cms_rule_logic([real_rse, test_rse], {'dids': [nano_did], 'account': user_account}))
print('User can put rand at many:', cms_rule_logic([real_rse, test_rse], {'dids': [non_did], 'account': user_account}))
print('Sync can put nano at many:', cms_rule_logic([real_rse, test_rse], {'dids': [nano_did], 'account': sync_account}))
print('Sync can put rand at many:', cms_rule_logic([real_rse, test_rse], {'dids': [non_did], 'account': sync_account}))
