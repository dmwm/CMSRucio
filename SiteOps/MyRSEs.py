#!/usr/bin/env python
#
# MyRses.py - find all rses that I have authority to approve
#

import sys
import os
from rucio.client.client import Client

c = Client()

def usage():
    print "usage:"
    print
    print "%s [-h] [<account>]"%(os.path.basename(sys.argv[0]))
    print
    print "    -h this message"
    print "       without <account>, the current account takes place."

def myrses(account=None):
    rses = []
    if account == None:
        account = c.whoami()['account']

    for r in c.list_rses():
        attributes = c.list_rse_attributes(r['rse'])
        if (attributes.has_key('quota_approvers') and attributes['quota_approvers'].find(account) != -1) \
	    or (attributes.has_key('rule_approvers') and attributes['rule_approvers'].find(account) != -1):
	    rses.append(r)

    return rses

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == '-h':
	    usage()
	    sys.exit(0)
        rses = myrses(sys.argv[1])
    else:
        rses = myrses()

    for r in rses:
        print "%-24s %s"%(r['rse'], r['rse_type'])

