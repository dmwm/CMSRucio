#!/usr/bin/env python

import sys
import os
import getopt
import pprint
from rucio.client.client import Client
from rucio.common.exception import (RuleNotFound, AccessDenied)

# default mode
auto = False
silent = False
verbose = False

def usage():
    me = os.path.basename(sys.argv[0])
    print "usage:"
    print
    print "%s [-f <file>] [-h] [-s] [-a] id ..."%(me)
    print
    print "    <file>: rules, one in each rwo, beginning with rule id as the first token"
    print "    without argument, %s takeinput from stdin"%(me)
    print
    print "    -s silent mode, no output; without -a, it still prompt for action"
    print "    -a auto mode, automatically approve one that can be approved"
    print "    -v verbose mode, overridden by -s"

opt, args = getopt.getopt(sys.argv[1:], "f:hasv")
for i in opt:
    if i[0] == '-h':
        usage()
	sys.exit(0)
    if i[0] == '-f':
        f = open(i[1])
	l = f.readline()
	while l:
	    args.append(l.strip().split()[0])
	    l = f.readline()
	f.close()
    if i[0] == '-a':
        auto = True
    if i[0] == '-s':
        silent = True
    if i[0] == '-v':
        verbose = True

# if there is nothing in args, take them from stdin
if len(args) < 1:
    f = sys.__stdin__
    l = f.readline()
    while l:
        args.append(l.strip().split()[0])
	l = f.readline()

# silent takes higher priority than verbose
if silent:
    verbose = False

client = Client()
approval_list = []

# checking
def check_rule(id):
    try:
        r = client.get_replication_rule(id)
	if r['state'] == 'WAITING_APPROVAL':
	    approval_list.append((r['id'], r['state'], r['name']))
	else:
	    if not silent:
	        print "Rule %s %s %s does not need approval"%(r['id'], r['state'], r['name'])
    except RuleNotFound:
        if not silent:
	    # print "No rule with the id %s found"%(i)
	    print sys.exc_info()[1].message
    except:
        if not silent:
            print sys.exc_info()[1]

for i in args:
    check_rule(i)

# approve rules

if not auto:
    if len(approval_list) > 0:
        print
        print "Rules to approve:"
        for i in approval_list:
            print i[0], i[1], i[2]
        print
        answer = raw_input("Approve these rules (Y/N)?")
        if not answer in ('Y','y','YES','Yes','yes'):
            print "No rule is approved"
    	sys.exit(0)
    else:
        print
        print "Nothing to approve"
	sys.exit(0)

for i in approval_list:
    if verbose:
        print "Aproving %s ..."%(i[0]),
    try:
        res = client.approve_replication_rule(i[0])
	if verbose:
	    print "Done"
    except AccessDenied:
        if not silent:
	    print sys.exc_info()[1].message, i[0]
    except:
        if not silent:
	    print sys.exc_info()[1]

print
for i in approval_list:
    r = client.get_replication_rule(i[0])
    print r['id'], r['state'], r['name']
