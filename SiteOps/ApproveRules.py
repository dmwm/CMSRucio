#!/usr/bin/env python

from __future__ import absolute_import, division, print_function
import sys
import os
import argparse
import pprint
from rucio.client.client import Client
from rucio.common.exception import (RuleNotFound, AccessDenied)


# Parsing input parameters
parser = argparse.ArgumentParser(
    description="Arrpove rules that are wating for approval",
    epilog="** without arguments, input is taken from stdin and warning is given if '-a' is not set")
parser.add_argument('-a', action='store_true', default=False, dest='auto', help="auto mode, automatically approve the ones that can be approved. by default, without '-a', it asks for confirmation")
parser.add_argument('-s', action='store_true', default=False, dest='silent', help="silent mode, no output; without '-a', it still prompts for action")
parser.add_argument('-v', action='store_true', default=False, dest='verbose', help="verbose mode, overridden by '-s'")
parser.add_argument('-f', action='append', dest='file', default=[], help="file of rules, one each line, beginning with rule id; could have multiple -f")
parser.add_argument('id', nargs='*', default=[], help="rule id")

opt = parser.parse_args()

# read id from files, if any
for i in opt.file:
    f = open(i)
    l = f.readline()
    while l:
        if l.strip() != "":
	    opt.id.append(l.strip().split()[0])
	l = f.readline()
    f.close()

# pprint.pprint(opt)

# sys.exit(0)


from_stdin = False

# if there is nothing in args, take them from stdin
if len(opt.id) < 1:
    f = sys.__stdin__
    l = f.readline()
    while l:
        if l.strip() != "":
            opt.id.append(l.strip().split()[0])
	l = f.readline()
    from_stdin = True

# silent takes higher priority than verbose
if opt.silent:
    opt.verbose = False

client = Client()
approval_list = []

# checking
def check_rule(id):
    try:
        r = client.get_replication_rule(id)
	if r['state'] == 'WAITING_APPROVAL':
	    approval_list.append((r['id'], r['state'], r['name']))
	else:
	    if not opt.silent:
	        print("Rule %s %s %s does not need approval"%(r['id'], r['state'], r['name']))
    except RuleNotFound:
        if not opt.silent:
	    # print("No rule with the id %s found"%(i))
	    print(sys.exc_info()[1].message)
    except:
        if not opt.silent:
            print(sys.exc_info()[1])

for i in opt.id:
    check_rule(i)

# approve rules

if not opt.auto:
    if len(approval_list) > 0:
        print()
        print("Rules to approve:")
        for i in approval_list:
            print(i[0], i[1], i[2])
        print()
	if from_stdin:
	    print("Warning: piped input requires '-a' for action. re-run the command with '-a'")
	    answer = 'N'
	else:
            answer = raw_input("Approve these rules (Y/N)? ")
	print()
        if not answer in ('Y','y','YES','Yes','yes'):
            print("No rule is approved")
    	    sys.exit(0)
    else:
        print()
        print("Nothing to approve")
	sys.exit(0)

count = 0
for i in approval_list:
    if opt.verbose:
        print("Aproving %s ..."%(i[0]), end=' ')
    try:
        res = client.approve_replication_rule(i[0])
	count += 1
	if opt.verbose:
	    print("Done")
    except AccessDenied:
        if not opt.silent:
	    print(sys.exc_info()[1].message, i[0])
    except:
        if not opt.silent:
	    print(sys.exc_info()[1])

if not opt.silent:
    print()
    print(count, "rule(s) approved")
    print()

if not opt.silent:
    for i in approval_list:
        r = client.get_replication_rule(i[0])
        print(r['id'], r['state'], r['name'])
