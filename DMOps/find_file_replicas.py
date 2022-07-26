#!/usr/bin/env python3

# find_file_replicas.py -- find replicas of files
#
# usage: list_file_replicas.py [-h] [-f FILE] [-v] [-n] [lfn [lfn ...]]
# 
# find RSEs that have replica
# 
# positional arguments:
#   lfn         logical file name
# 
#   optional arguments:
#     -h, --help  show this help message and exit
#     -f FILE     file of lfns, could be multiple
#     -v          verbose: show turl
#     -n          with -v, do not show LFN
# 
# ** without arguments, input is taken from stdin

from rucio.client import Client
from rucio.common.exception import RucioException
import sys
import argparse
import json
import pprint

# A Spool that takes a list of file names and a list of objects, then, iterates through
#
# Takes input from sys.__stdin__ if there is nothing in command line parameters
#
class Spool:
    def __init__(self, files, objs):
        self.files = files
        self.objs = objs
        self.fd = None
        if len(self.files):
            self.fd = open(self.files.pop(0))
        elif not len(self.objs):
            self.fd = sys.__stdin__

    def fetch(self, n = 1000):
        res = []
        if not self.fd: # No file to read
            if len(self.objs):
                ll = min(n, len(self.objs))
                res = self.objs[:ll]
                del self.objs[:ll]
        else: # reading from file
            l = self.fd.readline()
            if not l:
                if len(self.files):
                    self.fd = open(self.files.pop(0))
                    l = self.fd.readline()
                else:
                    self.fd = None
            count = 0
            while l:
                res.append(l.strip().split()[0])
                count += 1
                if count >= n:
                    break
                l = self.fd.readline()
                if not l:
                    if len(self.files):
                        self.fd = open(self.files.pop(0))
                        l = self.fd.readline()
                    else:
                        self.fd = None
            if not self.fd:
                if count < n:
                    ll = min(n-count, len(self.objs))
                    res = res + self.objs[:ll]
                    del self.objs[:ll]
        return res

# get lfn from and after "/store/"
def lfn(s):
    t = s.split('/store/')
    return '/store/'+ t[1]

scope = 'cms'
def did(s):
    return {'scope':scope, 'name':lfn(s)}

if __name__ == '__main__':

    # handle input parameters
    parser = argparse.ArgumentParser(
        description = "find RSEs that have replica",
        epilog = "** without arguments, input is taken from stdin")
    parser.add_argument('-f', action='append', dest='file', default=[], help="file of lfns, could be multiple")
    parser.add_argument('-v', action='store_true', dest='verbose', help="verbose: show turl")
    parser.add_argument('-n', action='store_true', dest='quiet', help="with -v, do not show LFN")
    parser.add_argument('lfn', nargs='*', default=[], help="logical file name")
    opt = parser.parse_args()
    
    try:
        c = Client()
        c.whoami()
    except RucioException as err:
        print(err)
        exit(1)
    
    s = Spool(opt.file, opt.lfn)
    lfns = s.fetch()
    #pprint.pprint(lfns)
    while lfns:
        res = []
        try:
            res = c.list_replicas(list(map(did, lfns)))
        except RucioException as err:
            print(err)
    
        for i in res:
            if not opt.verbose or not opt.quiet:
                print(i['name'], '[', end = '')
                for j in i['rses'].keys():
                    print ("",j , end = '')
                print('',']')
            if opt.verbose: # show turls
                for j in i['rses'].keys():
                    for k in i['rses'][j]:
                        print("    %-16s"%(j), k)
    
        lfns = s.fetch()
    
