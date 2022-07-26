#!/usr/bin/env python3

# transferurl.py -- compose transfer url at RSE
#
# usage: transferurl.py [-h] rse [protocol] file
# 
# compose transfer url at RSE according to storage.json
# 
# positional arguments:
#   rse         Rucio Storage Element
#   protocol    transfer protocol
#   file        file
# 
# optional arguments:
#   -h, --help  show this help message and exit
# 
# ** without arguments, input is taken from stdin, one line per file

from rucio.client import Client
from rucio.common.exception import RucioException
import re
import sys
import argparse

def usage():
    print("usage: %s <rse> [<protocol>] <file>")

def lfn(s):
    t = s.split('/store/')
    return '/store/'+ t[1]

def lfn2pfn(lfn, tfc):
    m = re.match(tfc['path'], lfn)
    if m:
        return tfc['out'].replace('$1',m.group(1))

def turl(protocols, scheme=None, file=None):
    res = []
    p = None
    for protocol in protocols:
        if not scheme or scheme == protocol['scheme']:
            if 'extended_attributes' in protocol and protocol['extended_attributes']:
                if 'tfc' in protocol['extended_attributes']:
                    for tfc in protocol['extended_attributes']['tfc']:
                        p = lfn2pfn(lfn(file), tfc)
                        if p:
                            res.append(p)
                            break
                    if p: # No more to be done
                        continue
            if protocol['extended_attributes'] and protocol['extended_attributes']['web_service_path']:
                extended_attributes = protocol['extended_attributes']['web_service_path']
            else:
                extended_attributes = ''
            prefix= protocol['prefix']
            if prefix[-1] == '/':
                prefix = prefix[:-1]
            p = protocol['scheme']+'://'+protocol['hostname']+':'+str(protocol['port'])+extended_attributes+prefix+lfn(file)
            res.append(p)
    return res

if __name__ == '__main__':
    c = Client()
    if len(sys.argv) < 2: # taking input from stdin
        f = sys.__stdin__
        l = f.readline()
        while l:
            args = l.strip().split()
            rse_name = args[0]
            if len(args) == 2:
                protocol = None
                file = args[1]
            elif len(args) == 3:
                protocol = args[1]
                file = args[2]
            else:
                print("Error! wrong number of arguments: %s"%(l.strip()))
                exit(1)
            try:
                rse = c.get_rse(rse_name)
            except RucioException as err:
                print(err)
                l = f.readline()
                continue
            r = turl(rse['protocols'], protocol, file)
            for i in r:
                print(i)
            l = f.readline()
    else:
        parser = argparse.ArgumentParser(
            description = "compose transfer url at RSE according to storage.json",
            epilog = "** without arguments, input is taken from stdin, one line per file")
        parser.add_argument('rse', nargs=1, help="Rucio Storage Element")
        parser.add_argument('protocol', nargs="?", help="transfer protocol")
        parser.add_argument('file', nargs=1, help="file")
        opt = parser.parse_args()

        try:
            rse = c.get_rse(opt.rse[0])
        except RucioException as err:
            print(err)
            exit(1)
        r = turl(rse['protocols'], opt.protocol, opt.file[0])
        for i in r:
            print(i)
    
