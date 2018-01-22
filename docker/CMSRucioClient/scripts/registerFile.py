#! /bin/env python
"""
Command line tool for registering into rucio a single file which is on storage.
The tools gets the relevant rse informations from rucio and
uses gfal API to get the size and checksum of the file
"""

from __future__ import absolute_import, division, print_function

import argparse

from gfal2 import Gfal2Context, GError
from rucio.client.replicaclient import ReplicaClient
import rucio.rse.rsemanager as rsemgr


PARSER = argparse.ArgumentParser(
    description="register_file: register an existing file on options['rse']"
)
PARSER.add_argument('--scope', dest='scope', help='scope of the file.', required=True)
PARSER.add_argument('--name', dest='name', help='DID of the file.', required=True)
PARSER.add_argument('--rse', dest='rse', help='RSE where the replica is.', required=True)

OPTIONS = PARSER.parse_args()

RSE = rsemgr.get_rse_info(OPTIONS.rse)

# Use the first protocol
PROTO = RSE['protocols'][0]

# Get the replica url
SCHEMA = PROTO['scheme']
PREFIX = PROTO['prefix'] + '/' + OPTIONS.scope.replace('.', '/')
if SCHEMA == 'srm':
    PREFIX = PROTO['extended_attributes']['web_service_path'] + PREFIX
URL = SCHEMA + '://' + PROTO['hostname']
if PROTO['port'] != 0:
    URL = URL + ':' + str(PROTO['port'])
URL = URL + PREFIX + '/' + OPTIONS.name

GFAL = Gfal2Context()

try:
    SIZE = GFAL.stat(str(URL)).st_size
    CHECKSUM = GFAL.checksum(str(URL), 'adler32')
    print("Registering file: pfn=%s size=%s checksum=%s" % (URL, SIZE, CHECKSUM))
except GError:
    print("no file found at %s" % URL)
    exit()

R = ReplicaClient()

REPLICAS = list(R.list_replicas([{'scope': OPTIONS.scope, 'name': OPTIONS.name}]))
if REPLICAS:
    REPLICAS = REPLICAS[0]
    if 'rses' in REPLICAS:
        if OPTIONS.rse in REPLICAS['rses']:
            print("file %s with scope %s has already a replica at %s" %
                  (OPTIONS.name, OPTIONS.scope, OPTIONS.rse))
            exit()



REPLICA = [{
    'scope': OPTIONS.scope,
    'name' : OPTIONS.name,
    'adler32': CHECKSUM,
    'bytes': SIZE,
    'pfn': URL
}]

R.add_replicas(rse=OPTIONS.rse, files=REPLICA)
