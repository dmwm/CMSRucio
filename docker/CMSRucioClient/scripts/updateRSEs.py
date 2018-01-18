#! /bin/env python
# Script for managing (creating/updating/deleting) the RSEs and their attributes 
# Initially will use PhEDEx nodes information as input,
# should be easily transformed/extended for using other sources. 

import argparse
import logging
import sys

import urlparse
import requests
import json
import re

from functools import wraps

from rucio.client.accountclient import AccountClient
from rucio.client.rseclient import RSEClient
from rucio.common.exception import Duplicate, RSEProtocolPriorityError, \
	RSEProtocolNotSupported, RSENotFound, InvalidObject, CannotAuthenticate

# Create reusable session:
session = requests.Session()
session.verify=('/etc/grid-security/certificates')

SUFFIX = '0000' # Set to None for generating real RSE names without suffix
DATASVC_URL = 'http://cmsweb.cern.ch/phedex/datasvc/json/prod/'
# Pre-compiled regex for PhEDEx returned data: 
prog = re.compile('.* -service (.*?) .*') 

def setup_logger(logger):
	""" Code borrowed from bin/rucio-admin
	"""
	logger.setLevel(logging.DEBUG)
	hdlr = logging.StreamHandler()
	def emit_decorator(fcn):
		def func(*args):
			if True:
				formatter = logging.Formatter("%(message)s")
			else:
				levelno = args[0].levelno
				if levelno >= logging.CRITICAL:
					color = '\033[31;1m'
				elif levelno >= logging.ERROR:
					color = '\033[31;1m'
				elif levelno >= logging.WARNING:
					color = '\033[33;1m'
				elif levelno >= logging.INFO:
					color = '\033[32;1m'
				elif levelno >= logging.DEBUG:
					color = '\033[36;1m'
				else:
					color = '\033[0m'
				formatter = logging.Formatter('{0}%(asctime)s %(levelname)s [%(message)s]\033[0m'.format(color))
			hdlr.setFormatter(formatter)
			return fcn(*args)
		return func
	hdlr.emit = emit_decorator(hdlr.emit)
	logger.addHandler(hdlr)

def exception_handler(function):
	"""Code borrowed from bin/rucio-admin
	"""
	@wraps(function)
	def new_funct(*args, **kwargs):
		try:
			return function(*args, **kwargs)
		except InvalidObject as error:
			logger.error(error)
			return error.error_code
		except CannotAuthenticate as error:
			logger.error(error)
			logger.error('Please verify that your proxy is still valid and renew it if needed.')
			sys.exit(1)
	return new_funct

# Functions for getting PhEDEx information: 

def PhEDEx_node_to_RSE(node, suffix = SUFFIX):
	""" Translates PhEDEx node names to RSE names. 
	Make sure new names comply with the policies defined in: 
	./lib/rucio/common/schema/cms.py
	./lib/rucio/core/permission/cms.py
	Because once created RSE name can't be reused, allow to postpend
	the name with a test_tag string (default: 0000).
	In reality something like USERDISK|DATADISK|SCRATCHDISK will be used.
	"""	
	if suffix is not None:
		suffix = args.suffix

	return (node + '_' + suffix).upper()

def PhEDEx_node_FTS_servers(node):
	"""Returns a list of FTS servers used in node's FileDownload agent configuration"""
	# FIXME: check node existence.
	payload={'agent':'FileDownload','node':node}
	URL = urlparse.urljoin(DATASVC_URL,'agentlogs')
	RESP = session.get(url=URL, params=payload )
	DATA = json.loads(RESP.content)
	servers = {}
	for agent in DATA['phedex']['agent']:
		for log in agent['log']:
			for message in log['message'].values():
				assert ('-backend FTS3' in message), \
					"FTS3 backend is not configured for " + node
				result = prog.match (message)
				if result:
					servers[result.group(1)] = True
	return servers.keys()

# Functions for translating information to Rucio standards

def PhEDEx_node_names():
	""" Returns a sorted list of PhEDEx node names via data service nodes API
	excluding nodes with no data. """
	URL = urlparse.urljoin(DATASVC_URL,'nodes')
	payload = {'noempty': 'y'}
	RESP = session.get(url=URL, params=payload)
	DATA = json.loads(RESP.content)
	names = []
	for n in DATA['phedex']['node']:
		names.append(n['name'])
	names.sort()
	return names

# Functions involving Rucio client actions

@exception_handler
def whoami ( account = 'natasha', auth_type='x509_proxy'):
	"""Runs whoami command for a given account via client tool, requires a valid proxy
	"""
	account_client = AccountClient(account=account, auth_type='x509_proxy')
	print("Connected to rucio as %s" % account_client.whoami()['account'])

def list_rses(client):
	"""Prints names of existing RSEs"""
	for rse in rse_client.list_rses():
		print (rse['rse'])

@exception_handler
def add_rse(client, name):
	"""Adds an rse """
	rse_client.add_rse(name)
	if args.verbose:
		print "Added RSE "+name
		info=rse_client.get_rse(name)
		for q,v in info.iteritems():
			print q+" :  ",v

if __name__ == '__main__':
	parser = argparse.ArgumentParser( \
		description = \
		'''Create or update RSEs and their attributes based on TMDB information''', \
		epilog = """This is a test version use with care!""")
	parser.add_argument('-v', '--verbose', action='store_true', \
		help='increase output verbosity')
	parser.add_argument('--test-auth', action='store_true', \
		help='executes AccountClient.whoami (use --account option to change identity')
	parser.add_argument('--list-nodes', action='store_true', \
		help='list PhEDEx node names')
	parser.add_argument('--list-rses', action='store_true', \
		help='list RSE names')
	parser.add_argument('--account', default='natasha', help=' use account ')
	parser.add_argument('--add-rse', metavar='RSE_NAME|all',
		help="""add RSE by name or for all PhEDEx nodes, using pre-generated names.
		PhEDEx nodes with no data are ignored. Can be combined with --suffix option. """)
	parser.add_argument('--suffix', default = SUFFIX, \
		help='append suffix to RSE names pre-generated from PhEDEx node names')
	parser.add_argument('--node-fts-servers', default = None, \
		help='List fts servers used by PhEDEx node (e.g. T2_PK_NCP)')
	args = parser.parse_args()
	if args.verbose:
		print (args)
	# Take care of Rucio exceptions:
	logger = logging.getLogger("user")
	setup_logger(logger)

	if args.test_auth:
		whoami(account=args.account)

	# create re-usable RSE client connection:
	rse_client = RSEClient(account=args.account, auth_type='x509_proxy')

	if args.list_rses:
		list_rses(rse_client)

	if args.list_nodes:
		nodes = PhEDEx_node_names()
		for n in nodes:
			print n, " => ", PhEDEx_node_to_RSE(n)

	if args.node_fts_servers:
		servers = PhEDEx_node_FTS_servers(args.node_fts_servers)
		print "FTS servers used by " + args.node_fts_servers + ' PhEDEx node:'
		for s in servers:
			print s 
					
	# Handle RSE additions
	if args.verbose:
		print "===== Initial list of RSEs:"
		list_rses(rse_client)

	if args.add_rse:
		if args.add_rse == 'all':
			for n in PhEDEx_node_names():
				#print "Adding RSE " +  PhEDEx_node_to_RSE(n)  # for test only
				add_rse(rse_client,  PhEDEx_node_to_RSE(n))
		else:
			#print "Adding RSE " +  PhEDEx_node_to_RSE(args.add_rse)  # for test only
			add_rse(rse_client, args.add_rse)
			
	if args.verbose:
		print "===== Current list of RSEs:"
		list_rses(rse_client)
