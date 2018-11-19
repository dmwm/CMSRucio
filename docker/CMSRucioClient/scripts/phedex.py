#! /bin/env python
"""
Module with methods implementing interface with phedex data service
and DAS.
"""

from __future__ import absolute_import, division, print_function

import json
import re
import time
import logging
import argparse

from subprocess import PIPE, Popen
import requests
from requests.exceptions import ReadTimeout

DEBUG_FLAG = False
DEFAULT_DASGOCLIENT = '/usr/bin/dasgoclient'

DEFAULT_PHEDEX_INST = 'prod'
DEFAULT_DATASVC_URL = 'https://cmsweb.cern.ch/phedex/datasvc'
DATASVC_MAX_RETRY = 3
DATASVC_RETRY_SLEEP = 10
DATASVC_URL_FORMAT = '%s/json/%s/%s?%s'

GSIFTP_SCHEME = re.compile(r'(gsiftp)://(.+?):?(\d+)?/?(/.*)')
SRM_SCHEME = re.compile(r'(srm)://(.+?):(\d+)?(.*=)?(/.*)')
GSIFTP_DEFAULT_PORT = '2811'
SRM_DEFAULT_PORT = '8446'

DEFAULT_PROTOCOL = 'srmv2'

DEFAULT_PROBE_LFNS = ('/store/data/prod/file/path.root', '/store/mc/prod/file/path.root')

FTS3_LOG_MATCH = re.compile('.* -backend FTS3 .*')
FTS3_LOG_SERVER_MATCH = re.compile('.* -service (.*?) .*')

MODULE_FUNCTION_LIST = [
    'das',
    'datasvc',
    'list_data_items',
    'check_data_item',
    'fileblock_files',
    'fileblocks_files',
    'subscriptions',
    'lfn2pfn',
    'seinfo',
    'senames',
    'fts',
    'pnns',
]

class PhEDEx(object):
    """
    wrapping DataService and DAS client to query the status of Data and
    Site Nodes in PhEDEx
    """

    def __init__(self, instance=DEFAULT_PHEDEX_INST, dasgoclient=DEFAULT_DASGOCLIENT,
                 datasvc=DEFAULT_DATASVC_URL):
        self.instance = instance
        self.dasgoclient = dasgoclient
        self.datasvc_url = datasvc

    def datasvc(self, call, options=None):
        """
        just wrapping a call to datasvc APIs
        :call:     the api to be called
        :options:  dictionary with the set of options

        returns: dictionnary with the data
        """

        if options:
            options = '&'.join([opt + '=' + val for opt, val in options.items()])
        else:
            options = ''

        url = DATASVC_URL_FORMAT % (self.datasvc_url, self.instance, call, options)

        done = False
        tries = 0

        logging.debug('phedex.datasvc url=%s', url)

        while tries < DATASVC_MAX_RETRY and (not done):
            try:
                done = True
                req = requests.get(url, allow_redirects=False, verify=False)
            except ReadTimeout:
                logging.warning('datasvc url=%s timed out. Retrying', url)
                done = False
                time.sleep(DATASVC_RETRY_SLEEP)
                tries += 1

        logging.debug('phedex.datasvc status code %s', req.status_code)
        logging.debug('phedex.datasvc output %s', req.text)

        if req.status_code != 200:
            raise Exception('Request Failed')

        return json.loads(req.text)


    def das(self, query):
        """
        just wrapping the dasgoclient command line
        :query: the query to be sent to DAS

        returns: dictionnary with data
        """
        logging.debug('phedex.das query %s', query)
        proc = Popen([self.dasgoclient, '-query=%s' % query, '-json'], stdout=PIPE)
        output = proc.communicate()[0]
        logging.debug('phedex.das output %s', output)
        return json.loads(output)

    @staticmethod
    def check_data_item(pditem):
        """
        Just checks if a phedex data item is a fileblock or a dataset
        :pditem:  phedex data item

        returns: {'isblock': bool, 'pds': <dataset name>,
                   'fbhash': <fileblock hash>, 'pfb': <fileblock name>}
        fbhash and pfb are none id isblock is false.
        """

        try:
            (pds, fbhash) = pditem.split('#')
            pfb = pditem
            isblock = True
        except ValueError:
            pds = pditem
            pfb = None
            fbhash = None
            isblock = False

        return {'isblock': isblock, 'pds': pds, 'fbhash': fbhash, 'pfb': pfb}

    def list_data_items(self, pditem=None, pnn=None, outtype='block', metadata=True, locality=True):
        """
        Gets a list of fileblocks or datasets at a site and/or in a dataset.
        :pditem:      phedex the dataitem: dataset or fileblock.
                      default None. If defined, only blocks in the item are considered.
        :pnn:         the phedex node name. default None.
                      If defined, only blocks at node are considered.
        :metadata:    list block metadata not only block names (default True).
        :outtype:        type of output items (default 'block').
        :locality:    include locality information (default True). Must be true id pnn is not None.

        returns: an array of blocks/datsets name or blocks/datasets metadata

        WARNING: not all combinations work properly.
        """

        if (pditem is None) and (pnn is None):
            logging.warning('phedex.list_fileblocks called with no args. returing an empty list.')
            return []

        if (not locality) and (pnn is not None):
            raise Exception('locality parameter must be True if pnn is not null')

        query = outtype

        if pditem is not None:
            check = self.check_data_item(pditem)
            if check['isblock'] and outtype != 'dataset':
                query += ' block=%s' % pditem
            else:
                query += ' dataset=%s' % check['pds']

        if pnn is not None:
            query += ' site=%s' % pnn

        if locality:
            query += ' system=phedex'

        pditems = self.das(query)

        if not metadata:
            pditems = [item[outtype][0]['name'] for item in pditems]

        return pditems


    def fileblock_files(self, pfb, pnn=None):
        """
        Just get the the phedex files in a fileblock at a node.
        :pfb:         phedex fileblock
        :pnn:         the phedex node name. default None.
                      if defined only the files at node are considered

        returns: {'<filename>': {'name': <filename>, 'size': <size>, 'checksum': <checksum>}, ...}
        """

        logging.debug('phedex.fileblock_files pfb=%s pnn=%s', pfb, pnn)

        block_summary = {}

        if pnn is None:
            files = self.das("file block=%s system=phedex" % pfb)
        else:
            files = self.das("file block=%s site=%s system=phedex"
                             % (pfb, pnn))

        for pfile in files:

            # sometimes dasgoclient does not return the checksum attribute for a file
            # re-fetching data fix the problem
            try:
                pfile['file'][0]['checksum']
            except KeyError:
                logging.warning("file %s misses checksum attribute, try to refetch from das",
                                pfile['file'][0]['name'])
                time.sleep(5)
                dummy = self.das("file file=%s system=phedex" % pfile['file'][0]['name'])
                pfile['file'][0] = dummy['file'][0]

            # extract the actual checksum
            try:
                cksum = re.match(r"\S*adler32:([^,]+)",
                                 pfile['file'][0]['checksum']).group(1)
            except AttributeError:
                raise AttributeError("file %s has non parsable checksum entry %s"\
                                     % (pfile['file'][0]['name'], pfile['file'][0]['checksum']))

            cksum = "{0:0{1}x}".format(int(cksum, 16), 8)
            block_summary[pfile['file'][0]['name']] = {
                'name': pfile['file'][0]['name'],
                'checksum': cksum,
                'size': pfile['file'][0]['size']
            }

        return block_summary


    def fileblocks_files(self, pditem=None, pnn=None):
        """
        Like fileblock_files but it does this on multiple fileblocks
        :pditem:      phedex fileblock. default None.
        :pnn:         the phedex node name. default None.
                      if defined only the files at node are considered

        returns: {'<block>': {'<filename>':
                   {'name': <filename>, 'size': <size>, 'checksum': <checksum>},
                 ...},...}
        """
        logging.info("phedex.fileblocks_files pditem=%s, pnn=%s. Start", pditem, pnn)
        ret = {}

        for pfb in self.list_data_items(pditem, pnn, metadata=False):
            ret[pfb] = self.fileblock_files(pfb, pnn)

        logging.info("phedex.fileblocks_files pditem=%s, pnn=%s. Done", pditem, pnn)

        return ret


    def subscriptions(self, pnn, pditem=None, since=None):
        """
        Get a dictionary of "per block" phedex subscriptions
        :pnn:     phedex node name
        :pditem:  data item: dataset or fileblock containing (default null, all datasets)
        :since:   datasets created since (default null, all datasets)

        return: {'<blockname>': <subscrition metadata>}
        """

        subs = {}
        req = {'node': pnn, 'collapse': 'n', 'percent_min': '100'}

        if pditem is not None:
            check = self.check_data_item(pditem)
            check['fbhash'] = check['fbhash'] or '*'
            req['block'] = check['pds'] + '%23' + check['fbhash']
        if since is not None:
            req['create_since'] = since

        phedex_subs = self.datasvc('subscriptions', req)

        if len(phedex_subs['phedex']['dataset']) == 0:
            logging.warning('phedex.get_subscriptions subs list \
                         for pnn=%s and pditem=%s is empty.', pnn, pditem)
            return {}

        for dataset in phedex_subs['phedex']['dataset']:
            for block in dataset['block']:
                if block['is_open'] == 'y':
                    logging.warning("Block %s is open, skipping", block['name'])
                    continue
                subs[block['name']] = block['subscription'][0]

        return subs


    def tfc(self, pnn, dump=True, proto='srmv2', exclude=None, concise=True):
        """
        Get the TFC of a PhEDEx node.
        :pnn:     phedex node name.
        :dump:    returns the full tfc as given by data svn. default True
        :proto:   starting protocol of the selection (if dump is False)
        :exclude: exclude some lfn paths (default None, all path are kept)
        :concise: return the tfc rule in the concise format used in
                  rucio RSE protocol definition.
                  Only if dump is False. default True.

        returns: array with the tfc rules.
        """
        req = self.datasvc('tfc', {'node': pnn})
        full = req['phedex']['storage-mapping']['array']

        if dump:
            return full

        try:
            exclude_re = re.compile(exclude)
        except TypeError:
            exclude_re = None

        protos = [proto]
        num_protos = 0

        while len(protos) != num_protos:
            num_protos = len(protos)
            for rule in full:
                if rule['element_name'] == 'lfn-to-pfn' and rule['protocol'] in protos:
                    if ('chain' in rule) and (rule['chain'] not in protos):
                        protos.append(rule['chain'])

        selected = []

        for rule in full:
            if rule['element_name'] == 'lfn-to-pfn' and rule['protocol'] in protos and \
               (exclude is None or not exclude_re.match(rule['path-match'])):
                if concise:
                    rule_info = {
                        'path': rule['path-match'],
                        # TO BE FIXED: Bad hack to fix the rules with \\
                        # in the srm path (like FNAL...)
                        'out': rule['result'].replace('\\', ''),
                        'proto': rule['protocol'],
                    }
                else:
                    rule_info = rule

                if 'chain' in rule:
                    rule_info['chain'] = rule['chain']
                selected.append(rule_info)

        return selected

    def senames(self, pnn=None, protocol=None):
        """
        Just wraps the senames datasvc call.
        :pnn:       phedex node name. If None, all nodes are queried
        :protocol:  protocol. If None, all protocols are queried.
        """
        parameters = {}
        if pnn is not None:
            parameters['node'] = pnn
        if protocol is not None:
            parameters['protocol'] = protocol
        req = self.datasvc('senames', parameters)
        return req['phedex']['senames']

    def pnns(self, select=None, exclude=None):
        """
        List all PhEDEx node nams
        :select:  list of regexp to select the pnn's.
                  A pnn is selected if it matches one regexp.
        :exclude: list of regexp to exclude.
                  A pnn is excluded if it matches one regexp.
        """

        select = select or ['.*']
        exclude = exclude or []

        select_re = []
        exclude_re = []

        for sel in select:
            select_re.append(re.compile(sel))

        for exc in exclude:
            exclude_re.append(re.compile(exc))

        pnns = []

        for sename in self.senames():
            pnn = sename['node']

            skip = False
            for exc in exclude_re:
                if exc.match(pnn):
                    skip = True

            if skip:
                continue

            for sel in select_re:
                if sel.match(pnn):
                    pnns.append(pnn)

        return list(set(pnns))


    def seinfo(self, pnn, protocol=DEFAULT_PROTOCOL, probes=DEFAULT_PROBE_LFNS):
        """
        Uses lfn2pfn, probes a pnn with some lfns and gets informations about the se.
        :pnn:       phedex node name.
        :protocol:  protocol. default is DEFAULT_PROTOCOL
        :probes:    list of files to use to probe the se. default is DEFAULT_PROBE_LFNS.

        return:    {protocol: <protocol>, hostname: <hostname>,
                    port: <port>, webpath: <webpath>, prefix: <prefix>}

        raise exception if different probes return different se infos.
        """

        seinfo = None
        for probe in probes:
            res = self.lfn2pfn(lfn=probe, pnn=pnn, protocol=protocol,
                               details=True)

            # remove useless keys
            for key in ['pfn', 'path']:
                res.pop(key, None)

            if seinfo:
                if seinfo != res:
                    raise Exception("Probes %s give different se info for pnn %s"
                                    % (probes, pnn))
            else:
                seinfo = res

        return seinfo


    def lfn2pfn(self, lfn, pnn, protocol=DEFAULT_PROTOCOL, details=False):
        """
        Wraps the lfn2pfn datasvc call
        :lfn:      logical file name
        :pnn:      PhEDEx node name
        :protocol: tfc protocol. Default srmv2
        :details:  if True, rather than just the pfn returns the result of
                   an attempted matching of the pfn: protocol, hostname, port
                   webpath, path, prefix. Some of these fields may be None for
                   some schemes. Default False

        return:    the pfn or a dictionnary with the details.
                   {pfn: <pfn>, protocol: <protocol>, hostname: <hostname>,
                    port: <port>, webpath: <webpath>, path: <path>,
                    prefix: <prefix>}
        """
        logging.debug('phedex.lfn2pfn: pnn=%s, lfn=%s, protocol=%s, details=%s',
                      pnn, lfn, protocol, details)

        req = self.datasvc('lfn2pfn', {'node': pnn,
                                       'protocol': protocol,
                                       'lfn': lfn})
        pfn = req['phedex']['mapping'][0]['pfn']

        if not details:
            return pfn

        # Get the relevant schemas parameters
        if SRM_SCHEME.match(pfn):
            (protocol, hostname, port, webpath, path) =\
                SRM_SCHEME.match(pfn).groups()
            port = port or SRM_DEFAULT_PORT

        elif GSIFTP_SCHEME.match(pfn):
            (protocol, hostname, port, path) =\
                GSIFTP_SCHEME.match(pfn).groups()
            port = port or GSIFTP_DEFAULT_PORT
            webpath = None
        else:
            raise Exception("lfn2pfn: No schema matched. Aborting.")

        try:
            prefix = re.compile(r'(\S*)' + lfn).match(path).groups()[0]
        except AttributeError:
            logging.warning('lfn2pfn of %s at %s has no prefix (pfn=%s)',
                            lfn, pnn, pfn)
            prefix = None

        return {
            'pfn': pfn,
            'protocol': protocol,
            'hostname': hostname,
            'port': int(port),
            'webpath': webpath,
            'path' : path,
            'prefix': prefix
        }

    def fts(self, pnn):
        """
        Returns a list of FTS servers from node's FileDownload agent config
        :pnn: PhEDEx Node Name.

        retruns: list of server names (unique)
        """
        logs = self.datasvc(call='agentlogs',
                            options={'agent': 'FileDownload', 'node': pnn})

        servers = []

        for agent in logs['phedex']['agent']:
            for log in agent['log']:
                for message in log['message'].values():
                    if FTS3_LOG_MATCH.match(message):
                        result = FTS3_LOG_SERVER_MATCH.match(message)
                        if result:
                            servers.append(result.group(1))

        return list(set(servers))



if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='''CLI for the functions defined in phedex.py''',
        )
    PARSER.add_argument('-v', '--verbose', dest='debug', action='store_true',
                        help='increase output verbosity')
    PARSER.add_argument('-t', '--dry-run', dest='dry', action='store_true',
                        help='only printout what would have been done')
    PARSER.add_argument('--func', dest='function',
                        choices=MODULE_FUNCTION_LIST, required=True,
                        help='Function that you want to run.')
    PARSER.add_argument('--inst', dest='instance', default=DEFAULT_PHEDEX_INST,
                        help='PhEDEx instance, default %s.' % DEFAULT_PHEDEX_INST)
    PARSER.add_argument('--dasgoclient', dest='dasgoclient', default=DEFAULT_DASGOCLIENT,
                        help='DAS client to use. default %s.' % DEFAULT_DASGOCLIENT)
    PARSER.add_argument('--pnn', dest='pnn', help='PhEDEx node name', default=None)
    PARSER.add_argument('--pditem', dest='pditem', help='PhEDEx data item', default=None)
    PARSER.add_argument('--query', dest='query', help='Query for the das system', default=None)
    PARSER.add_argument('--call', dest='call', default=None,
                        help='Data service API call.')
    PARSER.add_argument('--option', nargs=2, dest='options', action='append', default=None,
                        help='Data service option. KEY VALUE format, can be multiple.')
    PARSER.add_argument('--metadata', dest='metadata', action='store_true',
                        help='should return the metadata. Default False.')
    PARSER.add_argument('--locality', dest='locality', action='store_true',
                        help='should return the locality data. Default False.')
    PARSER.add_argument('--outtype', dest='outtype', default='block',
                        help='output data item type. Default block.')
    PARSER.add_argument('--since', dest='since', default=None,
                        help='starting time.')
    PARSER.add_argument('--protocol', dest='protocol', default=DEFAULT_PROTOCOL,
                        help='tfc  or url protocol.')
    PARSER.add_argument('--lfn', dest='lfn', default=None,
                        help='lfn to be resolved.')
    PARSER.add_argument('--details', dest='details', action='store_true',
                        help='detailed output.')
    PARSER.add_argument('--probe', dest='probes', action='append', default=None,
                        help='LFN for probing SE, can be multiple, default ' +
                        str(DEFAULT_PROBE_LFNS))
    PARSER.add_argument('--select', dest='select', action='append', default=None,
                        help='selecting regexps for pnn listing. Can be multiple.')
    PARSER.add_argument('--exclude', dest='exclude', action='append', default=None,
                        help='excluding regexps for pnn listing. Can be multiple.')


    OPTIONS = PARSER.parse_args()

    if OPTIONS.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    PCLI = PhEDEx(instance=OPTIONS.instance, dasgoclient=OPTIONS.dasgoclient)

    def fdas(opts):
        """
        Function to call the method: das
        """
        if opts.query is None:
            print("Function 'das' requires the --query parameter")
        else:
            print(PCLI.das(opts.query))

    def fdatasvc(opts):
        """
        Function to call the method: datasvc
        """
        if opts.options:
            opts.options = {opt[0]: opt[1] for opt in opts.options}
        if opts.call is None:
            print("Function 'datasvc' requires the --call parameter")
        else:
            print(PCLI.datasvc(opts.call, opts.options))

    def fcheck_item(opts):
        """
        Function to call the method: check_data_item
        """
        if opts.pditem is None:
            print("Function 'datasvc' requires the --pditem parameter")
        else:
            print(PCLI.check_data_item(opts.pditem))

    def flist_items(opts):
        """
        Function to call the method: list_data_items
        """
        print(PCLI.list_data_items(pnn=opts.pnn, pditem=opts.pditem, metadata=opts.metadata,
                                   outtype=opts.outtype, locality=opts.locality))

    def ffileblock_files(opts):
        """
        Function to call the method: fileblock_files
        """
        if opts.pditem is None:
            print("Function 'fileblock_files' requires the --pditem parameter")
        else:
            print(PCLI.fileblock_files(pfb=opts.pditem, pnn=opts.pnn))

    def ffileblocks_files(opts):
        """
        Function to call the method: fileblock_files
        """
        print(PCLI.fileblocks_files(pditem=opts.pditem, pnn=opts.pnn))

    def fsubscriptions(opts):
        """
        Function to call the method: subscriptions
        """
        if opts.pnn is None:
            print("Function 'subscriptions' requires the --pnn parameter")
        else:
            print(PCLI.subscriptions(pnn=opts.pnn, pditem=opts.pditem, since=opts.since))

    def fsenames(opts):
        """
        Function to call the method: senames
        """
        print(PCLI.senames(pnn=opts.pnn, protocol=opts.protocol))

    def fpnns(opts):
        """
        Function to call the method: senames
        """
        print(PCLI.pnns(select=opts.select, exclude=opts.exclude))

    def fseinfo(opts):
        """
        Function to call the method: lfn2pfn
        """
        opts.probes = opts.probes or DEFAULT_PROBE_LFNS

        if opts.pnn is None:
            print("Function 'seinfo' requires the --pnn parameter")
        else:
            print(PCLI.seinfo(pnn=opts.pnn, protocol=opts.protocol,
                              probes=opts.probes))

    def flfn2pfn(opts):
        """
        Function to call the method: lfn2pfn
        """
        if opts.pnn is None or opts.lfn is None:
            print("Function 'lfn2pfn' requires the --pnn and --lfn parameters")
        else:
            print(PCLI.lfn2pfn(lfn=opts.lfn, pnn=opts.pnn, protocol=opts.protocol,
                               details=opts.details))

    def ffts(opts):
        """
        Function to call the method: lfn2pfn
        """

        if opts.pnn is None:
            print("Function 'fts' requires the --pnn parameter")
        else:
            print(PCLI.fts(pnn=opts.pnn))


    FUNCTIONS = {
        'das': fdas,
        'datasvc' : fdatasvc,
        'check_item' : fcheck_item,
        'list_data_items': flist_items,
        'fileblock_files': ffileblock_files,
        'fileblocks_files': ffileblocks_files,
        'subscriptions': fsubscriptions,
        'senames': fsenames,
        'seinfo': fseinfo,
        'lfn2pfn': flfn2pfn,
        'fts': ffts,
        'pnns': fpnns,
    }

    FUNCTIONS[OPTIONS.function](OPTIONS)
