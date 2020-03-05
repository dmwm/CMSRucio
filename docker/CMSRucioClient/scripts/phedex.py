#! /bin/env python
"""
Module with methods implementing interface with phedex data service
and DAS.
"""

from __future__ import absolute_import, division, print_function

import argparse
import json
import logging
import re
import time
import urllib
import xml.etree.ElementTree as ET
from subprocess import PIPE, Popen

import requests
from cmstfc import tfc_lfn2pfn
from requests.exceptions import ReadTimeout

DEBUG_FLAG = False
DEFAULT_DASGOCLIENT = '/usr/bin/dasgoclient'

DEFAULT_PHEDEX_INST = 'prod'
DEFAULT_DATASVC_URL = 'https://cmsweb.cern.ch/phedex/datasvc'
#DEFAULT_DATASVC_URL = 'https://cmsweb-test.cern.ch/phedex/datasvc'
DATASVC_MAX_RETRY = 3
DATASVC_RETRY_SLEEP = 10
DATASVC_URL_FORMAT = '%s/json/%s/%s?%s'

GSIFTP_SCHEME = re.compile(r'(gsiftp)://(.+?):?(\d+)?/?(/.*)')
SRM_SCHEME = re.compile(r'(srm)://(.+?):(\d+)?(.*=)?(/.*)')
XROOTD_SCHEME = re.compile(r'(root)://(.+?):?(\d+)?/?(/.*)')
GSIFTP_DEFAULT_PORT = '2811'
SRM_DEFAULT_PORT = '8446'
XROOTD_DEFAULT_PORT = '1095'

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
    'tfc',
    'fts',
    'pnns',
    'links'
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
            options = '&'.join([opt + '=' + urllib.quote(val) for opt, val in options.items()])
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
        try:
            logging.debug('phedex.das query %s', query)
            proc = Popen([self.dasgoclient, '-query=%s' % query, '-json'], stdout=PIPE)
            output = proc.communicate()[0]
            logging.debug('phedex.das output %s', output)
            return json.loads(output)
        except (ValueError, OSError):
            logging.error('Skipping DAS query %s', query)
            return {}

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
        :outtype:     type of output items (default 'block').
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

    def summary_blocks_at_site(self, pnn, prefix=None, since=None):
        if prefix:
            params = {'node': pnn, 'dataset': '/%s*/*/*' % prefix}
        else:
            params = {'node': pnn, 'dataset': '/*/*/*'}

        if since:
            params['update_since'] = str(since)

        result = self.datasvc('blockreplicasummary', options=params)
        retval = {i['name']: True for i in result['phedex']['block'] if i['replica'][0]['complete'] == 'y'}
        return retval

    def block_at_pnn_phedex(self, block=None, pnn=None):
        """
        Use the PhEDEx data service to verify that a block is at the PNN

        :param block: Block name (CMS) or dataset name (Rucio)
        :param pnn: PhEDEx Node
        :return: boolean
        """

        if not block or not pnn:
            raise NotImplementedError('You must supply a block and node name')

        params = {'node': pnn, 'block': block}

        result = self.datasvc('blockreplicas', options=params)

        try:
            at_pnn = bool('phedex' in result and
                          'block' in result['phedex'] and
                          'replica' in result['phedex']['block'][0] and
                          result['phedex']['block'][0]['replica'][0]['complete'] == 'y')
        except IndexError:
            return False

        return at_pnn

    def fileblock_files_phedex(self, pfb, pnn=None):
        """
        Get the phedex files in a fileblock at a node using the PhEDEx data service
        :pfb:         phedex fileblock
        :pnn:         the phedex node name.

        returns: {'<filename>': {'name': <filename>, 'size': <size>, 'checksum': <checksum>}, ...}
        """

        if not pnn:
            raise NotImplementedError('fileblock_files_phedex requires a pnn to work')

        logging.debug('phedex.fileblock_files_phedex pfb=%s pnn=%s', pfb, pnn)

        params = {'node': pnn, 'block': pfb}
        phedex_result = self.datasvc('filereplicas', options=params)

        block_summary = {}
        for block in phedex_result['phedex']['block']:
            if block['name'] != pfb:
                continue
            files = block['file']
            for file in files:
                try:
                    cksum = re.match(r"\S*adler32:([^,]+)", file['checksum']).group(1)
                    cksum = "{0:0{1}x}".format(int(cksum, 16), 8)
                except AttributeError:
                    logging.warning("file %s has no adler32 checksum entry %s" % (file['name'], file['checksum']))
                    cksum = None
                block_summary[file['name']] = {'name': file['name'], 'checksum': cksum, 'size': file['bytes']}

        return block_summary

    def fileblock_files(self, pfb, pnn=None):
        """
        Just get the phedex files in a fileblock at a node.
        :pfb:         phedex fileblock
        :pnn:         the phedex node name. default None.
                      if defined only the files at node are considered

        returns: {'<filename>': {'name': <filename>, 'size': <size>, 'checksum': <checksum>}, ...}
        """
        raise NotImplementedError
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
                cksum = "{0:0{1}x}".format(int(cksum, 16), 8)
            except AttributeError:
                logging.warning("file %s has no adler32 checksum entry %s"\
                                     % (pfile['file'][0]['name'], pfile['file'][0]['checksum']))
                cksum = None

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
        raise NotImplementedError

        logging.info("phedex.fileblocks_files pditem=%s, pnn=%s. Start", pditem, pnn)
        ret = {}

        for pfb in self.list_data_items(pditem, pnn, metadata=False):
            ret[pfb] = self.fileblock_files(pfb, pnn)

        logging.info("phedex.fileblocks_files pditem=%s, pnn=%s. Done", pditem, pnn)

        return ret

    def blocks_at_site(self, pnn, prefix=None, since=None):

        if prefix:
            params = {'node': pnn, 'dataset': '/%s*/*/*' % prefix}
        else:
            params = {'node': pnn, 'dataset': '/*/*/*'}

        result = self.datasvc('blockreplicas', options=params)
        retval = {i['name']: i['files'] for i in result['phedex']['block'] if i['replica'][0]['complete'] == 'y'}
        return retval

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

    @staticmethod
    def tfc_from_xml(xmlfile):
        """
        Get the tfc from an xml file
        """
        
        tfc = []

        xml = ET.parse(xmlfile)

        for rule in xml.getroot():
            rule_dict = rule.__dict__['attrib']
            rule_dict['element_name'] = rule.__dict__['tag']
            tfc.append(rule_dict)

        return tfc

    def tfc(self, pnn, dump=True, proto='srmv2', exclude=None, concise=True, normalize=None):
        """
        Get the TFC of a PhEDEx node.
        :pnn:       phedex node name or the path to the tfc xml file.
        :dump:      returns the full tfc as given by data svn. default True
        :proto:     starting protocol of the selection (if dump is False)
        :exclude:   exclude some lfn paths (default None, all path are kept)
        :concise:   return the tfc rule in the concise format used in
                    rucio RSE protocol definition.
                    Only if dump is False. default True.
        :normalize: if not None, normalizes the rules in the tfc according to
                    some seinfo (in the form of the output of seinfo method)

        returns: array with the tfc rules.
        """
        if pnn[0] == '/':
            full = self.tfc_from_xml(pnn)
        else:
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
                if normalize:
                    prefix = normalize['protocol'] + '://' + normalize['hostname']

                    rule['result'] = rule['result'].replace('\\', '')\
                        .replace(prefix + '/', prefix + ':' + str(normalize['port']) + '/')

                if concise:
                    rule_info = {
                        'path': rule['path-match'],
                        'out': rule['result'],
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

    def seinfo(self, pnn, protocol=DEFAULT_PROTOCOL, probes=DEFAULT_PROBE_LFNS, tfc=None):
        """
        Uses lfn2pfn, probes a pnn with some lfns and gets informations about the se.
        :pnn:       phedex node name.
        :protocol:  protocol. default is DEFAULT_PROTOCOL
        :probes:    list of files to use to probe the se. default is DEFAULT_PROBE_LFNS.
        :tfc:       use this tfc instead of datasvc to perform lfn2pfn. Can be a json or
                    a path to an xml file

        return:    {protocol: <protocol>, hostname: <hostname>,
                    port: <port>, webpath: <webpath>, prefix: <prefix>}

        raise exception if different probes return different se infos.
        """

        seinfo = None
        for probe in probes:
            res = self.lfn2pfn(lfn=probe, pnn=pnn, protocol=protocol,
                               details=True, tfc=tfc)

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

    def lfn2pfn(self, lfn, pnn, protocol=DEFAULT_PROTOCOL, details=False, tfc=None):
        """
        Wraps the lfn2pfn datasvc call
        :lfn:      logical file name
        :pnn:      PhEDEx node name
        :protocol: tfc protocol. Default srmv2
        :details:  if True, rather than just the pfn returns the result of
                   an attempted matching of the pfn: protocol, hostname, port
                   webpath, path, prefix. Some of these fields may be None for
                   some schemes. Default False
        :tfc:      tfc to be used instead of calling datasvc. Can be a json string
                   or a path to an xml file

        return:    the pfn or a dictionnary with the details.
                   {pfn: <pfn>, protocol: <protocol>, hostname: <hostname>,
                    port: <port>, webpath: <webpath>, path: <path>,
                    prefix: <prefix>}
        """
        logging.debug('phedex.lfn2pfn: pnn=%s, lfn=%s, protocol=%s, details=%s, tfc=%s',
                      pnn, lfn, protocol, details, tfc)

        if tfc is None:
            req = self.datasvc('lfn2pfn', {'node': pnn,
                                           'protocol': protocol,
                                           'lfn': lfn})
            pfn = req['phedex']['mapping'][0]['pfn']
        else:
            if tfc[0] == '/':
                tfc_dict = self.tfc(tfc, dump=False, proto=protocol)
            else:
                tfc_dict = tfc
            pfn = tfc_lfn2pfn(lfn, tfc_dict, protocol)

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

        elif XROOTD_SCHEME.match(pfn):
            (protocol, hostname, port, path) =\
                XROOTD_SCHEME.match(pfn).groups()
            port = port or XROOTD_DEFAULT_PORT
            webpath = None

        else:
            raise Exception("lfn2pfn: No schema matched. Aborting.")

        try:
            prefix = re.compile(r'(\S*)' + lfn).match(path).groups()[0]
        except AttributeError:
            logging.warning('lfn2pfn of %s at %s has no prefix (pfn=%s)',
                            lfn, pnn, pfn)
            prefix = None

        info = {
            'pfn': pfn,
            'protocol': protocol,
            'hostname': hostname,
            'port': int(port),
            'path' : path,
            'prefix': prefix
        }

        if webpath is not None:
            info['webpath'] = webpath

        return info

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

    def pnns(self, select=None, exclude=None, metadata=False):
        """
        Wraps the nodes call of datasvc
        :select:   a node is selected if it matches any of the regex
                   in the list. Default match all
        :exclude:  a node is excluded if it matches any of the regex
                   in the list. Default empty list
        :metadata: returns metadata. Default False

        returns: list of nodes names or nodes metadata (if metadata=True)
        """
        if select is None:
            select = [r'\S+']
        if exclude is None:
            exclude = []

        select = [re.compile(regex) for regex in select]
        exclude = [re.compile(regex) for regex in exclude]

        nodes = self.datasvc(call='nodes')['phedex']['node']
        selected = [
            node for node in nodes if
            any(regex.match(node['name']) for regex in select) and
            not any(regex.match(node['name']) for regex in exclude)
        ]

        if not metadata:
            selected = [node['name'] for node in selected]

        return selected

    def links(self, src=None, dest=None, dump=False):
        """
        Wraps the links api call of datasvc
        :src:   source file (can be a db wildcards)
        :dest:  source file (can be a db wildcards)
        :dump:  dumps all info instead tha just distance

        returns a double dictionnary
            {<src>: {<dest>: <linkinfo>, ...},...}
        for all non disabled <src> to <dest> links
        (unless dump is True in which case all links are considered)
        where <linkinfo> is the distance of the links (or the full
        link info if dump is True)
        """

        opts = {}

        if src:
            opts['from'] = src
        if dest:
            opts['to'] = dest

        links = self.datasvc(call='links', options=opts)

        ret = {}

        for link in links['phedex']['link']:
            src = link['from']
            dest = link['to']

            if not dump and link['status'] == 'deactivated':
                continue

            if not dump:
                link = link['distance']

            if src not in ret:
                ret[src] = {}
            ret[src][dest] = link

        return ret


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
    PARSER.add_argument('--src', dest='src', default=None,
                        help='source node.')
    PARSER.add_argument('--dest', dest='dest', default=None,
                        help='destination node.')
    PARSER.add_argument('--dump', dest='dump', action='store_true',
                        help='full dump. Only with tfc and links calls.')
    PARSER.add_argument('--concise', dest='concise', action='store_true',
                        help='print tfc in concise format.')
    PARSER.add_argument('--exclude_re', dest='exclude_re',
                        help='exclusive regexp.')
    PARSER.add_argument('--normalize', dest='normalize', default=None,
                        help='SE info for normalizing the tfc.')
    PARSER.add_argument('--tfc', dest='tfc', default=None,
                        help='tfc to be used instead of datasvc.')


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
        raise NotImplementedError

        if opts.pditem is None:
            print("Function 'fileblock_files' requires the --pditem parameter")
        else:
            print(PCLI.fileblock_files(pfb=opts.pditem, pnn=opts.pnn))

    def ffileblocks_files(opts):
        """
        Function to call the method: fileblock_files
        """
        raise NotImplementedError

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
        Function to call the method: pnns
        """
        print(PCLI.pnns(select=opts.select, exclude=opts.exclude))

    def ftfc(opts):
        """
        Function to call the method: tfc
        """

        if opts.normalize is not None:
            opts.normalize = json.loads(opts.normalize.replace("'", '"'))

        logging.info(opts.normalize)

        if opts.pnn is None:
            print("Function 'tfc' requires the --pnn parameter")
        else:
            print(PCLI.tfc(pnn=opts.pnn, dump=opts.dump, proto=opts.protocol,
                           exclude=opts.exclude_re, concise=opts.concise,
                           normalize=opts.normalize))

    def fseinfo(opts):
        """
        Function to call the method: lfn2pfn
        """
        opts.probes = opts.probes or DEFAULT_PROBE_LFNS

        if opts.pnn is None:
            print("Function 'lfn2pfn' requires the --pnn parameter")
        else:
            print(PCLI.seinfo(pnn=opts.pnn, protocol=opts.protocol,
                              probes=opts.probes, tfc=opts.tfc))

    def flfn2pfn(opts):
        """
        Function to call the method: lfn2pfn
        """
        if opts.pnn is None or opts.lfn is None:
            print("Function 'lfn2pfn' requires the --pnn and --lfn parameters")
        else:
            print(PCLI.lfn2pfn(lfn=opts.lfn, pnn=opts.pnn, protocol=opts.protocol,
                               details=opts.details, tfc=opts.tfc))

    def ffts(opts):
        """
        Function to call the method: lfn2pfn
        """

        if opts.pnn is None:
            print("Function 'fts' requires the --pnn parameter")
        else:
            print(PCLI.fts(pnn=opts.pnn))

    def flinks(opts):
        """
        Function to call the method: lfn2pfn
        """

        print(PCLI.links(src=opts.src, dest=opts.dest, dump=opts.dump))


    FUNCTIONS = {
        'das': fdas,
        'datasvc' : fdatasvc,
        'check_data_item' : fcheck_item,
        'list_data_items': flist_items,
        'fileblock_files': ffileblock_files,
        'fileblocks_files': ffileblocks_files,
        'subscriptions': fsubscriptions,
        'senames': fsenames,
        'seinfo': fseinfo,
        'lfn2pfn': flfn2pfn,
        'fts': ffts,
        'tfc': ftfc,
        'pnns': fpnns,
        'links': flinks,
    }

    FUNCTIONS[OPTIONS.function](OPTIONS)
