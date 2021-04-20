#! /bin/env python3

"""
Class definition for a CMS RSE. And script for updating RSE
"""
import copy
import logging
import pprint
import re
import json

from rucio.client.client import Client
from rucio.common.exception import RSEProtocolNotSupported, RSENotFound

APPROVAL_REQUIRED = ['T1_DE_KIT_Tape', 'T1_ES_PIC_Tape', 'T1_RU_JINR_Tape', 'T1_UK_RAL_Tape', 'T1_US_FNAL_Tape']
DOMAINS_BY_TYPE = {
    'prod-real': {'wan': {'read': 1, 'write': 1, 'third_party_copy': 1, 'delete': 1},
                  'lan': {'read': 0, 'write': 0, 'delete': 0}},
    'int-real': {'wan': {'read': 1, 'write': 0, 'third_party_copy': 1, 'delete': 0},
                 'lan': {'read': 0, 'write': 0, 'delete': 0}},
    'test': {'wan': {'read': 1, 'write': 1, 'third_party_copy': 1, 'delete': 1},
             'lan': {'read': 0, 'write': 0, 'delete': 0}},
    'temp': {'wan': {'read': 1, 'write': 1, 'third_party_copy': 1, 'delete': 1},
             'lan': {'read': 0, 'write': 0, 'delete': 0}},
}
RUCIO_PROTOS = ['SRMv2', 'XRootD', 'WebDAV']
PROTO_WEIGHT_TPC = {'WebDAV':1, 'XRootD': 3, 'SRMv2': 2}
PROTO_WEIGHT_RWD = {'WebDAV':2, 'XRootD': 3, 'SRMv2': 1}
IMPL_MAP = {'SRMv2': 'rucio.rse.protocols.gfalv2.Default',
            'XRootD': 'rucio.rse.protocols.gfal.Default',
            'WebDAV': 'rucio.rse.protocols.gfalv2.Default'}
DEFAULT_PORTS = {'gsiftp': 2811, 'root': 1094, 'davs':443}


class CMSRSE:
    """
    Wrapping the definition of a CMS RSE. Gathering the information
    from PhEDEx and translating them into the definition of a Rucio RSE
    for the different expected types: real, test, temp.
    """

    def __init__(self, json, dry=False, cms_type='real', deterministic=True):

        self.json = json
        self.dry = dry
        self.cms_type = cms_type

        self.rcli = Client()

        self.protocols = []
        self.attrs = {}
        self.settings = {}
        self.settings['deterministic'] = deterministic
        self.rucio_rse_type = json['type'].upper()

        # If we are building a _Test or _Temp instance add the special prefix
        if cms_type=="test":
            self.rse_name = json['rse']+"_Test"
        elif cms_type=="temp":
            self.rse_name = json['rse']+"_Temp"
        else:
            self.rse_name = json['rse']

        self._get_attributes()
        self.attrs['fts'] = ','.join(json['fts'])

    def _get_attributes(self, tier=None, country=None, xattrs=None):
        """
        Gets the expected RSE attributes according to the
        given cmsrse parameters and to the info from phedex
        :fts:               fts server. If None the server defined for
                            the pnn is taken.
        :tier:              tier. If None it is taken from pnn
        :lfn2pfn_algorithm: algorithm for lfn2pfn. If None the default
                            rsetype to lfn2pfn mapping is used
        :country:           country code. If None it is taken from pnn
        :xattrs:            extra attributes
        """
        xattrs = xattrs or {}

        attrs = {}

        rse_regex = re.compile(r'T(\d+)\_(\S{2})\_\S+')
        pnn_match = rse_regex.match(self.rse_name)
        attrs['tier'] = tier or pnn_match.group(1)
        attrs['country'] = country or pnn_match.group(2)
        attrs[self.rse_name] = 'True'
        if self.cms_type in ['int-real', 'prod-real']:
            attrs['cms_type'] = 'real'
        else:
            attrs['cms_type'] = self.cms_type

        if self.cms_type in ['test', 'prod-real'] and not self.rse_name.endswith('_Tape'):
            attrs['reaper'] = True

        self.protocols = []
        protos_json = self.json['protocols']
        for proto_json in protos_json:
            algorithm, proto = self._get_protocol(proto_json, protos_json)
            if algorithm:
                self.protocols.append(proto)
                attrs['lfn2pfn_algorithm'] = algorithm

        if self.rse_name in APPROVAL_REQUIRED:
            attrs['requires_approval'] = 'True'

        for (key, value) in xattrs:
            attrs[key] = value

        self.attrs = attrs
        return

    def _set_attributes(self):
        try:
            rattrs = self.rcli.list_rse_attributes(rse=self.rse_name)
        except RSENotFound:
            rattrs = {}

        changed = False

        for (key, value) in self.attrs.items():
            if key not in rattrs or rattrs[key] != value:
                # Hack. I can find no way to define an attribute to 1
                # (systematically reinterpreted as True)
                if key in rattrs and rattrs[key] is True and \
                        (str(value) == '1' or str(value) == 'True'):
                    continue

                if key not in rattrs:
                    rattrs[key] = 'None'
                logging.debug('setting attribute %s from value %s to value %s for rse %s',
                              key, rattrs[key], value, self.rse_name)
                changed = True
                if self.dry:
                    logging.info('setting attribute %s to value %s for rse %s. Dry run, skipping',
                                 key, value, self.rse_name)
                else:
                    self.rcli.add_rse_attribute(rse=self.rse_name, key=key, value=value)

        return changed

    def _get_protocol(self, proto_json, protos_json):
        """
        Get the informations about the RSE protocol from creator argument or
        from phedex
        :seinfo:      informations about the SE (in the form of the seinfo method of PhEDEx class).
                      If None the info is gathered from PhEDEx using the seinfo method.
        :add_prefix:  path to be added to the prefix in seinfo. if none
                      SE_ADD_PREFIX_BYTYPE is used.
        :tfc:         dictionnary with tfc rules. If None the info is gathered from PhEDEx using
                      the PhEDEx.tfc method,
        :exclude:     rules to be excluded from tfc (in case it is gathered from PhEDEx).
        :domains:     domains dictionnary. If none the DOMAINS_BYTYPE constant is used.
        :token:       space token. default None
        :proto:       protocol to be considered. default DEFAULT_PROTOCOL.
        """

        protocol_name = proto_json['protocol']
        algorithm = None
        proto = {}

        if protocol_name not in RUCIO_PROTOS:
            return algorithm, proto
        if proto_json['access'] != 'global-rw':
            return algorithm, proto

        domains = copy.deepcopy(DOMAINS_BY_TYPE[self.cms_type])
        # Set the priorities for Read, Write, Delete and TPC for each protocol
        # IMPORTANT: set SRMv2 Read, Write and Delete as Priority #1 because ASO needs it that way
        # TPC is the only one we care about.
        try:
            for method, weight in domains['wan'].items():
                if weight and protocol_name in PROTO_WEIGHT_TPC:
                    if method == "third_party_copy":
                        domains['wan'][method] = PROTO_WEIGHT_TPC[protocol_name]
                    else:
                        domains['wan'][method] = PROTO_WEIGHT_RWD[protocol_name]
        except KeyError:
            pass  # We're trying to modify an unknown protocol somehow
        #TODO: Make sure global-rw is set
        if proto_json.get('prefix', None):
            """
            The simple case where all we have is a prefix. This just triggers the identity algorithm 
            with some simple settings
            """

            algorithm = 'cmstfc'
            try:
                logging.debug("Looking for prefix with web service path")
                prefix_regex = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+):(\d+)(\/.*\=)(.*)')
                prefix_match = prefix_regex.match(proto_json['prefix'])

                scheme = prefix_match.group(1)
                hostname = prefix_match.group(2)
                port = prefix_match.group(3)
                extended_attributes = {'web_service_path': prefix_match.group(4)}
                prefix = prefix_match.group(5)
            except AttributeError:  # Missing web service path?
                try:
                    logging.debug("Looking for prefix with port")

                    prefix_regex = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+):(\d+)/(.*)')
                    prefix_match = prefix_regex.match(proto_json['prefix'])

                    scheme = prefix_match.group(1)
                    hostname = prefix_match.group(2)
                    port = prefix_match.group(3)
                    prefix = '/' + prefix_match.group(4)
                    extended_attributes = None
                except AttributeError:
                    try:
                        logging.debug("Looking for prefix, no port and suffix")
                        prefix_regex = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+)/(.*)')
                        prefix_match = prefix_regex.match(proto_json['prefix'])

                        scheme = prefix_match.group(1)
                        hostname = prefix_match.group(2)
                        extended_attributes = None
                        prefix = '/' + prefix_match.group(3)
                        port = DEFAULT_PORTS[scheme]
                    except AttributeError:
                        try:
                            logging.debug("Looking for prefix, port and no suffix")
                            prefix_regex = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+):(\d+)')
                            prefix_match = prefix_regex.match(proto_json['prefix'])

                            scheme = prefix_match.group(1)
                            hostname = prefix_match.group(2)
                            extended_attributes = None
                            prefix = '/'
                            port = prefix_match.group(3)
                        except AttributeError:
                            logging.debug("Looking for prefix, no port and no suffix")
                            prefix_regex = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+)')
                            prefix_match = prefix_regex.match(proto_json['prefix'])
                            scheme = prefix_match.group(1)
                            hostname = prefix_match.group(2)
                            extended_attributes = None
                            prefix = '/'
                            port = DEFAULT_PORTS[scheme]

            # Make sure that prefix always ends with "/"
            if prefix[len(prefix) -1] != "/":
                prefix = prefix +"/"

            # if we are building a _Test instance add the specia prefix
            if self.cms_type == "test":
                prefix = prefix +"store/test/rucio/"

            elif self.cms_type == "temp":
                prefix = prefix +"store/temp/"

            proto = {
                'scheme': scheme,
                'hostname': hostname,
                'port': port,
                'extended_attributes': extended_attributes,
                'domains': domains,
                'prefix': prefix,
                'impl': IMPL_MAP[protocol_name]
            }
        elif proto_json.get('rules', None):
            """ 
            Instead we have a full set of TFC rules which we need to gather
            """
            chains = {protocol_name.lower()}
            done_chains = set()
            tfc = []

            algorithm = 'cmstfc'
            rules = proto_json['rules']
            for rule in rules:
                # On first pass, fill in the basic information we need before pulling out the rules
                # including any first level chain names
                if rule.get('chain', None):
                    chains.add(rule['chain'])
                try:
                    prefix_regex = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+):(\d+)(\/.*\=)(.*)')
                    prefix_match = prefix_regex.match(rule['pfn'])
                    proto['scheme'] = prefix_match.group(1)
                    proto['hostname'] = prefix_match.group(2)
                    proto['port'] = prefix_match.group(3)
                    proto['extended_attributes'] = {'tfc_proto': protocol_name.lower(),
                                                    'web_service_path': prefix_match.group(4)}
                    proto['prefix'] = '/'
                    proto['domains'] = domains
                    proto['impl'] = IMPL_MAP[protocol_name]
                except AttributeError:
                    prefix_regex = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+)(.*)')
                    prefix_match = prefix_regex.match(rule['pfn'])
                    proto['scheme'] = prefix_match.group(1)
                    proto['hostname'] = prefix_match.group(2)
                    proto['port'] = DEFAULT_PORTS[proto['scheme']]
                    proto['extended_attributes'] = {'tfc_proto': protocol_name.lower()}
                    proto['prefix'] = '/'
                    proto['domains'] = domains
                    proto['impl'] = IMPL_MAP[protocol_name]

            # Now go through all the protocols including ones we were not interested in at first and get rules
            # Turn {"protocol": "SRMv2",
            #       "access": "global-rw",
            #         "rules": [ {"lfn": "/+(.*)", "pfn": "srm://SRM_URL=/$1", "chain": "pnfs"} ]
            #      },
            # into {u'path': u'(.*)', u'out': u'/pnfs/gridka.de/cms$1', u'proto': u'srmv2', 'chain': 'pnfs'}
            while chains - done_chains:
                for test_proto in protos_json:  # Keep looking for what we need in all the protos
                    proto_name = test_proto['protocol'].lower()
                    if proto_name.lower() in chains:
                        for rule in test_proto['rules']:
                            entry = {'proto': proto_name.lower()}
                            entry.update({'path': rule['lfn'], 'out': rule['pfn']})
                            if 'chain' in rule:
                                chains.add(rule['chain'])  # If it's three layers deep
                                entry.update({'chain': rule['chain']})
                            tfc.append(entry)
                        done_chains.add(proto_name)


            # If we are building a _Test or _Temp instance
            if self.cms_type == "test" or self.cms_type == "temp":
                # We need to find the rule that applies to the special prefix
                # used for _Test(/store/test/rucio) or _Temp(/store/temp) and
                # adpat it as a prefix
                for rule in tfc:
                    prefix_regex = re.compile(rule['path'])
                    if self.cms_type =="test":
                        prefix_match = prefix_regex.match("/store/test/rucio")
                    else:
                        prefix_match = prefix_regex.match("/store/temp")
                    if prefix_match:
                        match_rule = rule['path']
                        g1 = prefix_match.group(1)
                        out = rule['out']
                        prefix = out.replace("$1", g1)
                        break

                # remove the part of the scheme://hostname...
                prefix_regex1 = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+):(\d+)/(.*)')
                prefix_regex2 = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+)/(.*)')
                prefix_match1 = prefix_regex1.match(prefix)
                prefix_match2 = prefix_regex2.match(prefix)
                if prefix_match1:
                    prefix = "/"+prefix_match1.group(4)
                elif prefix_match2:
                    prefix = "/"+prefix_match2.group(3)
                else:
                    # if we're here chances are the prefix didn't have a prefixed "scheme://"
                    logging.debug("couldn't find a scheme when calculating special prefix")

                proto['prefix']= prefix
                # Get rid of the TFC since were are using a prefix but don't get rid
                # of the web_service_path if it is there
                if 'web_service_path' in proto['extended_attributes']:
                    aux = proto['extended_attributes']['web_service_path']
                    proto['extended_attributes']= {'web_service_path':aux}
                else:
                    proto['extended_attributes']= None
            else:
                proto['extended_attributes']['tfc'] = tfc

        #pprint.pprint(proto)
        return algorithm, proto

    def _set_protocols(self):
        try:
            current_protocols = self.rcli.get_protocols(
                rse=self.rse_name
            )
        except (RSEProtocolNotSupported, RSENotFound):
            current_protocols = []

        # We need to get the new protocols sorted, so that the one
        # with the highest priority goes first, otherwise the priorites
        # get messed up.
        # when a protocol is removed from Rucio, the priorities of the remaining
        # protocols get adjusted so that there is a protocol with priority = 1
        sorted_new_protocols = sorted(self.protocols, key = lambda i:i['domains']['wan']['read'])
        for new_proto in sorted_new_protocols:
            protocol_unchanged = False
            for existing_proto in current_protocols:
                if existing_proto['scheme'] == new_proto['scheme']:
                    if new_proto == existing_proto:
                        protocol_unchanged = True
                    else:
                        if self.dry:
                            logging.info("Deleting definition which is not as expected (Dry run, skipping)")
                            logging.info(json.dumps(existing_proto, sort_keys=False, indent=4))
                            logging.info("expected (Dry run, skipping)")
                            logging.info(json.dumps(new_proto, sort_keys=False, indent=4))
                        else:
                            logging.info("Deleting definition which is not as expected:")
                            logging.info(json.dumps(existing_proto, sort_keys=False, indent=4))
                            logging.info("expected:")
                            logging.info(json.dumps(new_proto, sort_keys=False, indent=4))
                            try:
                                self.rcli.delete_protocols(rse=self.rse_name, scheme=new_proto['scheme'])
                            except RSEProtocolNotSupported:
                                logging.debug("Cannot remove protocol %s from %s", new_proto['scheme'], self.rse_name)

            if new_proto['scheme'] in ['srm', 'srmv2', 'gsiftp', 'root', 'davs'] and not protocol_unchanged:
                if self.dry:
                    logging.info('Adding %s to %s (Dry run, skipping)', new_proto['scheme'], self.rse_name)
                else:
                    logging.info('Adding %s to %s', new_proto['scheme'], self.rse_name)
                    self.rcli.add_protocol(rse=self.rse_name, params=new_proto)
        return

    def _create_rse(self):

        create = False

        try:
            rse = self.rcli.get_rse(self.rse_name)
        except RSENotFound:
            create = True

        if not create and rse['deterministic'] != self.settings['deterministic']:
            raise Exception("The rse %s was created with the wrong deterministic setting!",
                            self.rse_name)

        if create:
            if self.dry:
                logging.info('creating rse %s with deterministic %s and type %s. Dry run, skipping',
                             self.rse_name, self.settings['deterministic'], self.rucio_rse_type)
            else:
                self.rcli.add_rse(self.rse_name, deterministic=self.settings['deterministic'],
                                  rse_type=self.rucio_rse_type)
                logging.debug('created rse %s', self.rse_name)

        return create

    def update(self):
        """
        Creates, if needed, and updates the RSE according
        to CMS rules and PhEDEx data.
        """
        create_res = self._create_rse()

        attrs_res = self._set_attributes()
        proto_res = self._set_protocols()

        return create_res or attrs_res or proto_res
