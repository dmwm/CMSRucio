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
    'prod-real': {
        'wan': {'read': 1, 'write': 1, 'third_party_copy_write': 1, 'third_party_copy_read': 1,
                'delete': 1},
        'lan': {'read': 0, 'write': 0, 'delete': 0}},
    'int-real': {
        'wan': {'read': 1, 'write': 1, 'third_party_copy_write': 1, 'third_party_copy_read': 1,
                'delete': 1},
        'lan': {'read': 0, 'write': 0, 'delete': 0}},
    'test': {
        'wan': {'read': 1, 'write': 1, 'third_party_copy_write': 1, 'third_party_copy_read': 1,
                'delete': 1},
        'lan': {'read': 0, 'write': 0, 'delete': 0}},
    'temp': {
        'wan': {'read': 1, 'write': 1, 'third_party_copy_write': 1, 'third_party_copy_read': 1,
                'delete': 1},
        'lan': {'read': 0, 'write': 0, 'delete': 0}},
}
RUCIO_PROTOS = ['SRMv2', 'XRootD', 'WebDAV']
PROTO_WEIGHT_TPC = {'WebDAV': 1, 'XRootD': 3, 'SRMv2': 2}
PROTO_WEIGHT_RWD = {'WebDAV': 1, 'XRootD': 3, 'SRMv2': 2}

IMPL_MAP = {'SRMv2': 'rucio.rse.protocols.gfal.Default',
            'XRootD': 'rucio.rse.protocols.gfal.Default',
            'WebDAV': 'rucio.rse.protocols.gfal.Default'}
DEFAULT_PORTS = {'gsiftp': 2811, 'root': 1094, 'davs': 443}


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

        xattrs = {}

        # If we are building a _Test or _Temp instance add the special prefix
        if cms_type == "test":
            self.rse_name = json['rse']+"_Test"
        elif cms_type == "temp":
            self.rse_name = json['rse']+"_Temp"
        else:
            self.rse_name = json['rse']
            if json.get('loadtest', None) is not None:
                xattrs['loadtest'] = json['loadtest']

        xattrs['fts'] = ','.join(json['fts'])
        self._get_attributes(xattrs=xattrs)

    """
    Parses either a prefix or a pfn within a rule in the storage.json
    @url is something like:
    - root://redirector.t2.ucsd.edu:1094//$1
    - davs://xrootd.ultralight.org:1094
    - srm://cmsrm-se01.roma1.infn.it:8443/srm/managerv2?SFN=/pnfs/roma1.infn.it/data/cms
    @protocol_name. Is one of RUCIO_PROTOS = ['SRMv2', 'XRootD', 'WebDAV']
    @is_prefix. Tell use whethere we are analyzing a prefix or a rule from the TFC
    """

    def _parse_url(self, url, protocol_name, is_prefix):
        error = False
        prefix_regexp_list = [
            {'type': 1, 'regexp': re.compile(
                r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+):(?P<port>\d+)(?P<service_p>\/.*\=)(?P<prefix>.*)')},
            {'type': 2, 'regexp': re.compile(
                r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+):(?P<port>\d+)/(?P<prefix>.*)')},
            {'type': 3, 'regexp': re.compile(r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+):(?P<port>\d+)')},
            {'type': 4, 'regexp': re.compile(r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+)/(?P<prefix>.*)')},
            {'type': 5, 'regexp': re.compile(r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+)')}]

        regexp_type = 0
        for prefix_regexp in prefix_regexp_list:
            prefix_regexp_match = prefix_regexp['regexp'].match(url)
            if prefix_regexp_match:
                regexp_type = prefix_regexp['type']
                break

        scheme = prefix_regexp_match.group('scheme')
        hostname = prefix_regexp_match.group('host')

        if regexp_type == 1:
            # logging.debug("Looking for prefix with web service path")
            port = prefix_regexp_match.group('port')
            prefix = prefix_regexp_match.group('prefix')
            if is_prefix:
                extended_attributes = {'web_service_path': prefix_regexp_match.group('service_p')}
            else:
                extended_attributes = {'tfc_proto': protocol_name.lower(),
                                       'web_service_path': prefix_regexp_match.group('service_p')}

        elif regexp_type == 2:
            # logging.debug("Looking for prefix with port")
            port = prefix_regexp_match.group('port')
            prefix = '/' + prefix_regexp_match.group('prefix')
            if is_prefix:
                extended_attributes = None
            else:
                extended_attributes = {'tfc_proto': protocol_name.lower()}

        elif regexp_type == 3:
            # logging.debug("Looking for port and no prefix")
            port = prefix_regexp_match.group('port')
            prefix = '/'
            if is_prefix:
                extended_attributes = None
            else:
                extended_attributes = {'tfc_proto': protocol_name.lower()}

        elif regexp_type == 4:
            # logging.debug("Looking for a prefix and no port")
            port = DEFAULT_PORTS[scheme]
            prefix = '/' + prefix_regexp_match.group('prefix')
            if is_prefix:
                extended_attributes = None
            else:
                extended_attributes = {'tfc_proto': protocol_name.lower()}

        elif regexp_type == 5:
            # logging.debug("Looking for no prefix and no port")
            port = DEFAULT_PORTS[scheme]
            prefix = '/'
            if is_prefix:
                extended_attributes = None
            else:
                extended_attributes = {'tfc_proto': protocol_name.lower()}

        else:
            # logging.error("Cannot parse the following url: "+url)
            error = True

        if error:
            return None, None, None, None, None

        # When dealing with rules as opposed to a prefix, the prefix is always "/"
        if not is_prefix:
            prefix = "/"

        return scheme, hostname, port, prefix, extended_attributes

    # Make sure that when buidling a TFC all the rules have the same schems, hostname
    # and port thant the protocol, otherwise the URL gets wrongly calculated
    def _verify_and_fix(self, rule_pfn, proto):
        status = None
        pfn = None
        rule_regex1 = re.compile(r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+):(?P<port>\d+)/(?P<prefix>.*)')
        rule_regex2 = re.compile(r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+)/(?P<prefix>.*)')
        rule_match1 = rule_regex1.match(rule_pfn)
        rule_match2 = rule_regex2.match(rule_pfn)
        if rule_match1:
            # The rule has scheme, hostname and port. Make sure they are the
            # exact same as in 'proto'
            scheme = rule_match1.group('scheme')
            hostname = rule_match1.group('host')
            port = rule_match1.group('port')
            if scheme == proto['scheme'] and hostname == proto['hostname'] and int(port) == proto['port']:
                status = "ok"
                pfn = rule_pfn
            else:
                status = "error"
        elif rule_match2:
            # The rule has scheme and hostname but not port. Add the port from 'proto'
            scheme = rule_match2.group('scheme')
            hostname = rule_match2.group('host')
            prefix = rule_match2.group('prefix')
            if scheme == proto['scheme'] and hostname == proto['hostname']:
                status = "changed"
                pfn = scheme+"://"+hostname+":"+str(proto['port'])+"/"+prefix
            else:
                status = "error"
        else:
            # In this case we assume that the rule was a simple prefix
            status = "ok"
            pfn = rule_pfn

        return status, pfn

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

        # We should no longer be turning reaper back on
        # if self.cms_type in ['test', 'prod-real'] and not self.rse_name.endswith('_Tape'):
        #     attrs['reaper'] = True

        self.protocols = []
        protos_json = self.json['protocols']
        for proto_json in protos_json:
            algorithm, proto = self._get_protocol(proto_json, protos_json)
            if algorithm:
                self.protocols.append(proto)
                attrs['lfn2pfn_algorithm'] = algorithm
            elif proto_json['protocol'] in RUCIO_PROTOS:
                logging.info("Not adding protocol: "+proto_json['protocol'])

        if self.rse_name in APPROVAL_REQUIRED:
            attrs['requires_approval'] = 'True'

        for key, value in xattrs.items():
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
        access = proto_json['access']
        algorithm = None
        proto = {}

        if protocol_name not in RUCIO_PROTOS:
            logging.debug("Not adding unsupported rucio protocol: "+protocol_name)
            return algorithm, proto
        if access not in ['global-rw', 'global-ro']:
            logging.debug(
                "Only global-rw and global-ro access are supported. Not adding protocol: " + protocol_name +
                " with access: " + access)
            return algorithm, proto

        domains = copy.deepcopy(DOMAINS_BY_TYPE[self.cms_type])
        # Set the priorities for Read, Write, Delete and TPC for each protocol
        # IMPORTANT: set SRMv2 Read, Write and Delete as Priority #1 because ASO needs it that way
        # TPC is the only one we care about.
        try:
            for method, weight in domains['wan'].items():
                if weight and protocol_name in PROTO_WEIGHT_TPC:
                    if method.startswith("third_party_copy"):
                        if access == 'global-ro' and method != 'third_party_copy_read':
                            domains['wan'][method] = 0
                        else:
                            domains['wan'][method] = PROTO_WEIGHT_TPC[protocol_name]

                    else:
                        if access == 'global-ro' and method != 'read':
                            domains['wan'][method] = 0
                        else:
                            domains['wan'][method] = PROTO_WEIGHT_RWD[protocol_name]
        except KeyError:
            # We're trying to modify an unknown protocol somehow
            logging.error("Unknown protocol: "+protocol_name)

        if proto_json.get('prefix', None):
            """
            The simple case where all we have is a prefix. This just triggers the identity algorithm
            with some simple settings
            """

            algorithm = 'cmstfc'
            scheme, hostname, port, prefix, extended_attributes = self._parse_url(
                proto_json['prefix'], protocol_name, True)

            # If we cannot parse the prefix correctly, let's better not try to configure this protocol
            if scheme is None:
                logging.error("Cannot parse prefix: "+proto_json['protocol'])
                return None, None

            # Make sure that prefix always ends with "/"
            if prefix[len(prefix) - 1] != "/":
                prefix = prefix + "/"

            # if we are building a _Test instance add the specia prefix
            if self.cms_type == "test":
                prefix = prefix + "store/test/rucio/"

            elif self.cms_type == "int-real":
                prefix = prefix + "store/test/rucio/int/"

            elif self.cms_type == "temp":
                prefix = prefix + "store/temp/"

            proto = {
                'scheme': scheme,
                'hostname': hostname,
                'port': int(port),
                'extended_attributes': extended_attributes,
                'domains': domains,
                'prefix': prefix,
                'impl': IMPL_MAP[protocol_name]
            }
        elif proto_json.get('rules', None):
            """
            Instead we have a full set of TFC rules which we need to gather
            """
            chains = {protocol_name}
            done_chains = set()
            tfc = []

            algorithm = 'cmstfc'
            rules = proto_json['rules']
            for rule in rules:
                # On first pass, fill in the basic information we need before pulling out the rules
                # including any first level chain names
                if rule.get('chain', None):
                    chains.add(rule['chain'])
                scheme, hostname, port, prefix, extended_attributes = self._parse_url(rule['pfn'], protocol_name, False)

                # If we couldn't parse a Rule, better not configure this protocol
                if scheme is None:
                    logging.error("Cannot parse rules: "+proto_json['protocol'])
                    return None, None

                proto = {
                    'scheme': scheme,
                    'hostname': hostname,
                    'port': int(port),
                    'extended_attributes': extended_attributes,
                    'domains': domains,
                    'prefix': prefix,
                    'impl': IMPL_MAP[protocol_name]
                }

            # Now go through all the protocols including ones we were not interested in at first and get rules
            # Turn {"protocol": "SRMv2",
            #       "access": "global-rw",
            #         "rules": [ {"lfn": "/+(.*)", "pfn": "srm://SRM_URL=/$1", "chain": "pnfs"} ]
            #      },
            # into {u'path': u'(.*)', u'out': u'/pnfs/gridka.de/cms$1', u'proto': u'srmv2', 'chain': 'pnfs'}
            while chains - done_chains:
                for test_proto in protos_json:  # Keep looking for what we need in all the protos
                    proto_name = test_proto['protocol']
                    if proto_name in chains:
                        for rule in test_proto['rules']:
                            entry = {'proto': proto_name.lower()}
                            # make sure that the rule has the exact same scheme, hostname and port as 'proto'
                            status, rule_pfn = self._verify_and_fix(rule['pfn'], proto)
                            if status == "ok" or status == "changed":
                                entry.update({'path': rule['lfn'], 'out': rule_pfn})
                            else:
                                logging.warning(
                                    "the 'scheme' and/or 'hostname' is different in the rule: " + str(rule['pfn']) +
                                    " than in the protocol : " + str(proto_json['protocol']))
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
                    if self.cms_type == "test":
                        prefix_match = prefix_regex.match("/store/test/rucio/")
                    else:
                        prefix_match = prefix_regex.match("/store/temp/")
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
                    # if we're here chances are that the prefix didn't have a prefixed "scheme://"
                    logging.debug("couldn't find a scheme when calculating special prefix")

                # Make sure that prefix always ends with "/"
                if prefix[len(prefix) - 1] != "/":
                    prefix = prefix + "/"

                proto['prefix'] = prefix
                # Get rid of the TFC since were are using a prefix but don't get rid
                # of the web_service_path if it is there
                if 'web_service_path' in proto['extended_attributes']:
                    aux = proto['extended_attributes']['web_service_path']
                    proto['extended_attributes'] = {'web_service_path': aux}
                else:
                    proto['extended_attributes'] = None
            else:
                proto['extended_attributes']['tfc'] = tfc

        # pprint.pprint(proto)
        return algorithm, proto

    def _set_protocols(self):
        try:
            current_protocols = self.rcli.get_protocols(
                rse=self.rse_name
            )
        except (RSEProtocolNotSupported, RSENotFound):
            current_protocols = []

        new_changes = False
        # We need to get the new protocols sorted, so that the one
        # with the highest priority goes first, otherwise the priorites
        # get messed up.
        # when a protocol is removed from Rucio, the priorities of the remaining
        # protocols get adjusted so that there is a protocol with priority = 1
        sorted_new_protocols = sorted(self.protocols, key=lambda i: i['domains']['wan']['read'])
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
                    new_changes = True
                else:
                    logging.info('Adding %s to %s', new_proto['scheme'], self.rse_name)
                    self.rcli.add_protocol(rse=self.rse_name, params=new_proto)
                    new_changes = True

        # delete protocols that are not in the new list
        updated_current_protocols = self.rcli.get_protocols(rse=self.rse_name)
        updated_protocols_set = set([proto['scheme'] for proto in updated_current_protocols])
        gitlab_protocols_set = set([proto['scheme'] for proto in self.protocols])

        if updated_protocols_set != gitlab_protocols_set:
            for proto in updated_current_protocols:
                if proto['scheme'] not in gitlab_protocols_set:
                    if self.dry:
                        logging.info('Deleting %s from %s (Dry run, skipping)', proto['scheme'], self.rse_name)
                        new_changes = True
                    else:
                        logging.info('Deleting %s from %s', proto['scheme'], self.rse_name)
                        self.rcli.delete_protocols(rse=self.rse_name, scheme=proto['scheme'])
                        new_changes = True

        return new_changes

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

    # This can be used to see how a protocol would be set for a given RSE
    # @scheme the scheme of the protocol we want to see e.g. srm, gsiftp, davs
    def show_proto(self, scheme):
        if scheme in ["srm", "gsiftp"]:
            schemes = ["srm", "gsiftp"]
        elif scheme == "all":
            schemes = ["srm", "gsiftp", "root", "davs"]
        else:
            schemes = [scheme]
        for proto in self.protocols:
            if proto['scheme'] in schemes:
                print(json.dumps(proto, sort_keys=False, indent=4))