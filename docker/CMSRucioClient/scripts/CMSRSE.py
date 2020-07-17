#! /bin/env python3

"""
Class definition for a CMS RSE. And script for updating RSE
"""
import logging
import pdb
import pprint
import re

from rucio.client.client import Client
from rucio.common.exception import RSEProtocolNotSupported, RSENotFound

APPROVAL_REQUIRED = ['T1_DE_KIT_Tape', 'T1_ES_PIC_Tape', 'T1_RU_JINR_Tape', 'T1_UK_RAL_Tape', 'T1_US_FNAL_Tape']
DOMAINS_BY_TYPE = {
    'real': {'wan': {'read': 1, 'write': 0, 'third_party_copy': 1, 'delete': 0},
             'lan': {'read': 0, 'write': 0, 'delete': 0}},
    'test': {'wan': {'read': 1, 'write': 1, 'third_party_copy': 1, 'delete': 1},
             'lan': {'read': 0, 'write': 0, 'delete': 0}},
    'temp': {'wan': {'read': 1, 'write': 1, 'third_party_copy': 1, 'delete': 1},
             'lan': {'read': 0, 'write': 0, 'delete': 0}},
}
RUCIO_PROTOS = ['SRMv2']
IMPL_MAP = {'SRMv2': 'rucio.rse.protocols.gfalv2.Default'}


class CMSRSE:
    """
    Wrapping the definition of a CMS RSE. Gathering the information
    from PhEDEx and translating them into the definition of a Rucio RSE
    for the different expected types: real, test, temp.
    """

    def __init__(self, json,
                 dry=False, ):

        self.rcli = Client()
        self.dry = dry
        self.json = json

        self.cms_type = 'real'

        self.protocols = []
        self.attrs = {}
        self.settings = {}

        self.rucio_rse_type = json['type'].upper()
        self.rse_name = json['rse']

        # pdb.set_trace()

        self._get_attributes()
        self.attrs['fts'] = json['fts'][0]

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
        attrs['cms_type'] = self.cms_type

        # attrs['lfn2pfn_algorithm'] = lfn2pfn_algorithm or LFN2PFN_BYTYPE[self.rsetype]
        # Get from JSON with an algorithm

        # If this is even needed
        self.settings['deterministic'] = True

        # self._get_protocol(seinfo, add_prefix, tfc_exclude, domains, space_token, proto)
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

    # def _check_lfn2pfn(self):
    #     """
    #     Checks that lfn2pfn works properly
    #     """
    #     for lfn in SE_PROBES_BYTYPE[self.cms_type]:
    #
    #         # this is what rucio does
    #         pfn = self.proto['scheme'] + '://' + self.proto['hostname'] + \
    #               ':' + str(self.proto['port'])
    #
    #         if 'web_service_path' in self.proto['extended_attributes']:
    #             pfn = pfn + self.proto['extended_attributes']['web_service_path']
    #
    #         pfn = pfn + '/' + cmstfc('cms', lfn, None, None, self.proto)
    #
    #         # this should match dataservice pfn, modulo some normalization
    #         # (e.g.: adding the port number)
    #         pfn_datasvc = []
    #
    #         wo_port = self.pcli.lfn2pfn(
    #             pnn=self.pnn, lfn=lfn, tfc=self.tfc,
    #             protocol=self.proto['extended_attributes']['tfc_proto'])
    #         wo_port = re.sub('/+', '/', wo_port)
    #         w_port = wo_port.replace(
    #             self.proto['hostname'],
    #             self.proto['hostname'] + ':' + str(self.proto['port'])
    #         )
    #
    #         # Get rid of ALL multiple slashes, including separating protocol from host (valid for comparison only)
    #         pfn_datasvc.append(wo_port)
    #         pfn_datasvc.append(w_port)
    #         pfn = re.sub('/+', '/', pfn)
    #
    #         if pfn not in pfn_datasvc:
    #             raise Exception("rucio and datasvc lfn2pfn mismatch, rucio: %s ; datasvc: %s" %
    #                             (pfn, pfn_datasvc))
    #
    #         logging.debug("checking lfn2pfn ok %s", pfn)

    # def _get_protocol(self, seinfo, add_prefix, exclude, domains, token, proto):
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

        domains = DOMAINS_BY_TYPE[self.cms_type]

        if proto_json.get('prefix', None):
            """
            The simple case where all we have is a prefix. This just triggers the identity algorithm 
            with some simple settings
            """

            algorithm = 'identity'
            prefix_regex = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+):(\d+)(\/.*\=)(.*)')
            prefix_match = prefix_regex.match(proto_json['prefix'])

            scheme = prefix_match.group(1)
            hostname = prefix_match.group(2)
            port = prefix_match.group(3)
            extended_attributes = {'web_service_path': prefix_match.group(4)}
            prefix = prefix_match.group(5)

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
            prefix_regex = re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+):(\d+)(\/.*\=)(.*)')
            rules = proto_json['rules']
            for rule in rules:
                # On first pass, fill in the basic information we need before pulling out the rules
                # including any first level chain names
                if rule.get('chain', None):
                    chains.add(rule['chain'])
                try:
                    prefix_match = prefix_regex.match(rule['pfn'])
                    proto['scheme'] = prefix_match.group(1)
                    proto['hostname'] = prefix_match.group(2)
                    proto['port'] = prefix_match.group(3)
                    proto['extended_attributes'] = {'tfc_proto': protocol_name.lower(),
                                                    'web_service_path': prefix_match.group(4)}
                    proto['prefix'] = '/'
                    proto['domains'] = domains
                    proto['impl'] = IMPL_MAP[protocol_name]
                except NotImplementedError:
                    print('Replace with actual no match error')
                    raise

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

            proto['extended_attributes']['tfc'] = tfc

        pprint.pprint(proto)
        return algorithm, proto

    def _set_protocols(self):
        try:
            current_protocols = self.rcli.get_protocols(
                rse=self.rse_name
            )
        except (RSEProtocolNotSupported, RSENotFound):
            current_protocols = []

        for new_proto in self.protocols:

            for existing_proto in current_protocols:
                if existing_proto['scheme'] == new_proto['scheme']:
                    if new_proto != existing_proto:
                        logging.info("Deleting definition which is not as expected: \nrucio=%s  \nexpected=%s",
                                     str(existing_proto), str(new_proto))
                        try:
                            self.rcli.delete_protocols(rse=self.rse_name, scheme=new_proto['scheme'])
                        except RSEProtocolNotSupported:
                            logging.debug("Cannot remove protocol %s from %s", new_proto['scheme'], self.rse_name)

            if new_proto['scheme'] in ['srm', 'srmv2', 'gsiftp']:
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
