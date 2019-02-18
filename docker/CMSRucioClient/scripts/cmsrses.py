#! /bin/env python
"""
Class definition for a CMS RSE. And script for updating RSE
"""

import argparse
import logging
import os

import re
import json

from phedex import PhEDEx, DEFAULT_PHEDEX_INST, DEFAULT_DASGOCLIENT,\
DEFAULT_DATASVC_URL, DEFAULT_PROTOCOL
from rucio.client.client import Client
from rucio.common.exception import RSEProtocolNotSupported, RSENotFound
from cmstfc import cmstfc

DEFAULT_RSETYPE = 'prod'
DEFAULT_SUFFIXES = {
    'real': '',
    'temp': '_Temp',
    'test': '_Test',
}

DOMAINS_BYTYPE = {
    'real': {'wan': {'read': 1, 'write': 0, 'third_party_copy': 1, 'delete': 0},
             'lan': {'read': 0, 'write': 0, 'delete': 0}},
    'test': {'wan': {'read': 1, 'write': 1, 'third_party_copy': 1, 'delete': 1},
             'lan': {'read': 0, 'write': 0, 'delete': 0}},
    'temp': {'wan': {'read': 1, 'write': 1, 'third_party_copy': 1, 'delete': 1},
             'lan': {'read': 0, 'write': 0, 'delete': 0}},
}

LFN2PFN_BYTYPE = {
    'real': 'cmstfc',
    'test': 'identity',
    'temp': 'hash',
}

EXCLUDE_TFC = r'\S+LoadTest\S*'

SE_PROBES_BYTYPE = {
    'real': ['/store/data/prod/file.root', '/store/mc/prod/file.root'],
    'test': ['/store/test/rucio/test/file.root'],
    #'temp': ['/store/temp/file.root'], # this will be the correct path in prod
    'temp': ['/store/test/rucio/temp/file.root']
}

SE_ADD_PREFIX_BYTYPE = {
    'real': '',
    'test': '/store/test/rucio',
    'temp': '/store/temp', # this will be the correct path in prod
    #'temp': '/store/test/rucio/temp/',
}

PNN_MATCH = re.compile(r'T(\d+)\_(\S{2})\_\S+')


class CMSRSE(object):
    """
    Wrapping the definition of a CMS RSE. Gathering the information
    from PhEDEx and translating them into the definition of a Rucio RSE
    for the different expected types: real, test, temp.
    """

    def __init__(self, pnn, account, auth_type=None, rsetype=DEFAULT_RSETYPE, suffix=None,
                 dry=False, fts=None, tier=None, lfn2pfn_algorithm=None, country=None,
                 attrs=None, seinfo=None, tfc=None, tfc_exclude=EXCLUDE_TFC, domains=None,
                 space_token=None, add_prefix=None, proto=DEFAULT_PROTOCOL,
                 instance=DEFAULT_PHEDEX_INST, dasgoclient=DEFAULT_DASGOCLIENT,
                 datasvc=DEFAULT_DATASVC_URL):

        attrs = attrs or []

        self.pnn = pnn
        self.rsetype = rsetype
        if suffix is None:
            suffix = DEFAULT_SUFFIXES[rsetype]

        self.suffix = suffix
        self.rsename = pnn + self.suffix

        if tfc and os.path.isdir(tfc):
            self.tfc = tfc + '/' + pnn + '/PhEDEx/storage.xml'
        else:
            self.tfc = tfc

        self.pcli = PhEDEx(instance=instance, dasgoclient=dasgoclient, datasvc=datasvc)
        self.rcli = Client(account=account, auth_type=auth_type)

        self.dry = dry

        self._get_attributes(fts, tier, lfn2pfn_algorithm, country, attrs)

        self._get_settings()

        self._get_protocol(seinfo, add_prefix, tfc_exclude, domains, space_token, proto)


    def _get_attributes(self, fts, tier, lfn2pfn_algorithm, country, xattrs):
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

        attrs = {}
        attrs['fts'] = fts or self.pcli.fts(self.pnn)[0]

        pnn_match = PNN_MATCH.match(self.pnn)

        attrs['tier'] = tier or pnn_match.group(1)

        attrs['country'] = country or pnn_match.group(2)

        attrs['lfn2pfn_algorithm'] = lfn2pfn_algorithm or LFN2PFN_BYTYPE[self.rsetype]

        attrs[self.rsename] = 'True'

        attrs['pnn'] = self.pnn

        attrs['cms_type'] = self.rsetype

        for (key, value) in xattrs:
            attrs[key] = value

        self.attrs = attrs


    def _set_attributes(self):
        try:
            rattrs = self.rcli.list_rse_attributes(rse=self.rsename)
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
                              key, rattrs[key], value, self.rsename)
                changed = True
                if self.dry:
                    logging.info('setting attribute %s to value %s for rse %s. Dry run, skipping',
                                 key, value, self.rsename)
                else:
                    self.rcli.add_rse_attribute(rse=self.rsename, key=key, value=value)

        return changed


    def _get_settings(self):
        """
        Get expected settings for the RSE
        (so far only deterministic vs non-deterministic)
        """
        self.settings = {}
        if self.attrs['lfn2pfn_algorithm'] == 'hash':
            self.settings['deterministic'] = False
        else:
            self.settings['deterministic'] = True


    def _check_lfn2pfn(self):
        """
        Checks that lfn2pfn works properly
        """
        for lfn in SE_PROBES_BYTYPE[self.rsetype]:

            # this is what rucio does
            pfn = self.proto['scheme']  + '://' + self.proto['hostname'] +\
                ':' +  str(self.proto['port'])

            if 'web_service_path' in self.proto['extended_attributes']:
                pfn = pfn + self.proto['extended_attributes']['web_service_path']


            pfn = pfn + '/' + cmstfc('cms', lfn, None, None, self.proto)

            # this should match dataservice pfn, modulo some normalization
            # (e.g.: adding the port number)
            pfn_datasvc = []
            pfn_datasvc.append(self.pcli.lfn2pfn(
                pnn=self.pnn, lfn=lfn, tfc=self.tfc,
                protocol=self.proto['extended_attributes']['tfc_proto']))
            pfn_datasvc.append(pfn_datasvc[0].replace(
                self.proto['hostname'],
                self.proto['hostname'] + ':' + str(self.proto['port'])
            ))

            if pfn not in pfn_datasvc:
                raise Exception("rucio and datasvc lfn2pfn mismatch, rucio: %s ; datasvc: %s" %
                                (pfn, pfn_datasvc))

            logging.debug("checking lfn2pfn ok %s", pfn)


    def _get_protocol(self, seinfo, add_prefix, exclude, domains, token, proto):
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


        seinfo = seinfo or self.pcli.seinfo(pnn=self.pnn, probes=SE_PROBES_BYTYPE[self.rsetype],
                                            protocol=proto, tfc=self.tfc)

        if self.tfc is not None and self.tfc[0] == '/':
            pnn_arg = self.tfc
            self.tfc = None
        else:
            pnn_arg = self.pnn

        self.tfc = self.tfc or self.pcli.tfc(
            pnn=pnn_arg, dump=False,
            exclude=exclude, normalize=seinfo, proto=proto)

        domains = domains or DOMAINS_BYTYPE[self.rsetype]

        self.proto = {
            'scheme': seinfo['protocol'],
            'hostname': seinfo['hostname'],
            'port': seinfo['port'],
            'extended_attributes': {},
            'domains': domains
        }

        if 'webpath' in seinfo:
            self.proto['extended_attributes']['web_service_path'] = seinfo['webpath']

        if self.attrs['lfn2pfn_algorithm'] == 'cmstfc':
            self.proto['prefix'] = '/'
            self.proto['extended_attributes']['tfc_proto'] = proto
            self.proto['extended_attributes']['tfc'] = self.tfc
            self._check_lfn2pfn()
        else:
            self.proto['prefix'] = seinfo['prefix']

        if add_prefix is None:
            add_prefix = SE_ADD_PREFIX_BYTYPE[self.rsetype]

        self.proto['prefix'] += add_prefix

        if token:
            self.proto['extended_attributes']['space_token'] = token

        if self.proto['extended_attributes'] == {}:
            self.proto['extended_attributes'] = None

        self.proto['impl'] = 'rucio.rse.protocols.gfalv2.Default'


    def _set_protocol(self):
        try:
            rprotos = self.rcli.get_protocols(
                rse=self.rsename
            )
        except (RSEProtocolNotSupported, RSENotFound):
            rprotos = []

        rproto = {}

        for item in rprotos:
            if item['scheme'] == self.proto['scheme']:
                rproto = item
                break

        update = False
        if self.proto != rproto:
            logging.debug("protocol definition not as expected: rucio=%s, expected=%s",
                          str(rproto), str(self.proto))
            update = True

        if update:
            if self.dry:
                logging.info('Modifying protocol to %s. Dry run, skipping', str(self.proto))
                return update

            try:
                self.rcli.delete_protocols(rse=self.rsename, scheme=self.proto['scheme'])
            except RSEProtocolNotSupported:
                logging.debug("Cannot remove protocol (scheme, rse) = (%s,%s)",
                              self.proto['scheme'], self.rsename)

            self.rcli.add_protocol(rse=self.rsename, params=self.proto)

        return update


    def _create_rse(self):

        create = False

        try:
            rse = self.rcli.get_rse(self.rsename)
        except RSENotFound:
            create = True

        if not create and rse['deterministic'] != self.settings['deterministic']:
            raise Exception("The rse %s was created with the wrong deterministic setting!",
                            self.rsename)

        if create:
            if self.dry:
                logging.info('creating rse %s with deterministic %s. Dry run, skipping',
                             self.rsename, self.settings['deterministic'])
            else:
                self.rcli.add_rse(self.rsename, deterministic=self.settings['deterministic'])
                logging.debug('created rse %s', self.rsename)

        return create


    def update(self):
        """
        Creates, if needed, and updates the RSE according
        to CMS rules and PhEDEx data.
        """

        create_res = self._create_rse()

        attrs_res = self._set_attributes()
        proto_res = self._set_protocol()

        return create_res or attrs_res or proto_res


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='''CLI for updating a CMS RSE''',
    )
    PARSER.add_argument('-v', '--verbose', dest='debug', action='store_true',
                        help='increase output verbosity')
    PARSER.add_argument('-t', '--dry', dest='dry', action='store_true',
                        help='only printout what would have been done')
    PARSER.add_argument('--inst', dest='instance', default=DEFAULT_PHEDEX_INST,
                        help='PhEDEx instance, default %s.' % DEFAULT_PHEDEX_INST)
    PARSER.add_argument('--dasgoclient', dest='dasgoclient', default=DEFAULT_DASGOCLIENT,
                        help='DAS client to use. default %s.' % DEFAULT_DASGOCLIENT)
    PARSER.add_argument('--pnn', dest='pnn', help='PhEDEx node name. Can be multiple.\
                        "all" to loop on all pnn',
                        action='append', default=[])
    PARSER.add_argument('--type', dest='type', help='RSE Type. Can be multiple.',
                        action='append', default=[])
    PARSER.add_argument('--suffix', dest='suffix', default=None,
                        help='RSE suffix. If missing, inferred by the RSE type.')
    PARSER.add_argument('--fts', dest='fts', default=None,
                        help='FTS server. If missing, taken from PhEDEx.')
    PARSER.add_argument('--tier', dest='tier', default=None,
                        help='PNN Tier. If missing, taken from PhEDEx.')
    PARSER.add_argument('--country', dest='country', default=None,
                        help='Country Flag. If missing, taken from PhEDEx.')
    PARSER.add_argument('--lfn2pfn', dest='lfn2pfn', default=None,
                        help='lfn2pfn algorithm. If missing, inferred by the RSE type.')
    # TODO: correct attrs to be a multiple key value option
    PARSER.add_argument('--attr', dest='attrs', default=None,
                        help='dictionnary of extra RSE attributes. Default None')
    PARSER.add_argument('--seinfo', dest='seinfo', default=None,
                        help='SE informations. If missing, taken from PhEDEx.')
    PARSER.add_argument('--tfc', dest='tfc', default=None,
                        help='TFC rules. If missing, taken from PhEDEx.\
                        It can be also the path to a xml file or to the SITECONF dir.')
    PARSER.add_argument('--domains', dest='domains', default=None,
                        help='RSE domains. If missing, inferred by the RSE type.')
    PARSER.add_argument('--account', dest='account', default=os.environ['RUCIO_ACCOUNT'],
                        help='Rucio accoun. default RUCIO_ACCOUNT')
    PARSER.add_argument('--space_token', dest='token', default=None,
                        help='Space Token. default None')
    PARSER.add_argument('--proto', dest='proto', default=DEFAULT_PROTOCOL,
                        help='Protocol. default %s' % DEFAULT_PROTOCOL)
    PARSER.add_argument('--tfc_exclude', dest='tfc_exclude', default=EXCLUDE_TFC,
                        help='Regexp for rule paths to be excluded from TFC. default %s'
                        % EXCLUDE_TFC)
    PARSER.add_argument('--select', dest='select', action='append', default=None,
                        help='selecting regexps for pnn listing. Can be multiple.')
    PARSER.add_argument('--exclude', dest='exclude', action='append', default=None,
                        help='excluding regexps for pnn listing. Can be multiple.')

    OPTIONS = PARSER.parse_args()

    if OPTIONS.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if OPTIONS.domains:
        OPTIONS.domains = json.loads(OPTIONS.domains.replace("'", '"'))

    if OPTIONS.seinfo is not None:
        OPTIONS.seinfo = json.loads(OPTIONS.seinfo.replace("'", '"'))

    if 'all' in OPTIONS.pnn:
        OPTIONS.pnn = PhEDEx(instance=OPTIONS.instance).pnns(
            select=OPTIONS.select,
            exclude=OPTIONS.exclude
        )

    CHANGED = []
    TOT = []

    for node_name in OPTIONS.pnn:
        for rse_type in OPTIONS.type:
            logging.info('Starting pnn %s and type %s', node_name, rse_type)
            RSE = CMSRSE(
                pnn=node_name, rsetype=rse_type, account=OPTIONS.account, dry=OPTIONS.dry,
                suffix=OPTIONS.suffix, fts=OPTIONS.fts, tier=OPTIONS.tier,
                lfn2pfn_algorithm=OPTIONS.lfn2pfn, country=OPTIONS.country,
                seinfo=OPTIONS.seinfo, tfc=OPTIONS.tfc, domains=OPTIONS.domains,
                attrs=OPTIONS.attrs, space_token=OPTIONS.token, tfc_exclude=OPTIONS.tfc_exclude,
                instance=OPTIONS.instance, dasgoclient=OPTIONS.dasgoclient, proto=OPTIONS.proto
            )
            if RSE.update():
                logging.warning('RSE %s corresponding to pnn %s and type %s changed',
                                RSE.rsename, RSE.pnn, RSE.rsetype)
                CHANGED.append(RSE.rsename)

            TOT.append(RSE.rsename)

    logging.info("%d RSEs considered: %s", len(TOT),
                 ', '.join(TOT))
    logging.info("%d RSEs have changed: %s", len(CHANGED),
                 ', '.join(CHANGED))
