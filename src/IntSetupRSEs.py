#! /usr/bin/env python3

import copy
import logging
import pdb
import re

from rucio.client import Client
from rucio.common.exception import RSEProtocolNotSupported, RSENotFound, Duplicate

RSES_TO_SET = ['T1_US_FNAL_Disk', 'T2_CH_CERN', 'T2_US_Purdue',]
RSES_WITHOUT_INPUT = ['T3_US_NERSC', 'T2_US_Wisconsin', 'T2_US_Nebraska', 'T2_US_UCSD'] 
RSES_ONLY_INPUT = ['T1_US_FNAL_Tape', 'T0_CH_CERN_Tape', 'T1_IT_CNAF_Tape']

#RSES_TO_SET = ['T1_US_FNAL_Disk']
#RSES_WITHOUT_INPUT = []
#RSES_ONLY_INPUT = []




INT_SETTINGS = {'availability_read': True, 'availability_write': True, 'availability_delete': True}
INT_ATTRIBUTES = {'ddm_quota': 0, 'cms_type': 'int', 'reaper': True}
INPUT_SETTINGS = {'availability_read': True, 'availability_write': False, 'availability_delete': False}
INPUT_ATTRIBUTES = {'ddm_quota': 0, 'cms_type': 'input', 'reaper': False}

DEFAULT_PORTS = {'gsiftp': 2811, 'root': 1094, 'davs': 1094}


def create_rse(client, rse_name, rse_type='DISK', settings=None):
    """
    Create an RSE if it does not exist

    :param client: The Rucio client object
    :param rse_name:   The RSE name
    :param rse_type:   DISK or TAPE
    :param settings:   Settings for RSE
    """

    settings = settings or {}

    try:
        client.add_rse(rse_name, deterministic=True, rse_type=rse_type)
    except Duplicate:
        pass

    client.update_rse(rse_name, parameters=settings)


def overwrite_protocols(client, rse_name, new_protocols=None):
    """
    Delete and restore the protocols to an RSE based on what is in new_protocols

    :param client: The Rucio client object
    :param rse_name:   The RSE name
    :param new_protocols:  The dictionary of protocols
    """
    new_protocols = new_protocols or []

    current_protocols = []
    try:
        current_protocols = client.get_protocols(rse=rse_name)
    except RSEProtocolNotSupported:  # RSE has no protocols
        logging.debug("No protocols exist for %s", rse_name)

    for proto in current_protocols:
        scheme = proto['scheme']
        try:
            client.delete_protocols(rse=rse_name, scheme=scheme)
        except RSEProtocolNotSupported:
            logging.debug("Cannot remove protocol %s from %s", scheme, rse_name)

    for new_proto in new_protocols:
        if new_proto['scheme'] in ['srm', 'srmv2', 'gsiftp', 'root', 'davs']:
            try: 
                print('Adding %s to %s' % (new_proto['scheme'], rse_name))
                client.add_protocol(rse=rse_name, params=new_proto)
            except Duplicate:
                print('Could not add additional %s to %s' % (new_proto['scheme'], rse_name))


def set_rse_attributes(client, rse_name, attributes):
    attributes = attributes or []

    # FIXME: This might be used later. If not delete
    try:
        current_attributes = client.list_rse_attributes(rse=rse_name)
    except RSENotFound:
        current_attributes = {}

    # changed = False

    for (key, value) in attributes.items():
        print('Setting %s=%s for %s' % (key, value, rse_name))
        client.add_rse_attribute(rse=rse_name, key=key, value=value)

    # return changed


def rewrite_protocols(protocols, pfns, read_write=False, enforce_prefix=True):
    """
    :param protocols: the dictionary of protocols
    :param pfns: The PFNs helping match old to new
    :param read_write: Specify the set of domains to use

    :return: The dictionary of rewritten protocols
    """
#    import pdb; pdb.set_trace()
    new_protocols = []
    scheme, hostname, port, prefix = None, None, None, None  # Supress warnings
    if enforce_prefix:
        regexes = [r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+):(?P<port>\d+)/(?P<prefix>.*)',
                   r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+)/(?P<prefix>.*)',
                   r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+):(?P<port>\d+)(?P<wsp>\/.*\=)(?P<prefix>.*)',
                   r'(?P<scheme>\w+)://(?P<host>[a-zA-Z0-9\-\.]+)(?P<wsp>\/.*\=)(?P<prefix>.*)',
 ]
#{'type': 1, 'regexp': re.compile(r'(\w+)://([a-zA-Z0-9\-\.]+):(\d+)(\/.*\=)(.*)')},

        int_root = pfns['cms:/store/test/rucio/int/']
        for regex in regexes:
            try:
                prefix_regex = re.compile(regex)
                prefix_match = prefix_regex.match(int_root)
                print('Found a match for %s ' % regex)
                scheme = prefix_match.group('scheme')
                hostname = prefix_match.group('host')
                try:
                    port = prefix_match.group('port')
                except IndexError:
                    port = DEFAULT_PORTS[scheme.lower()]
                prefix = '/' + prefix_match.group('prefix')
                break
            except AttributeError:
                continue

    if read_write:
        new_domains = {'lan': {'delete': None, 'read': None, 'write': None},
                       'wan': {'delete': 1, 'read': 1, 'third_party_copy': 1, 'write': 1}}
    else:
        new_domains = {'lan': {'delete': None, 'read': None, 'write': None},
                       'wan': {'delete': None, 'read': 1, 'third_party_copy': 1, 'write': None}}

    for protocol in protocols:
        new_protocol = copy.deepcopy(protocol)
        new_protocol['domains'] = copy.deepcopy(new_domains)
        if enforce_prefix and protocol['scheme'] == scheme:
            new_protocol['hostname'] = hostname
            new_protocol['port'] = port
            new_protocol['extended_attributes'] = None
            new_protocol['prefix'] = prefix
            # Need to handle SRM separately
            extended_attributes = protocol['extended_attributes'] or {}
            if 'web_service_path' in extended_attributes:
                web_path = extended_attributes['web_service_path']
                new_protocol['extended_attributes'] = {}
                new_protocol['extended_attributes']['web_service_path'] = web_path
                prefix = prefix.replace(web_path, '')
                new_protocol['prefix'] = prefix
        if protocol['scheme'] in ['gsiftp', 'srm']:
            new_protocols.append(new_protocol)

    return new_protocols


if __name__ == '__main__':
    rci = Client(rucio_host='http://cms-rucio-int.cern.ch', auth_host='https://cms-rucio-auth-int.cern.ch',
                 account='root')
    rcp = Client(rucio_host='http://cms-rucio.cern.ch', auth_host='https://cms-rucio-auth.cern.ch',
                 account='ewv')

    input_rses = set(RSES_TO_SET+RSES_ONLY_INPUT)
    write_rses = set(RSES_TO_SET+RSES_WITHOUT_INPUT)

    for rse_name in set(RSES_TO_SET+RSES_WITHOUT_INPUT+RSES_ONLY_INPUT):
        # Fetch the values needed from the production RSE
        pfns = rcp.lfns2pfns(rse_name, ['cms:/store/test/rucio/int/'], operation='read')
        rse = rcp.get_rse(rse_name)
        rse_attributes = rcp.list_rse_attributes(rse=rse_name)

        # Setup the integration RSE
        if rse_name in write_rses:
            int_rse = rse_name 
            settings = INT_SETTINGS
            int_attributes = copy.deepcopy(rse_attributes)
            del int_attributes[rse_name]
            int_attributes.update(INT_ATTRIBUTES)
            create_rse(client=rci, rse_name=int_rse, settings=settings)
            set_rse_attributes(client=rci, rse_name=int_rse, attributes=int_attributes)
            rse_protocols = rse['protocols']
            #import pdb; pdb.set_trace()
            int_protocols = rewrite_protocols(rse_protocols, pfns, read_write=True)
            overwrite_protocols(client=rci, rse_name=int_rse, new_protocols=int_protocols)
        if rse_name in input_rses:
            input_rse = rse_name + '_Input'
            settings = INPUT_SETTINGS
            input_attributes = copy.deepcopy(rse_attributes)
            del input_attributes[rse_name]
            input_attributes.update(INPUT_ATTRIBUTES)
            create_rse(client=rci, rse_name=input_rse, settings=settings)
            set_rse_attributes(client=rci, rse_name=input_rse, attributes=input_attributes)
            rse_protocols = rse['protocols']
            input_protocols = rewrite_protocols(rse_protocols, pfns, read_write=False, enforce_prefix=False)
            overwrite_protocols(client=rci, rse_name=input_rse, new_protocols=input_protocols)
