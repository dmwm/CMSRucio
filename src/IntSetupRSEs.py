#! /usr/bin/env python3

import copy
import logging
import pdb
import re
import argparse

from rucio.client import Client
from rucio.common.exception import RSEProtocolNotSupported, RSENotFound, Duplicate


INT_SETTINGS = {'availability_read': True, 'availability_write': True, 'availability_delete': True}
INT_ATTRIBUTES = {'dm_weight': 1, 'cms_type': 'int', 'reaper': True}
INPUT_SETTINGS = {'availability_read': True, 'availability_write': False, 'availability_delete': False}
INPUT_ATTRIBUTES = {'dm_weight': 1,'cms_type': 'input', 'reaper': False}

DEFAULT_PORTS = {'root': 1094, 'davs': 2880}


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
        if new_proto['scheme'] in ['root', 'davs']:
            try: 
                print('Adding %s to %s' % (new_proto['scheme'], rse_name))
                client.add_protocol(rse=rse_name, params=new_proto)
            except Duplicate:
                print('Could not add additional %s to %s' % (new_proto['scheme'], rse_name))


def set_rse_attributes(client, rse_name, attributes):
    attributes = attributes or []

    for (key, value) in attributes.items():
        print('Setting %s=%s for %s' % (key, value, rse_name))
        client.add_rse_attribute(rse=rse_name, key=key, value=value)
        changed = True

    return changed


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
        new_domains = {'lan': {'delete': 0, 'read': 0, 'write': 0},
                       'wan': {'delete': 1, 'read': 1, 'third_party_copy_read': 1,'third_party_copy_write': 1, 'write': 1}}
    else:
        new_domains = {'lan': {'delete': 0, 'read': 0, 'write': 0},
                       'wan': {'delete': 0, 'read': 1, 'third_party_copy_read': 1,'third_party_copy_write': 1, 'write': 0}}

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
            if 'tfc' in extended_attributes:
                tfc = extended_attributes['tfc']
                tfc_proto = extended_attributes['tfc_proto']
                new_protocol['extended_attributes'] = {}
                new_protocol['extended_attributes']['tfc'] = tfc
                new_protocol['extended_attributes']['tfc_proto'] = tfc_proto
        if protocol['scheme'] in ['davs', 'root']:
            if protocol['scheme']=='root':
                new_protocol['domains']['wan'] = {'delete': 0, 'read': 3, 'third_party_copy_read': 3, 'third_party_copy_write': 0, 'write': 0}
            new_protocols.append(new_protocol)

    return new_protocols

def main():
    parser = argparse.ArgumentParser(description="A script to set up RSEs in the rucio integration cluster.")
    parser.add_argument("rses_to_set",help="Comma-separated list of RSEs that will be configured in the integration cluster.")
    parser.add_argument("rses_input",help="Comma-separated list of production RSEs from which the data will be transferred from.")

    args = parser.parse_args()

    rci = Client(rucio_host='http://cms-rucio-int.cern.ch', auth_host='https://cms-rucio-auth-int.cern.ch',
                 account='transfer_ops')
    rcp = Client(rucio_host='http://cms-rucio.cern.ch', auth_host='https://cms-rucio-auth.cern.ch',
                 account='transfer_ops')
    
    RSES_TO_SET = args.rses_to_set.replace(' ','').split(',')#['T2_CH_CERN']
    RSES_INPUT = args.rses_input.replace(' ','').split(',')#['T2_DE_DESY']



    input_rses = set(RSES_INPUT)
    write_rses = set(RSES_TO_SET)

    for rse_name in set(RSES_TO_SET+RSES_INPUT):
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


if __name__ == '__main__':
    main()