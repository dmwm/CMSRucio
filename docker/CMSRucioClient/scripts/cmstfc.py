#! /usr/bin/env python

"""
LFN-to-path algorithms for TFC
"""
import re

REGISTER = True

try:
    from rucio.rse.protocols.protocol import RSEDeterministicTranslation
except ImportError:
    REGISTER = False

MAX_CHAIN_DEPTH = 5


def cmstfc(scope, name, rse, rse_attrs, proto_attrs):
    """
    Map lfn into pfn according to the declared tfc in the protocol.
    """

    # Prevents unused argument warnings in pylint
    del rse_attrs

    # Getting the TFC
    try:
        if proto_attrs.get('extended_attributes', None):
            tfc = proto_attrs['extended_attributes']['tfc']
            tfc_proto = proto_attrs['extended_attributes']['tfc_proto']

            # matching the lfn into a pfn
            pfn = tfc_lfn2pfn(name, tfc, tfc_proto)

            # now we have to remove the protocol part of the pfn
            proto_pfn = proto_attrs['scheme'] + '://' + proto_attrs['hostname'] + ':' + str(proto_attrs['port'])
            if 'extended_attributes' in proto_attrs and \
                    'web_service_path' in proto_attrs['extended_attributes']:
                proto_pfn += proto_attrs['extended_attributes']['web_service_path']
            proto_pfn += proto_attrs['prefix']

            proto_less = pfn.replace(proto_pfn, "")
            return re.sub('/+', '/', proto_less)  # Remove unnecessary double slashes
        else:
            path = '/' + name
            path = re.sub('/+', '/', path)
            return path
    except TypeError:
        raise TypeError('Cannot determine PFN for LFN %s:%s at %s with proto %s' %
                        scope, name, rse, proto_attrs)

def tfc_lfn2pfn(lfn, tfc, proto, depth=0):
    """
    Performs the actual tfc matching
    """

    if depth > MAX_CHAIN_DEPTH:
        raise Exception("Max depth reached matching lfn %s and protocol %s with tfc %s" %
                        lfn, proto, tfc)

    for rule in tfc:
        if rule['proto'] == proto:
            if 'chain' in rule:
                lfn = tfc_lfn2pfn(lfn, tfc, rule['chain'], depth + 1)

            regex = re.compile(rule['path'])
            if regex.match(lfn):
                return regex.sub(rule['out'].replace('$', '\\'), lfn)

    if depth > 0:
        return lfn

    raise ValueError("lfn %s with proto %s cannot be matched by tfc %s" % (lfn, proto, tfc))


if REGISTER:
    RSEDeterministicTranslation.register(cmstfc)

if __name__ == '__main__':

    # Test 1: simple srm endpoint (T2_FR_GRIF_LLR)
    PROTO_ATTRS = {
        'extended_attributes': {'tfc_proto': 'srmv2', 'web_service_path': '/srm/managerv2?SFN=',
                                'tfc': [
                                    {'destination-match': '.*', 'proto': 'direct',
                                     'out': '/dpm/in2p3.fr/home/cms/trivcat/$1',
                                     'path': u'/+(.*)'},
                                    {'proto': 'srmv2', 'chain': 'direct', 'destination-match': '.*',
                                     'out': 'srm://polgrid4.in2p3.fr:8446/srm/managerv2?SFN=/$1',
                                     'path': '/+(.*)'}
                                ]},
        'hostname': 'polgrid4.in2p3.fr', 'prefix': '/', 'scheme': 'srm', 'port': 8446
    }

    # Test 2 RAL T1 ECHO endpoint
    PROTO_ATTRS2 = {
        'extended_attributes': {'tfc_proto': 'srmv2',
                                'tfc': [
                                    {u'path': u'/+store/(.*)', u'out': u'/store/$1', u'proto': u'direct'},
                                    {u'path': u'(.*)', u'out': u'gsiftp://gridftp.echo.stfc.ac.uk:2811/cms:$1',
                                     u'chain': u'direct', u'proto': u'srmv2'}
                                ]},
        'hostname': 'gridftp.echo.stfc.ac.uk', 'prefix': '/', 'scheme': 'gsiftp', 'port': 2811
    }

    # Test 3 MIT, full use of chain rules
    PROTO_ATTRS3 = {
        'extended_attributes': {'tfc_proto': 'srmv2',
                                'tfc': [
                                    {u'path': u'/+(.*)', u'out': u'/mnt/hadoop/cms/$1', u'proto': u'direct'},
                                    {u'path': u'/+store/(.*)', u'out': u'/mnt/hadoop/cms/store/$1',
                                     u'proto': u'direct'},
                                    {u'path': u'/mnt/hadoop/(.*)', u'out': u'gsiftp://se01.cmsaf.mit.edu:2811/$1',
                                     u'chain': u'direct', u'proto': u'srmv2'}
                                ]},
        'hostname': 'se01.cmsaf.mit.edu',
        'prefix': '/',
        'scheme': 'gsiftp',
        'port': '2811'
    }
    PROTO_ATTRS4 = {
        'extended_attributes': None,
        'hostname': 'se01.cmsaf.mit.edu',
        'prefix': '/mnt/hadoop/cms/',
        'scheme': 'gsiftp',
        'port': '2811'
    }


    def test_tfc_mapping(name, proto_attrs, pfn, scope="cms"):
        """
        Unit test for lfn to pfn mapping
        """

        mapped_pfn = cmstfc(scope, name, None, None, proto_attrs)
        if mapped_pfn == pfn:
            print "%s:%s -> %s" % (scope, name, pfn)
        else:
            print "FAILURE: %s:%s -> %s (expected %s)" % (scope, name, mapped_pfn, pfn)


    test_tfc_mapping(
        "/store/some//path//file.root",
        PROTO_ATTRS,
        "dpm/in2p3.fr/home/cms/trivcat/store/some/path/file.root"
    )

    test_tfc_mapping(
        "/store/data/some//path/file.root",
        PROTO_ATTRS2,
        "cms:/store/data/some/path/file.root"
    )

    test_tfc_mapping(
        "/store/some/path/file.root",
        PROTO_ATTRS,
        "dpm/in2p3.fr/home/cms/trivcat/store/some/path/file.root"
    )

    test_tfc_mapping(
        "/store/data/some/path/file.root",
        PROTO_ATTRS2,
        "cms:/store/data/some/path/file.root"
    )

    test_tfc_mapping(
        "/store/data/some/path/file.root",
        PROTO_ATTRS3,
        "cms/store/data/some/path/file.root"
    )
    test_tfc_mapping(
        "//store/data/some/path/file.root",
        PROTO_ATTRS4,
        "/store/data/some/path/file.root"
    )
    test_tfc_mapping(
        "//store/user/rucio//ewv/some/path/file.root",
        PROTO_ATTRS4,
        "/store/user/rucio/ewv/some/path/file.root"
    )
