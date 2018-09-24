"""
LFN-to-path algorithms for TFC
"""
import re

from rucio.rse.protocols.protocol import RSEDeterministicTranslation

def cmstfc(scope, name, rse, rse_attrs, proto_attrs):
    """
    Map lfn into pfn accoring to the declared tfc in the protocol.

    """

    # Prevents unused argument warnings in pylint
    del rse
    del rse_attrs
    del scope

    # Getting the TFC
    tfc = proto_attrs['extended_attributes']['tfc']
    tfc_proto = proto_attrs['extended_attributes']['tfc_proto']

    # matching the lfn into a pfn
    pfn = tfc_lfn2pfn(name, tfc, tfc_proto)

    # now we have to remove the protocol part of the pfn
    proto_pfn = proto_attrs['scheme'] + '://' + \
       proto_attrs['hostname'] + ':' + str(proto_attrs['port'])
    if 'extended_attributes' in proto_attrs and \
        'web_service_path' in proto_attrs['extended_attributes']:
        proto_pfn += proto_attrs['extended_attributes']['web_service_path']
    proto_pfn += proto_attrs['prefix']

    return pfn.replace(proto_pfn, "")

def tfc_lfn2pfn(lfn, tfc, proto):
    """
    Performs the actual tfc matching
    """

    for rule in tfc:
        if rule['proto'] == proto:
            regex = re.compile(rule['path'])
            if regex.match(lfn):
                if 'chain' in rule:
                    lfn = tfc_lfn2pfn(lfn, tfc, rule['chain'])
                return regex.sub(rule['out'].replace('$', '\\'), lfn)

    raise ValueError("lfn %s with proto %s cannot be matched by tfc %s" % (lfn, proto, tfc))

RSEDeterministicTranslation.register(cmstfc)


if __name__ == '__main__':

    PROTO_ATTRS = {
        'extended_attributes': {'tfc_proto': 'srmv2', 'web_service_path': '/srm/managerv2?SFN=',\
        'tfc': [
            {'destination-match': '.*', 'proto': 'direct',
             'out': '/dpm/in2p3.fr/home/cms/trivcat/$1',
             'path': u'/+(.*)'},
            {'proto': 'srmv2', 'chain': 'direct', 'destination-match': '.*',
             'out': 'srm://polgrid4.in2p3.fr:8446/srm/managerv2?SFN=/$1',
             'path': '/+(.*)'}
        ]},
        'hostname': 'polgrid4.in2p3.fr', 'prefix': '/dpm', 'scheme': 'srm', 'port': 8446
    }


    def test_tfc_mapping(name, pfn, scope="cms"):
        """
        Unit test for lfn to pfn mapping
        """

        mapped_pfn = cmstfc(scope, name, None, None, PROTO_ATTRS)
        if mapped_pfn == pfn:
            print "%s:%s -> %s" % (scope, name, pfn)
        else:
            print "FAILURE: %s:%s -> %s (expected %s)" % (scope, name, mapped_pfn, pfn)

    test_tfc_mapping("/store/some/path/file.root",
                     "/in2p3.fr/home/cms/trivcat/store/some/path/file.root")
