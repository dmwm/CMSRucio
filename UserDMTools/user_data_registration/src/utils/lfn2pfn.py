import re
from argparse import ArgumentParser


def get_pfns_temp(temp_rse, lfns, account_type="user"):
    """
    get pfns from RSE name and list of lfns for temporary RSES
    :param temp_rse: TempRSE name
    :type temp_rse: str
    :param lfns: list of lnfs
    :type lfns: list[str]
    :return: dictionary {lfn1: pfn1, lfn2: pfn2, ...}
    :rtype: dict
    """
    temp_lfn_to_lfn = {}
    pfn_map = {}
    pattern = f"/{account_type}"
    for lfn in lfns:
        index = lfn.find(pattern)
        temp_lfn = lfn[:index] + '/temp' + lfn[index:]
        temp_lfn_to_lfn[temp_lfn] = lfn

    pfn_map_temp = get_pfns(temp_rse, list(temp_lfn_to_lfn.keys()))

    for temp_lfn in pfn_map_temp:
        pfn_map[temp_lfn_to_lfn[temp_lfn]] = pfn_map_temp[temp_lfn]

    return pfn_map


def get_pfns(rse: str, lfns: list):
    from rucio.client import Client
    pfns = []
    pfn_map = {}
    rucio_client = Client()
    rucio_scope = f'user.{rucio_client.account}'
    # TODO do we need a check for this?

    try:
        rgx = rucio_client.get_protocols(
            rse.split("_Temp")[0], protocol_domain='ALL', operation="write")[0]

        if not rgx['extended_attributes'] or 'tfc' not in rgx['extended_attributes']:
            pfn_0 = rucio_client.lfns2pfns(
                rse=rse.split("_Temp")[0],
                lfns=[rucio_scope + ":" + lfns[0]],
                operation="write"
            )

            pfns.append(pfn_0[rucio_scope + ":" + lfns[0]])
            prefix = pfn_0[rucio_scope + ":" + lfns[0]].split(lfns[0])[0]

            for lfn in lfns:
                pfn_map.update({lfn: prefix+lfn})
        else:
            for lfn in lfns:
                if 'tfc' in rgx['extended_attributes']:
                    tfc = rgx['extended_attributes']['tfc']
                    tfc_proto = rgx['extended_attributes']['tfc_proto']

                    pfn_map.update({lfn: tfc_lfn2pfn(lfn, tfc, tfc_proto)})

    except TypeError:
        raise TypeError('Cannot determine PFN for LFN %s:%s at %s with proto %s'
                        % rucio_scope, lfn, rse, rgx)
    return pfn_map


def tfc_lfn2pfn(lfn, tfc, proto, depth=0):
    """
    Performs the actual tfc lfn2pfn matching
    """
    MAX_CHAIN_DEPTH = 5

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

    raise ValueError(
        "lfn %s with proto %s cannot be matched by tfc %s" % (lfn, proto, tfc))


if __name__ == "__main__":
    parser = ArgumentParser(
        prog='LFN2PFN',
        description='Converts lfn to pfn',
        epilog='Help Text')

    parser.add_argument("-r", "--rse")
    parser.add_argument("-l", "--lfn")

    args = parser.parse_args()

    if "_Temp" in args.rse:
        print(get_pfns_temp(args.rse, [args.lfn]))
    else:
        print(get_pfns(args.rse, [args.lfn]))
