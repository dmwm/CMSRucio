#!/usr/bin/env python
import argparse

DOMAIN_ALL = {u'read': 1, u'write': 1, u'third_party_copy': 1, u'delete': 1}
DOMAIN_READ = {u'read': 1, u'write': 0, u'third_party_copy': 1, u'delete': 0}


def modify_protocol(args):
    from rucio.client import Client
    client = Client(account="transfer_ops")
    changed = []
    for protocol in client.get_protocols(args.rse[0]):
        if protocol[u'scheme'] in args.scheme:
            domains = protocol[u'domains']
            if args.wan_read is not None:
                changed.append(protocol)
                domains[u'wan'][u'read'] = args.wan_read
            if args.wan_write is not None:
                changed.append(protocol)
                domains[u'wan'][u'write'] = args.wan_write
            if args.wan_tpc is not None:
                changed.append(protocol)
                domains[u'wan'][u'third_party_copy'] = args.wan_tpc
            if args.wan_delete is not None:
                changed.append(protocol)
                domains[u'wan'][u'delete'] = args.wan_delete
            ok = client.update_protocols(
                args.rse[0],
                protocol[u'scheme'],
                {"domains": domains},
                hostname=protocol[u'hostname'],
                port=protocol[u'port'],
            )
            if not ok:
                raise RuntimeError("Failed to update protocol")
    if len(changed):
        print("Successfully changed protocols")
    else:
        print("No protocols were modified")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Modify the protocol domain attributes in-place for an RSE')
    parser.add_argument("rse", metavar="RSE", nargs=1, help="The RSE name")
    parser.add_argument("scheme", metavar="SCHEME", nargs="+", help="The protocol schemes to modify")
    parser.add_argument("--wan_read", type=int, help="WAN Read priority")
    parser.add_argument("--wan_write", type=int, help="WAN Write priority")
    parser.add_argument("--wan_tpc", type=int, help="WAN Third-party copy priority")
    parser.add_argument("--wan_delete", type=int, help="WAN delete priority")
    modify_protocol(parser.parse_args())
