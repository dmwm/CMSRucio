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
            if args.wan_all:
                if domains[u'wan'] == DOMAIN_ALL:
                    continue
                changed.append(protocol)
                domains[u'wan'] = DOMAIN_ALL
            elif args.wan_read:
                if domains[u'wan'] == DOMAIN_READ:
                    continue
                changed.append(protocol)
                domains[u'wan'] = DOMAIN_READ
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
    parser.add_argument("--wan_all", action="store_true", help="Enable all WAN capabilities (read, write, tpc, delete)")
    parser.add_argument("--wan_read", action="store_true", help="Enable read-only WAN capabilities (read, tpc)")
    modify_protocol(parser.parse_args())
