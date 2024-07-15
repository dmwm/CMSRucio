#!/usr/bin/env python

from rucio.client.client import Client


def get_rse_usage(client, rse_expression):
    rse_usage = {}
    for rse_item in client.list_rses(rse_expression):
        rse = rse_item["rse"]
        rse_usage[rse] = {}
        for usage in client.get_rse_usage(rse):
            rse_usage[rse][usage['source']] = {
                'used': usage['used'],
                'free': usage['free'],
                'total': usage['total']
            }
        rse_usage[rse]['MinFreeSpace'] = client.get_rse_limits(rse)['MinFreeSpace']
    return rse_usage


def get_freespace(rse, source):
    dynamic = source['expired']['used'] - source['obsolete']['used']
    occupancy = source['rucio']['used']
    target_occupancy = source['static']['total'] - source['MinFreeSpace']
    freespace_bytes = (target_occupancy - occupancy) + dynamic
    freespace_tb = round(freespace_bytes / 1e12)

    if freespace_tb < 0:
        print(f"Warning {rse} Free Space < 0")
        return 0
    else:
        return freespace_tb

client = Client()
rse_expression = "(rse_type=DISK)&(cms_type=real)&(tier>0)&(tier<3)"
rse_usage = get_rse_usage(client, rse_expression)

for rse, source in rse_usage.items():
    freespace = get_freespace(rse, source)

    try:
        client.add_rse_attribute(rse, 'freespace', freespace)
        print(f"{rse} Freespace : {freespace} TB")
    except Exception as e:
        print(e)
