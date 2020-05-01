#!/usr/bin/env python
from __future__ import print_function
from rucio.common.exception import RucioException
from rucio.client.client import Client

client = Client(account='root')

target_occupancy = 0.9
quotas = {
    'T1_US_FNAL_Mock': 30e9,
    'T2_CH_CERN_Mock': 40e9,
    'T2_US_Florida_Mock': 50e9,
    'T2_US_Wisconsin_Mock': 60e9,
}

for rse, quota in quotas.items():
    rusage = [
        usage
        for usage in client.get_rse_usage(rse)
        if usage['source'] == 'rucio'
    ].pop()
    print("RSE %s used %.1f of %.1f GB" % (rse, rusage['used'] * 1e-9, quota * 1e-9))
    client.set_rse_usage(rse, source='storage', used=rusage['used'], free=quota - rusage['used'])
    client.set_rse_limits(rse, 'MinFreeSpace', int(quota * (1 - target_occupancy)))
