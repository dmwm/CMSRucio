#! /usr/bin/env python

from statsd import StatsClient

statsd = StatsClient('statsd-exporter-rucio-statsd-exporter', 8125)

count = 0
with open('count.txt', 'r') as oracle_output:
    for line in oracle_output:
        if line.strip():
            count = int(line)

statsd.gauge('rucio.db.connections', count)

