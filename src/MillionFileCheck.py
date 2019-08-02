#! /usr/bin/env python


"""
A bit of code to check on the progress of the million file test
"""

from __future__ import division, print_function

from rucio.client.client import Client

client = Client()
client.whoami()

rses = client.list_rses(rse_expression='cms_type=test')

total_bytes = 0
total_files = 0
total_bytes_possible = 0
total_files_possible = 0
total_blocks = 0

for obj in sorted(rses):
    rse = obj['rse']
    rse_bytes = 0
    rse_files = 0
    rse_bytes_possible = 0
    rse_files_possible = 0
    rse_blocks = 0

    for dsn in client.list_datasets_per_rse(rse):
        if 'NANOAODSIM' in dsn['name']:
            total_blocks += 1
            rse_blocks += 1
            try:
                total_bytes_possible += int(dsn['bytes'])
                rse_bytes_possible += int(dsn['bytes'])
                total_files_possible += int(dsn['length'])
                rse_files_possible += int(dsn['length'])
                total_bytes += int(dsn['available_bytes'])
                rse_bytes += int(dsn['available_bytes'])
                total_files += int(dsn['available_length'])
                rse_files += int(dsn['available_length'])
            except TypeError:
                pass
    if rse_files_possible and rse_bytes_possible:
        print('%25s - blocks: %7d,  files: %7d of %7d (%3d%%),  size: %7.1f of %7.1f TB' %
              (rse, rse_blocks, rse_files, rse_files_possible, (rse_files / rse_files_possible) * 100,
               rse_bytes / 1e12, rse_bytes_possible / 1e12))

print('---------------------')
print('%25s - blocks: %7d,  files: %7d of %7d (%3d%%),  size: %7.1f of %7.1f TB' %
      ('Totals', total_blocks, total_files, total_files_possible, (total_files / total_files_possible) * 100,
       total_bytes / 1e12, total_bytes_possible / 1e12))
