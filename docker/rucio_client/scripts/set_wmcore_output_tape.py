#! /usr/bin/env python3

"""
DM whitelists tape sites via wmcore_output_tape RSE attribute for Production Output. 
For a given production output dataset, WM(MSOutput) chooses the tape destination amongst the 
whitelisted tape RSEs. To be specific, MSOutput makes a weighted random selection based on the dm_weight
attribute amongst the whitelisted tape RSEs
"""

from rucio.client import Client
from setSiteAvailability import should_enable_availability_on_occupancy

DRY_RUN = False

def main():

    rclient = Client()
    tape_rses = [rse['rse'] for rse in rclient.list_rses(rse_expression='rse_type=TAPE&cms_type=real')]
    try:
        skip_rses = [rse['rse'] for rse in rclient.list_rses(rse_expression='skip_wmcore_output_tape_update=True')]
    except:
        skip_rses = []
    
    print(f"Following RSEs will be skipped in this automatic update {skip_rses}")

    for rse in tape_rses:
        if rse in skip_rses:
            continue
        
        wmcore_output_tape = should_enable_availability_on_occupancy(rse)

        if DRY_RUN:
            print(f"DRY-RUN: Setting wmcore_output_tape at {rse} to {wmcore_output_tape}")
        else:
            print(f"Setting wmcore_output_tape at {rse} to {wmcore_output_tape}")
            rclient.add_rse_attribute(rse, "wmcore__output_tape", wmcore_output_tape)

if __name__ == "__main__":
    main()

