#! /usr/bin/env python3

import io
import json
import os
import requests
import time
from rucio.client import Client

DEFAULT_FREE_THRESHOLD = 2 * 1e15
DEFAULT_REL_FREE_PCT_THRESHOLD = 2

rclient = Client()


def fetch_rse_occupancy_metrics(rse):
    rse_usage = list(rclient.get_rse_usage(rse))

    required_fields = {"static", "rucio", "expired"}
    usage_metrics = {}

    for source in rse_usage:
        # Assuming source and used keys exist
        usage_metrics[source["source"]] = source["used"]

    if not required_fields.issubset(usage_metrics.keys()):
        raise Exception(f"Following RSE usage metric(s) are missing: {str(required_fields - usage_metrics.keys())}")

    static, rucio, expired, unavailable = usage_metrics["static"], usage_metrics["rucio"], usage_metrics["expired"], usage_metrics["unavailable"]
    return static, rucio, expired, unavailable

def get_thresholds():
    try:
        free_threshold = int(rclient.get_config(section="rses", option="free_threshold"))
    except Exception as e:
        free_threshold = DEFAULT_FREE_THRESHOLD
    
    try:
        rel_free_pct_threshold = int(rclient.get_config(section="rses", option="rel_free_pct_threshold"))
    except Exception as e:
        rel_free_pct_threshold = DEFAULT_REL_FREE_PCT_THRESHOLD
    return free_threshold, rel_free_pct_threshold


def should_enable_availability_on_occupancy(rse):
    """
    Calculates free space and relative free space in the given RSE 
    Returns False if free space AND relative free space is below the corresponding thresholds.
    """
    try:
        static, rucio, expired, unavailable = fetch_rse_occupancy_metrics(rse)
    except Exception as e:
        print(f"Occupancy check will skip {rse}: {str(e)}")
        return True
    free = static - rucio
    rel_free_pct = (free / static) * 100
    free_threshold, rel_free_pct_threshold = get_thresholds()
    return not (free < free_threshold and rel_free_pct < rel_free_pct_threshold)


def main():

    QUERY_HEADER = '{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_cmssst_*"]}'
    DRY_RUN = True

    with open('availability_lucene.json', 'r') as lucene_json:
        lucene = json.load(lucene_json)

    lucene["query"]["bool"]["filter"]["range"]["metadata.timestamp"]["gte"] = int(time.time() - 3 * 24 * 60 * 60)
    lucene["query"]["bool"]["filter"]["range"]["metadata.timestamp"]["lt"] = int(time.time() + 900)

    query = io.StringIO(QUERY_HEADER + '\n' + json.dumps(lucene) + '\n')

    headers = {'Authorization': 'Bearer %s' % os.environ['MONIT_TOKEN'],
            'Content-Type': 'application/json'}

    r = requests.post('https://monit-grafana.cern.ch/api/datasources/proxy/9475/_msearch', data=query, headers=headers)

    j = json.loads(r.text)

    sites = [record['_source']['data'] for record in j['responses'][0]['hits']['hits']]

    # Map storing the decisions for read/write/delete based on SSB
    available_map = {}
    skip_rses = [rse['rse'] for rse in rclient.list_rses(rse_expression='skip_site_availability_update=True')]
    all_rses = [rse['rse'] for rse in rclient.list_rses()]

    # Records are sorted most recent to least recent. Pick up the value for the most recent for a site
    for site in sites:
        site_name = site['name']

        # Get the disk rse for the site
        # Are there sites that don't have a disk rse but only a tape rse?
        site_rses = []
        if site_name in all_rses:
            rse_disk = site_name
            site_rses.append(rse_disk)
        elif site_name + '_Disk' in all_rses:
            rse_disk = site_name + '_Disk'
            site_rses.append(rse_disk)

        # Get the Tape rse for the site - if any
        if site_name + '_Tape' in all_rses:
            rse_tape = site_name + '_Tape'
            site_rses.append(rse_tape)

        for rse in site_rses:
            if rse in skip_rses:
                continue
            if rse.startswith('T3_'):
                continue  # Until we get good metrics for Tier3s
            if rse in available_map:
                continue
            rucio_status = site.get('rucio_status', None)
            if not rucio_status or rucio_status not in ['dependable', 'enabled']:
                available_map[rse] = {
                    "availability_read": False,
                    "availability_write": False,
                    "availability_delete": False
                }
            else:
                available_map[rse] = {
                    "availability_read": True,
                    "availability_write": True,
                    "availability_delete": True
                }

            # If the site is enabled based on SSB, do an occupancy check
            if available_map[rse]:
                should_enable = should_enable_availability_on_occupancy(rse)
                if not should_enable:
                    print(f"The following site doesn't have enough free space. Availability write should be disabled: {rse}")
                    available_map[rse]["availability_write"] = False


    print("Following RSE are skipped due to skip_site_availability_update: %s" % skip_rses)

    for rse, decision in available_map.items():
        try:
            if not DRY_RUN:
                rclient.update_rse(rse, {"availability_write": decision["availability_write"], "availability_delete": decision["availability_delete"], "availability_read": decision["availability_read"]})
                print('Setting availability for %s to %s' % (rse, decision))
            else:
                print('DRY-RUN: Setting availability for %s to %s' % (rse, decision))
        except Exception as e:  # Should never be the case
            print('Failed to update RSE %s, Error %s' % (rse, e))



if __name__ == "__main__":
    main()