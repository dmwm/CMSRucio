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