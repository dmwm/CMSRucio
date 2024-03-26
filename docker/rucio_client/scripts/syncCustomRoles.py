#! /usr/bin/env python3
import json
import logging
import os
import requests

from rucio.client import Client
from argparse import ArgumentParser

PROXY = os.getenv('X509_USER_PROXY')
logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def make_request(api_endpoint, cert=PROXY, verify=False):
    # Pods don't like the CRIC certificate
    result = requests.get(api_endpoint, cert=cert, verify=verify)
    return json.loads(result.text)

def add_site_capacity_manager_role(rucio_client, cric_endpoint, dry_run=False):
    """
    Add site capacity manager role to T2_CH_CERN and T0_CH_CERN_Disk RSEs
    This syncs the cms-tier0-ops group to the site-capacity-manager attribute 
    site-capacity-manager - The list of accounts that are allowed to manage the site capacity
        For all other sites this attribute is not set and the capacity is automatically synced from cmssst SiteCapacity page 
    """
    logger = logging.getLogger("add_site_capacity_manager_role")

    res = make_request(cric_endpoint)
    users = res["CMS_TIER_0_OPS"]["users"]

    accounts = set()
    for user in users:
        accounts.add(user["login"])

    t0_rse_attr = rucio_client.list_rse_attributes("T0_CH_CERN_Disk")
    current_t0_ch_cern_disk_capacity_managers = t0_rse_attr.get("site_capacity_manager", "")
    current_t0_ch_cern_disk_capacity_managers = set(current_t0_ch_cern_disk_capacity_managers.split(","))

    t2_rse_attr = rucio_client.list_rse_attributes("T2_CH_CERN")
    current_t2_ch_cern_capacity_managers = t2_rse_attr.get("site_capacity_manager", "")
    current_t2_ch_cern_capacity_managers = set(current_t2_ch_cern_capacity_managers.split(","))
    
    if current_t0_ch_cern_disk_capacity_managers == accounts and current_t2_ch_cern_capacity_managers == accounts:
        logger.info("No changes to site capacity manager accounts")
        return
    
    new_capacity_managers = ",".join(accounts)

    if current_t0_ch_cern_disk_capacity_managers != accounts:
        logger.info("Updating T0_CH_CERN_Disk site capacity manager accounts to %s", new_capacity_managers)
        if not dry_run:
            rucio_client.add_rse_attribute("T0_CH_CERN_Disk", "site_capacity_manager", new_capacity_managers)

    if current_t2_ch_cern_capacity_managers != accounts:
        logger.info("Updating T2_CH_CERN site capacity manager accounts to %s", new_capacity_managers)
        if not dry_run:
            rucio_client.add_rse_attribute("T2_CH_CERN", "site_capacity_manager", new_capacity_managers)



if __name__=="__main__":
    """
    Sync Custome Roles:
    Add site specific roles to RSE attributes from an external source such as CRIC
    Each role addition shall be contained in a separate function
    """
    #Get Dry Run Option
    parser = ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    args = parser.parse_args()
    dry_run = args.dry_run

    rucio_client = Client()
    add_site_capacity_manager_role(rucio_client, "https://cms-cric.cern.ch/api/accounts/group/query/?json&name=CMS_TIER_0_OPS", dry_run=dry_run)
