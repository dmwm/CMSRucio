#!/bin/env python3

import base64
from collections import ChainMap, defaultdict
import json
import os
from pathlib import Path

import gitlab
from rucio.client import Client
from rucio.common.exception import Duplicate

IGNORED_RSE_INFO = ['id', 'protocols']

CMS_TYPES = ['test', 'temp']

# from rucio.core.rse
MUTABLE_RSE_PROPERTIES = {
    'name',
    'availability_read',
    'availability_write',
    'availability_delete',
    'latitude',
    'longitude',
    'time_zone',
    'rse_type',
    'volatile',
    'deterministic',
    'region_code',
    'country_name',
    'city',
    'staging_area',
    'qos_class',
    'continent',
    'availability'
}


try:
    client = Client()
except:
    pass


def get_info_from_rucio(rse: str) -> dict:
    '''
    Gets RSE Settings and Attributes

    Similar output to `rucio rse show`

    returns a dict of settings and attributes
    '''
    rse_info = client.get_rse(rse=rse)
    attributes = client.list_rse_attributes(rse=rse)

    # Settings
    settings = {key: rse_info[key] for i, key in enumerate(sorted(rse_info)) if key in MUTABLE_RSE_PROPERTIES}

    return dict(ChainMap(settings, attributes))


def commit_to_gitlab(site: str, data: dict, dry_run: bool = True) -> bool:
    private_token = os.environ['GITLAB_TOKEN']
    gl = gitlab.Gitlab('https://gitlab.cern.ch', private_token=private_token)
    group = gl.groups.get('siteconf')
    projects = group.projects.list(all=True, search=site)
    if len(projects) > 1:
        print(f'multiple projects found for site: {site}')
        return False
    full_project = gl.projects.get(projects[0].id)
    if not dry_run:
        try:
            commit = full_project.commits.create(data)
            return True
        except Exception as e:
            print(e)
            return False
    else:
        print(f"DRY RUN: {site}, {data}")
        return False


def commit_to_personal_gitlab(site: str, data: dict, dry_run: bool = True) -> bool:
    private_token = os.environ['GITLAB_TOKEN']
    gl = gitlab.Gitlab('https://gitlab.cern.ch', private_token=private_token)
    projects = gl.projects.list(owned=True, search=site)
    if len(projects) > 1:
        print(f'multiple projects found for site: {site}')
        return False
    full_project = gl.projects.get(projects[0].id)
    if not dry_run:
        try:
            commit = full_project.commits.create(data)
            return True
        except Exception as e:
            print(e)
            return False
    else:
        print(f"DRY RUN: {site}, {data}")
        return False


def extract_siteconf_project_name(rse: str) -> str:
    to_remove = ['_Temp', '_Test', '_Tape', '_Disk']
    for s in to_remove:
        rse = rse.removesuffix(s)
    return rse


if __name__ == '__main__':
    rses_by_site = defaultdict(list)
    rses = client.list_rses('update_from_json=True')
    for rse in rses:
        name = rse['rse']
        rse_info = get_info_from_rucio(name)
        file_dest = Path('rse_configs', f'{name}.json')

        with open(file_dest, 'w') as f:
            json.dump(rse_info, f, indent=4)
        
        project_name = extract_siteconf_project_name(rse['rse'])
        rses_by_site[project_name].append(rse['rse'])

    sites_to_commit = []
    sites_to_ignore = ['T2_US_Nebraska']

    for site, rses in rses_by_site.items():
        if site in sites_to_commit:
            print(rses)
            a = {
                'branch': 'master',
                'commit_message': 'upload initial rse config',
                'actions': []
            }
            for rse in rses:
                file_src = Path('rse_configs', f'{rse}.json')
                action = {
                    'action': 'create',
                    'file_path': f'rucio/{rse}.json',
                    'content': open(file_src, 'r').read()
                }
                a['actions'].append(action)

            #commit = commit_to_gitlab(site, a, dry_run=False)
            commit = commit_to_personal_gitlab(site, a, dry_run=False)
        
