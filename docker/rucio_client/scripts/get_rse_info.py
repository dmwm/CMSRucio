#!/bin/env python3

import json
import os
from pathlib import Path

import gitlab
from rucio.client import Client
from rucio.common.exception import Duplicate

IGNORED_RSEINFO = ['id', 'protocols']

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


client = Client()


def get_info_from_rucio(rse: str) -> dict:
    '''
    Gets RSE Settings and Attributes

    Similar output to `rucio rse show`

    returns a dict of settings and attributes
    '''
    rseinfo = client.get_rse(rse=rse)
    attributes = client.list_rse_attributes(rse=rse)
    usage = client.get_rse_usage(rse=rse)
    rse_limits = client.get_rse_limits(rse)

    # Settings
    settings = {key: rseinfo[key] for i, key in enumerate(sorted(rseinfo)) if key in MUTABLE_RSE_PROPERTIES}

    return settings, attributes


def get_info_from_gitlab(rse: str) -> dict:
    private_token = os.environ['GITLAB_TOKEN']
    gl = gitlab.Gitlab('https://githlab.cern.ch', private_token=private_token)
    group = gl.groups.get('siteconf')
    projects = group.projects.list(all=True)


def set_rse_settings(rse: str, config: dict, dry_run: bool = True):
    '''
    Setts RSE settings and attributes from a config dict
    '''
    settings = config['settings']
    attributes = config['attributes']

    current_settings, current_attrs = get_info_from_rucio(rse)

    # update settings
    # TODO: Handle KeyError
    settings_to_update = []
    for key, value in settings.items():
        if current_settings.get(key) != value:
            settings_to_update.append(key)

    # update attributes
    # TODO: Handle KeyError
    attr_to_update = []
    for key, value in attributes.items():
        if current_attrs.get(key) != value:
            attr_to_update.append(key)
        else:
            print(f'attribute {key} is not added to RSE')

    print(f'Settings to update: {settings_to_update}')
    print(f'Attributes to update: {attr_to_update}')

    if not dry_run:
        # update settings
        to_update = {k: settings[k] for k in settings_to_update}
        # client.update_rse(rse, to_update)

        for attribute in attr_to_update:
            value = attributes[attribute]
            if attribute in current_attrs.keys():
                print(f'Attribute exists, Deleting {attribute} from {rse}')
                # client.delete_rse_attribute(rse, attribute)
            try:
                print(f'Adding {attribute}: {value} to {rse}')
                # client.add_rse_attribute(rse, attribute, value)
            except Duplicate:
                print(f'{attribute} already exists')



if __name__ == '__main__':
    rses = client.list_rses('update_from_json=True')
    for rse in rses:
        name = rse['rse']
        settings, attributes = get_info_from_rucio(name)
        file_dest = Path('rse_configs', f'{name}.json')

        with open(file_dest, 'w') as f:
            json.dump({"settings": settings,
                       "attributes": attributes},
                       f,
                       indent=4)

    # rse = 'T0_CH_CERN_Disk'
    # with open(f'rse_configs/{rse}.json', 'r', encoding='utf-8') as f:
    #     config = json.load(f)
    #     set_rse_settings(rse, config)