#!/bin/env python3

import json

from rucio.client import Client

IGNORED_RSEINFO = ['id', 'protocols']


client = Client()


def get_rse_info(rse: str):
    '''
    Gets RSE Settings and Attributes

    Similar output to `rucio rse show`
    '''
    rseinfo = client.get_rse(rse=rse)
    attributes = client.list_rse_attributes(rse=rse)
    usage = client.get_rse_usage(rse=rse)
    rse_limits = client.get_rse_limits(rse)

    # Settings
    settings = {key: rseinfo[key] for i, key in enumerate(sorted(rseinfo)) if key not in IGNORED_RSEINFO}

    rse_config = {
        "settings": settings,
        "attributes": attributes
    }

    with open(f'rse_configs/{rse}.json', 'w') as f:
        json.dump(rse_config, f, indent=4)


def set_rse_settings(rse: str, config: dict, dry_run: bool = True):
    '''
    Setts RSE settings and attributes from a config dict
    '''
    settings = config['settings']
    attributes = config['attributes']

    # update settings
    settings_to_update = []
    for key, value in settings.items():
        current = client.get_rse(rse=rse)
        if current[key] != value:
            settings_to_update.append(key)

    # update attributes
    attr_to_update = []
    for key, value in attributes.items():
        current = client.list_rse_attributes(rse=rse)
        if current[key] != value:
            attr_to_update.append(key)

    print(f'Settings to update: {settings_to_update}')
    print(f'Attributes to update: {attr_to_update}')

    if not dry_run:
        for setting in settings_to_update:
            value = settings[setting]
        # client.add_rse_attribute(rse, )
        pass


if __name__ == '__main__':
    # rses = client.list_rses('update_from_json=True')
    # for rse in rses:
    #     name = rse['rse']
    #     get_rse_info(name)

    rse = 'T0_CH_CERN_Disk'
    with open(f'rse_configs/{rse}.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
        set_rse_settings(rse, config)