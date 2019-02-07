#! /usr/bin/env python

from __future__ import absolute_import, division, print_function

import json
import urllib2

AGIS_URL = 'http://atlas-agis-api.cern.ch/request/site/query/list/?json&'
CRIC_URL = 'https://cms-cric.cern.ch/api/cms/site/query/?json&rcsite_state=ANY'

agis_data = json.load(urllib2.urlopen(AGIS_URL))
cric_data = json.load(urllib2.urlopen(CRIC_URL))

# Get Site, experiment site, cloud, country, Tier


print('ATLAS sites')
for entry in agis_data:  # ATLAS URL returns a list of all sites
    site = entry['name']
    exp_site = entry['rc_site']
    cloud = entry['cloud']
    country = entry['country']
    tier = entry['tier_level']

    print('Site: %27s Exp Site: %44s Tier: %1s Country: %18s Cloud: %10s' %
          (site, exp_site, tier, country, cloud))
print()

print('CMS Sites')
for name, entry in cric_data.items():  # CRIC URL returns a dictionary where
    site = name
    if name != entry['name']:
        print('Site names do no match')
    exp_site = entry['facility']
    cloud = 'N/A'
    country = entry['country_code']
    tier = entry['tier_level']

    print('Site: %27s Exp Site: %44s Tier: %1s Country: %18s Cloud: %10s' %
          (site, exp_site, tier, country, cloud))
