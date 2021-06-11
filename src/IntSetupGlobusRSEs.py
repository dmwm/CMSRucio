#! /usr/bin/env python3

from rucio.client.rseclient import RSEClient
from rucio.common.exception import Duplicate, RSEProtocolNotSupported


rseclient = RSEClient()
rse_name = 'T3_US_Theta' 
rse_properties = {'ASN': 'ASN', 'availability': 7, 'deterministic': True, 'volatile': False, 'city': 'Chicago', 'region_code': 'IL', 'country_name': 'US', 'continent': 'NA', 
'time_zone': 'America/Chicago', 'ISP': None, 'staging_area': False, 'rse_type': 'DISK', 'longitude': 41.88, 'latitude': -87.63}
try:
    r = rseclient.add_rse(rse_name, **rse_properties) # r is true on success
    print('Added Theta %s' % r)
except Duplicate: 
    print('Theta existed')
    
prefix = '/lus/theta-fs0/projects/HighLumin/uscms/' # Be sure to use a relative path for your endpoint 

params = {'scheme': 'globus', 'prefix': prefix, 
'impl': 'rucio.rse.protocols.globus.GlobusRSEProtocol', 'third_party_copy': 1, 'domains': {"lan": {"read": 1,"write": 1,"delete": 1},"wan": {"read": 1,"write": 1,"delete": 1}}} 

try:
    p = rseclient.add_protocol(rse_name, params) # p is true on success
    print('Added protocol to Theta %s' % p)
except Duplicate:
    print('Protocol existed on Theta')

#result = rseclient.add_rse_attribute(rse = rse_name, key = 'naming_convention', value = 'surl') # This is the value for relative SURL
result = rseclient.add_rse_attribute(rse = rse_name, key = 'globus_endpoint_id', value = '08925f04-569f-11e7-bef8-22000b9a448b')
result = rseclient.add_rse_attribute(rse = rse_name, key = 'lfn2pfn_algorithm', value = 'cmstfc')
#result = rseclient.add_rse_attribute(rse = rse_name, key = 'istape', value = False)

rse_name = 'T3_US_NERSC'


prefix = '/global/cscratch1/sd/uscms/rucio/cms//store/test/rucio/int/'
params = {'scheme': 'globus', 'prefix': prefix, 
'impl': 'rucio.rse.protocols.globus.GlobusRSEProtocol', 'third_party_copy': 1, 'domains': {"lan": {"read": 1,"write": 1,"delete": 1},"wan": {"read": 1,"write": 1,"delete": 1}}} 
try:
    rseclient.delete_protocols(rse_name, scheme='globus')
except RSEProtocolNotSupported:
    print("NERSC did not have a Globus protocol")
p = rseclient.add_protocol(rse_name, params) # p is true on success
print('Added protocol to NERSC %s' % p)


result = rseclient.add_rse_attribute(rse = rse_name, key = 'globus_endpoint_id', value = '9d6d994a-6d04-11e5-ba46-22000b92c6ec')

