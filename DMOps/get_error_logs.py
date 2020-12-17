#! /usr/bin/env python


"""
usage python get_error_logs.py --secret token --dst_rse T2_CH_CERN_Test
returns transfer errors for the last one hour
full object:
  "transfer_link": "https://fts3.cern.ch:8449/fts3/ftsmon/#/job/020b4be8-b77f-11ea-9dfd-fa163ef35aba",
  "account": null,
  "transfer_id": "020b4be8-b77f-11ea-9dfd-fa163ef35aba",
  "tool_id": "rucio-conveyor",
  "checksum_md5": null,
  "transferred_at": "2020-06-26 07:48:32",
  "scope": "cms",
  "event_type": "transfer-failed",
  "protocol": "srm",
  "tf_started_at": 1593157457,
  "src_url": "gsiftp://gridftp.echo.stfc.ac.uk:2811/cms:/store/test/rucio/cms//store/mc/RunIIAutumn18NanoAODv6/QstarToGJ_M-1000_f-0p5_TuneCP2_13TeV-pythia8/NANOAODSIM/Nano25Oct2019_102X_upgrade2018_realistic_v20-v1/40000/990EBCF2-6CB8-6045-A504-A1185458E8D6.root",
  "reason": "TRANSFER [70] DESTINATION OVERWRITE srm-ifce err: Communication error on send, err: [SE][srmRm][] httpg://lcg58.sinp.msu.ru:8446/srm/managerv2: CGSI-gSOAP running on fts446.cern.ch reports could not open connection to lcg58.sinp.msu.ru:8446\n\n",
  "activity": "User Subscriptions",
  "submitted_at": "2020-06-26 07:31:11",
  "created_at": null,
  "dst_url": "srm://lcg58.sinp.msu.ru:8446/srm/managerv2?SFN=/dpm/sinp.msu.ru/home/cms/store/test/rucio/cms//store/mc/RunIIAutumn18NanoAODv6/QstarToGJ_M-1000_f-0p5_TuneCP2_13TeV-pythia8/NANOAODSIM/Nano25Oct2019_102X_upgrade2018_realistic_v20-v1/40000/990EBCF2-6CB8-6045-A504-A1185458E8D6.root",
  "checksum_adler": "80a52915",
  "duration": 255,
  "tf_transferred_at": 1593157712,
  "file_size": 65153935,
  "dst_type": "DISK",
  "transfer_endpoint": "https://fts3.cern.ch:8446",
  "dst_rse": "T2_RU_SINP_Test",
  "bytes": 65153935,
  "name": "/store/mc/RunIIAutumn18NanoAODv6/QstarToGJ_M-1000_f-0p5_TuneCP2_13TeV-pythia8/NANOAODSIM/Nano25Oct2019_102X_upgrade2018_realistic_v20-v1/40000/990EBCF2-6CB8-6045-A504-A1185458E8D6.root",
  "tf_submitted_at": 1593156671,
  "purged_reason": "TRANSFER [70] DESTINATION OVERWRITE srm-ifce err: Communication error on send, err: [SE][srmRm][] IP|HOST:PORT/PATH CGSI-gSOAP running on HOST reports could not open connection to IP|HOST:PORT\n\n",
  "started_at": "2020-06-26 07:44:17",
  "guid": null,
  "vo": "cms",
  "src_type": "DISK",
  "previous_request_id": null,
  "src_rse": "T1_UK_RAL_Disk_Test",
  "request_id": "8a1f0ce603c24b8dafe60dd15d68988d"


"""

from __future__ import division, print_function
import argparse
import sys,getopt
import optparse
import os
import datetime

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='errors_from_kibana')
        self.parser.add_argument("--secret", help="provide grafana token", action="store", required=True)
        self.parser.add_argument("--src_rse", help="provide source RSE", action="store")
        self.parser.add_argument("--dst_rse", help="provide destination RSE", action="store")

def get_logs(secret, src_rse, dst_rse):
    if src_rse and dst_rse:
        os.system("""curl -s --location --request POST 'https://monit-grafana.cern.ch/api/datasources/proxy/9269/_msearch' --header 'Content-Type: application/json' --header 'Authorization: Bearer %s' -d '{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_cms_rucio_raw_events_*"]} 
{"size":500,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":"now-1h","lte":"now","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"*"}}]}},"sort":{"metadata.timestamp":{"order":"desc","unmapped_type":"boolean"}},"script_fields":{}}
' | jq '.responses[].hits.hits[]._source.data | select(.event_type == "transfer-failed" and .src_rse == "%s" and .dst_rse == "%s")' | jq '.src_rse +" "+.dst_rse +" "+.name + " " + .purged_reason +" "+ .request_id' | sed 's/"//g' """ % (secret, src_rse, dst_rse) )
    elif src_rse:
        os.system("""curl -s --location --request POST 'https://monit-grafana.cern.ch/api/datasources/proxy/9269/_msearch' --header 'Content-Type: application/json' --header 'Authorization: Bearer %s' -d '{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_cms_rucio_raw_events_*"]} 
{"size":500,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":"now-1h","lte":"now","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"*"}}]}},"sort":{"metadata.timestamp":{"order":"desc","unmapped_type":"boolean"}},"script_fields":{}}
' | jq '.responses[].hits.hits[]._source.data | select(.event_type == "transfer-failed" and .src_rse == "%s")' | jq '.src_rse +" "+.dst_rse +" "+.name + " " + .purged_reason +" "+ .request_id' | sed 's/"//g' """ % (secret, src_rse) )
    elif dst_rse:
        os.system("""curl -s --location --request POST 'https://monit-grafana.cern.ch/api/datasources/proxy/9269/_msearch' --header 'Content-Type: application/json' --header 'Authorization: Bearer %s' -d '{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_cms_rucio_raw_events_*"]} 
{"size":500,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":"now-1h","lte":"now","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"*"}}]}},"sort":{"metadata.timestamp":{"order":"desc","unmapped_type":"boolean"}},"script_fields":{}}
' | jq '.responses[].hits.hits[]._source.data | select(.event_type == "transfer-failed" and .dst_rse == "%s")' | jq '.src_rse +" "+.dst_rse +" "+.name + " " + .purged_reason +" "+ .request_id' | sed 's/"//g' """ % (secret, dst_rse) )
    else:
        os.system("""curl -s --location --request POST 'https://monit-grafana.cern.ch/api/datasources/proxy/9269/_msearch' --header 'Content-Type: application/json' --header 'Authorization: Bearer %s' -d '{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_cms_rucio_raw_events_*"]} 
{"size":500,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":"now-1h","lte":"now","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"*"}}]}},"sort":{"metadata.timestamp":{"order":"desc","unmapped_type":"boolean"}},"script_fields":{}}
' | jq '.responses[].hits.hits[]._source.data | select(.event_type == "transfer-failed")' | jq '.src_rse +" "+.dst_rse +" "+.name + " " + .purged_reason +" "+ .request_id' | sed 's/"//g' """ % (secret) )

def get_recent_errors(args):
    get_logs(args.secret, args.src_rse, args.dst_rse)
        
if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    get_recent_errors(args)
