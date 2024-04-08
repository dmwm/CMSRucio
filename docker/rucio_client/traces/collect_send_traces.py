#! /usr/bin/env python3

from __future__ import absolute_import, division, print_function

import io
import json
import os
import time

import requests

QUERY_HEADER = '{"search_type":"query_then_fetch","ignore_unavailable":true}'
WMA_URL = 'https://monit-grafana.cern.ch/api/datasources/proxy/7572/_msearch'
SITE_MAP = {
    'T1_US_FNAL': 'T1_US_FNAL_Disk',
    'T1_DE_KIT': 'T1_DE_KIT_Disk',
    'T1_ES_PIC': 'T1_ES_PIC_Disk',
    'T1_RU_JINR': 'T1_RU_JINR_Disk',
    'T1_FR_CCIN2P3': 'T1_FR_CCIN2P3_Disk',
    'T1_UK_RAL': 'T1_UK_RAL_Disk',
    'T1_IT_CNAF': 'T1_IT_CNAF_Disk',
    'T2_CH_CERN_HLT': 'T2_CH_CERN',
    'T2_CH_CERNBOX': 'T2_CH_CERN',
    'T3_UK_SGrid_Oxford': 'T2_UK_London_IC',
}


def send_trace(trace, trace_endpoint, user_agent, retries=5):
    """
    Send the given trace to the trace endpoint
    :param trace: the trace dictionary to send
    :param trace_endpoint: the endpoint where the trace should be send
    :param user_agent: the user agent sending the trace
    :param retries: the number of retries if sending fails
    :return: True on success, False on failure
    """
    if user_agent.startswith('pilot'):
        return True
    for dummy in range(retries):
        try:
            requests.post(trace_endpoint + '/traces/', verify=False, data=json.dumps(trace))
            return True
        except Exception as err:
            # ToDO write to log instead of print
            print('Handling run-time error:', err)
    return False


def collect_traces():
    """
    Query WMArchive ES DB to get FWJRs. Then create the traces from FWJRs and send them to trace server.
    We set the search time interval from "now-12m" to now for  a 12 min window. We plan to run the cron for every 10 minutes.   
    """

    print("**** Starting time **** ", time.asctime(time.gmtime()))
    t1 = int(time.time())

    with open('query_collect.json', 'r') as WMArchive_json:
        wma = json.load(WMArchive_json)

    # metadata.timestamp uses ms for unit.      
    trange = {
        "range": {
            "metadata.timestamp": {
                "gte": int(time.time() - 12 * 60) * 1000,
                "lte": int(time.time()) * 1000
            }
        }
    }
    # adding time
    wma["query"]["bool"]["must"].append(trange)
    print("**** ES Query ****  ", json.dumps(wma))
    query = io.StringIO(QUERY_HEADER + '\n' + json.dumps(wma) + '\n')

    headers = {'Authorization': 'Bearer %s' % os.environ['MONIT_TOKEN'],
               'Content-Type': 'application/json'}

    r = requests.post(WMA_URL, data=query, headers=headers)
    if not (200 <= r.status_code <= 229):
        print("Error for query ES. Http error code: ", r.status_code)
        print(r.text)
        # TODO we need wait and redo. How many times to try? In addition, we need to have better error handing when running with cron
        exit(1)
    #
    result = json.loads(r.text)
    t2 = int(time.time())
    print("**** Time spend on ES **** ", t2 - t1)
    traces = []
    moreQuery = False
    hits = 0
    for key, value in result.items():
        if key == "responses":
            # print(json.dumps(value[0]))
            for v in value:
                # there are two ways to get the total hits and they shoube the same
                length = len(v['hits']['hits'])
                total = v['hits']['total']['value']
                if length != total:
                    # TODO raise exception?
                    print("Error: result array size ", length, "is not equal to total hits ", total)
                    exit(1)
                print("*** total number of hits: ", total)
                if total > 10000:
                    moreQuery = True
                    hits += 1

                for h in v['hits']['hits']:
                    data = h['_source']['data']
                    LFNArrayRef = data['LFNArrayRef']
                    fallbackFiles = data['fallbackFiles']
                    LFNArray = data['LFNArray']

                    trace = {}
                    goodlfn = []
                    site = ''
                    ts = data['meta_data'].get('ts', int(time.time()))
                    jobtype = data['meta_data'].get('jobtype', 'unknown')
                    wn_name = data['meta_data'].get('wn_name', 'unknown')

                    for step in data['steps']:
                        if 'input' in step:
                            for lfndict in step['input']:
                                if 'lfn' in lfndict:
                                    if lfndict['lfn'] in fallbackFiles:
                                        pass
                                    else:
                                        if 'lfn' in LFNArrayRef:
                                            goodlfn.append(LFNArray[lfndict['lfn']])
                        if 'site' in step:
                            site = step['site']

                    if goodlfn and site:
                        if site in SITE_MAP:
                            site = SITE_MAP[site]
                        for g in goodlfn:
                            trace.update(
                                {'eventVersion': 'API_1.21.6', 'clientState': 'DONE', 'scope': 'cms', 'eventType': 'get',
                                'usrdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=yuyi/CN=639751/CN=Yuyi Guo/CN=706639693',
                                'account': 'yuyi'})
                            trace.update(filename=g)
                            trace.update(remoteSite=site)
                            trace.update(DID='cms:' + trace['filename'])
                            trace.update(file_read_ts=ts)
                            trace.update(jobtype=jobtype)
                            trace.update(wn_name=wn_name)
                            if ts:
                                trace.update(timestamp=ts)
                            else:
                                trace.update(timestamp=int(time.time()))
                            trace.update(traceTimeentryUnix=trace['timestamp'])
                            traces.append(trace)

    if moreQuery:
        print("There are more than 10k hits. ")
    # TODO: we need to get the rest hits here. 
    print("***total traces: " + str(len(traces)))
    t3 = int(time.time())
    print("**** Time spend on making traces **** ", t3 - t2)

    print("**** Starting time for sending traces **** ", time.asctime(time.gmtime()))
    for t in traces:
        trace_server = 'http://' + os.environ['RUCIO_TRACE_SERVER']
        r = send_trace(t, trace_server, "CMS_trace_generator")
        if not r:
            print("***Error when send trace ***")
            print(t)
    t4 = int(time.time())
    print("**** Starting time for sending traces **** ", time.asctime(time.gmtime()))
    print("**** Time spend on sending traces **** ", t4 - t3)
    # print(traces)
    # f = {}
    # f['data'] = traces
    # print(json.dumps(f))
    print("*** All Done ***")


def main():
    collect_traces()


if __name__ == "__main__":
    main()
