#! /usr/bin/env python3

from __future__ import absolute_import, division, print_function

import io
import json
import os
import time

import requests
#
def send_trace(trace, trace_endpoint, user_agent, retries=5):
    """
    Send the given trace to the trace endpoint
    :param trace: the trace dictionary to send
    :param trace_endpoint: the endpoint where the trace should be send
    :param user_agent: the user agent sending the trace
    :param retries: the number of retries if sending fails
    :return: 0 on success, 1 on failure
    """
    if user_agent.startswith('pilot'):
        return 0
    for dummy in range(retries):
        try:
            requests.post(trace_endpoint + '/traces/', verify=False, data=json.dumps(trace))
            return 0
        except Exception as err:
            print('Handling run-time error:', err)
            return 1
    return 1

def collect_traces():
    """
    Query WMArchive ES DB to get FWJRs. Then create the traces from FWJRs and send them to trace server.
    We set the search time interval from "now-12m" to now for  a 12 min window. We plan to run the cron for every 10 minutes.   
    """

    print ("**** Starting time **** ", time.asctime(time.gmtime()))
    t1 = int(time.time())

    QUERY_HEADER = '{"search_type":"query_then_fetch","ignore_unavailable":true}'

    with open('query_collect.json', 'r') as WMArchive_json:
        wma = json.load(WMArchive_json)

    # metadata.timestamp uses ms for unit.      
    trange = {
            "range": {
                        "metadata.timestamp":{
                                            "gte": int(time.time() - 12*60)*1000,
                                            "lte": int(time.time())*1000
                                            }
                        }
            }
    # adding time
    wma["query"]["bool"]["must"].append(trange)
    print("**** ES Query ****  ", json.dumps(wma))
    query = io.StringIO(QUERY_HEADER + '\n' + json.dumps(wma) + '\n')
    
    headers = {'Authorization': 'Bearer %s' % os.environ['MONIT_TOKEN'],
            'Content-Type': 'application/json'}
    
    r = requests.post('https://monit-grafana.cern.ch/api/datasources/proxy/9545/_msearch', data=query, headers=headers)
    if not (200 <= r.status_code <= 229):
        print("Error for query ES. Http error code: ", r.status_code)
        print(r.text)
        #TODO we need wait and redo. How many times to try? In addition, we need to have better error handing when running with cron
        exit(1)
    #
    result = json.loads(r.text)
    t2 = int(time.time())
    print("**** Time spend on ES **** ", t2-t1)
    traces = []
    moreQuery = False
    hits = 0 
    for key, value in result.items():
        #print(json.dumps(value[0]))
        for v in value:
            #there are two ways to get the total hits and they shoube the same
            l = len(v['hits']['hits'])
            total = v['hits']['total']
            if ( l != total):
                #TODO raise exception?
                print ("Error: result array size ", l , "is not eaual to total hits ",  total)
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

                for s in data['steps']:
                    if 'input' in s.keys():
                        for lfndict in s['input']:
                            if 'lfn' in lfndict.keys():
                                if lfndict['lfn'] in fallbackFiles:
                                    pass
                                else:
                                    if 'lfn' in LFNArrayRef:
                                        goodlfn.append(LFNArray[lfndict['lfn']])
                    if 'site' in s.keys():
                        site = s['site']

                if goodlfn and site:
                    for g in goodlfn:
                        trace={'eventVersion': 'API_1.21.6', 'clientState': 'DONE', 'scope': 'cms', 'eventType': 'get', 
                                'usrdn':'/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=yuyi/CN=639751/CN=Yuyi Guo/CN=706639693', 'account':'yuyi'}
                        trace['filename'] = g
                        trace['remoteSite'] = site
                        trace['DID'] = 'cms:'+  trace['filename']
                        trace['file_read_ts'] = ts
                        trace['jobtype'] = jobtype
                        trace['wn_name'] = wn_name
                        if ts:
                            trace['timestamp'] = ts
                        else:
                            trace['timestamp'] = int(time.time())
                        trace['traceTimeentryUnix'] = trace['timestamp']

                        traces.append(trace)
                        
    if moreQuery:
        print("There are more than 10k hits. ")
    # TODO: we need to get the rest hits here. 
    print("***total traces: "  + str(len(traces)))   
    t3 = int(time.time())
    print("**** Time spend on making traces **** ", t3-t2)   

    print ("**** Starting time for sending traces **** ", time.asctime(time.gmtime()))           
    for t in traces:
        r = send_trace(t, "http://cmsrucio-trace-int.cern.ch", "CMS_trace_generator")
        if r != 0:
            print("***Error when send trace ***")
            print(t)
    t4 = int(time.time())
    print ("**** Starting time for sending traces **** ", time.asctime(time.gmtime()))   
    print("**** Time spend on sending traces **** ", t4-t3)
    #print(traces)
    #f = {}
    #f['data'] = traces
    #print(json.dumps(f))
    print("*** All Done ***")

def main():
    collect_traces()

if __name__ == "__main__":
    main()
