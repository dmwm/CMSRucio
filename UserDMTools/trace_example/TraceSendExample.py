#! /usr/bin/env python3

"""
A small bit of example python code to send traces to Rucio.

The loop over all the existing replicas can be used if one does not know which replica was accessed.
Then it is safer, in a way, to mark all the replicas as accessed. But if one knows, just send a single trace with
the known replica.

The trace server runs unauthenticated on port 80, so a simple curl will suffice. The regular Rucio client requires
X509 or token authentication to find the RSEs of the replicas. As such the hostname of the trace server is redacted.
"""

import subprocess
from json import dumps

from rucio.client import Client

FILE = '/store/mc/Run3Summer23BPixNanoAODv12/GluGlutoH0PHto2Zto2E2Mu_M-125_Ga-SM_M4L-70_4LFilter_newPS_TuneCP5_13p6TeV_mcfm-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_postBPix_v6-v4/2820000/34bc470c-1909-4722-9bde-0607b43b4c29.root'

cl = Client()

dids = [{'scope': 'cms', 'name': FILE}]

rr = cl.list_replicas(dids)
for replica in rr:
    for rse, availability in replica['states'].items():
        if availability == 'AVAILABLE':
            touch_frame = {'eventType': 'touch', 'clientState': 'DONE', 'account': None, 'localSite': rse,
                           'remoteSite': rse, 'scope': 'cms', 'filename': FILE}

            command = ['curl', '-X', 'POST', 'http://xxx.xxx.xxx/traces/', '-H',
                       'Content-Type: application/json', '-d', dumps(touch_frame)]

            result = subprocess.run(command, capture_output=True, text=True)
            print()
            print(result)
