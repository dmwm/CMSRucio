#!/usr/bin/env python
from __future__ import print_function
from rucio.common.exception import RucioException
from rucio.client.client import Client


client = Client(
    account="root",
)

# https://github.com/dmwm/WMCore/issues/9655
client.add_key(
    key="workflowActivity",
    key_type="COLLECTION",
)

