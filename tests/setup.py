#!/usr/bin/env python
from __future__ import print_function
from rucio.common.exception import RucioException
from rucio.client.client import Client
from rucio.db.sqla.util import build_database, destroy_database, create_root_account, create_base_vo

destroy_database()
build_database()
create_base_vo()
create_root_account()

client = Client(
    account="root",
)

try:
    client.add_scope(account="root", scope="cms")
    client.add_account("wma_prod", "SERVICE", "wmagent@cern.ch")
    client.add_identity(
        account="wma_prod",
        identity="ddmlab",
        authtype="USERPASS",
        email="blah@asfd.com",
    )

    # why is this still a workaround?
    from rucio.core.account import add_account_attribute
    from rucio.common.types import InternalAccount

    add_account_attribute(InternalAccount("wma_prod"), "admin", True)

    # as wma_prod is an admin account, this has no effect
    # client.set_account_limit('wma_prod', 'T2_US_Wisconsin_Mock', int(20e9), 'local')
except RucioException:
    print("already made wma_prod")
    pass


prod_rses = [
    "T1_US_FNAL_Mock",
    "T2_CH_CERN_Mock",
    "T2_US_Florida_Mock",
    "T2_US_Wisconsin_Mock",
]
for rse in prod_rses:
    try:
        client.add_rse(
            rse,
            deterministic=True,
            volatile=False,
            country_name=rse.split("_")[1],
        )
        client.add_protocol(
            rse,
            params={
                "scheme": "mock",
                "impl": "rucio.rse.protocols.mock.Default",
                "domains": {
                    "lan": {
                        "read": 1,
                        "write": 1,
                        "delete": 1
                    },
                    "wan": {
                        "read": 1,
                        "write": 1,
                        "third_party_copy": 1,
                        "delete": 1
                    }
                },
                "prefix": "/tmp/rucio_rse/"
            },
        )
        client.add_rse_attribute(rse, "production_buffer", True)
    except RucioException:
        print("Already added RSE %s" % rse)
        pass

for rse in prod_rses:
    for rse2 in prod_rses:
        if rse2 == rse:
            continue
        try:
            client.add_distance(rse, rse2, {"distance": 1, "ranking": 1})
        except RucioException:
            pass


client.add_key(
    key="workflowActivity",
    key_type="COLLECTION",
    # https://github.com/rucio/rucio/issues/3512
    # value_type=str,
    # value_regexp="\w{,50}",
)
