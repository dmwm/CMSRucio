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


prod_rses = {
    "T1_US_FNAL_Mock": {"cms_type": "real", "region": "A"},
    "T2_CH_CERN_Mock": {"cms_type": "real", "region": "B"},
    "T2_US_Florida_Mock": {"cms_type": "real", "region": "A"},
    "T2_US_Wisconsin_Mock": {"cms_type": "real", "region": "A"},
    "T2_BE_IIHE_Mock": {"cms_type": "real", "region": "B"},
    "T2_CN_Beijing_Mock": {"cms_type": "real", "region": "C"},
}

for rse, attr in prod_rses.items():
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
    for key, value in attr.items():
        client.add_rse_attribute(rse, key, value)

for rse in prod_rses:
    for rse2 in prod_rses:
        if rse2 == rse:
            continue
        client.add_distance(rse, rse2, {"distance": 1, "ranking": 1})

client.add_scope(account="root", scope="cms")

client.add_account("transfer_ops", "SERVICE", "wmagent@cern.ch")
client.add_identity(
    account="transfer_ops",
    identity="ddmlab",
    authtype="USERPASS",
    email="blah@asfd.com",
)
# why is this still a workaround?
from rucio.core.account import add_account_attribute
from rucio.common.types import InternalAccount
add_account_attribute(InternalAccount("transfer_ops"), "admin", True)

client.add_account("wma_prod", "SERVICE", "wmagent@cern.ch")
client.add_identity(
    account="wma_prod",
    identity="ddmlab",
    authtype="USERPASS",
    email="blah@asfd.com",
)
add_account_attribute(InternalAccount("wma_prod"), "admin", True)
# as wma_prod is an admin account, this has no effect
client.set_account_limit('wma_prod', 'T2_US_Wisconsin_Mock', int(20e9), 'local')

client.add_account("jdoe", "USER", "j@doe.com")
client.add_scope(account="jdoe", scope="user.jdoe")
client.add_identity(
    account="jdoe",
    identity="ddmlab",
    authtype="USERPASS",
    email="blah@asfd.com",
)
client.set_account_limit('jdoe', 'T2_US_Florida_Mock', int(20e9), 'local')
