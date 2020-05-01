#!/usr/bin/env python
from __future__ import print_function
from rucio.common.exception import ReplicaNotFound
from rucio.common.types import InternalAccount, InternalScope
from rucio.core.did import list_files
from rucio.core.rse import get_rse_id
from rucio.core.replica import update_replicas_states, add_replicas, delete_replicas
from rucio.core.rule import get_rule, add_rule, delete_rule
from rucio.db.sqla.session import transactional_session
from rucio.db.sqla.constants import DIDType
import json


@transactional_session
def add_sync_rule(rse, scope, block, account, session=None):
    rse_id = get_rse_id(rse=rse, session=session)
    scope = InternalScope(scope)
    account = InternalAccount(account)

    files = []
    for file in list_files(scope, block, long=False, session=session):
        try:
            update_replicas_states(
                replicas=[
                    {
                        "scope": scope,
                        "name": file["name"],
                        "rse_id": rse_id,
                        "state": "A",
                    }
                ],
                add_tombstone=False,
                session=session,
            )
        except ReplicaNotFound:
            add_replicas(
                rse_id=rse_id,
                files=[
                    {
                        "scope": scope,
                        "name": file["name"],
                        "bytes": file["bytes"],
                        "adler32": file["adler32"],
                    }
                ],
                account=account,
                ignore_availability=True,
                session=session,
            )

    rules = add_rule(
        dids=[{"scope": scope, "name": block}],
        copies=1,
        rse_expression=rse,
        weight=None,
        lifetime=None,
        grouping="DATASET",
        account=account,
        locked=False,
        subscription_id=None,
        source_replica_expression=None,
        activity="Data Consolidation",
        notify=None,
        purge_replicas=False,
        ignore_availability=True,
        comment=None,
        ask_approval=False,
        asynchronous=False,
        priority=3,
        split_container=False,
        meta=json.dumps({"phedex_group": "DataOps", "phedex_custodial": True}),
        session=session,
    )
    return rules[0]


@transactional_session
def delete_sync_rule(rule_id, session=None):
    rule = get_rule(rule_id, session=session)
    if rule["did_type"] != DIDType.DATASET:
        raise RuntimeError("Rule applies to did with wrong type")
    block = rule["name"]
    try:
        rse_id = get_rse_id(rse=rule["rse_expression"], session=session)
    except RSENotFound:
        raise RuntimeError("Rule does not apply to a specific RSE")
    scope = rule["scope"]
    account = rule["account"]

    files = []
    for file in list_files(scope, block, long=False, session=session):
        files.append(
            {"scope": scope, "name": file["name"], "rse_id": rse_id, "state": "U"}
        )

    update_replicas_states(
        replicas=files, add_tombstone=False, session=session
    )
    delete_rule(rule_id=rule_id, purge_replicas=True, soft=False, session=session)


rse = "T2_US_Florida_Mock"
scope = "cms"
block = (
    "/AnotherSample/RunIIBlah-something/AODSIM#aa4b593e-840c-11ea-850c-0242ac180004"
)
account = "wma_prod"

rule_id = add_sync_rule(rse, scope, block, account)
print(rule_id)
delete_sync_rule(rule_id)
