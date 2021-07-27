#!/usr/bin/env python3
"""Fix destination file exists issue

Files are marked OK if
- The PFN has correct size
- The PFN has correct checksum
- The PFN has 'NEARLINE' in the user.status xattr (i.e., archived)

This is a short-term hack. Long-term solution:
https://github.com/rucio/rucio/issues/4447
"""
import argparse
import logging
from collections import defaultdict

import gfal2
from rucio.client import Client
from sqlalchemy import MetaData, create_engine, select

logger = logging.getLogger(__name__)


def build_query(engine, args):
    meta = MetaData(bind=engine, schema="cms_rucio_prod")
    meta.reflect(only=["replicas", "rses", "locks", "rules"])
    rses = meta.tables["cms_rucio_prod.rses"]
    replicas = meta.tables["cms_rucio_prod.replicas"]
    locks = meta.tables["cms_rucio_prod.locks"]
    rules = meta.tables["cms_rucio_prod.rules"]
    qry = (
        select(
            [
                replicas.c.scope,
                replicas.c.name,
                rses.c.rse,
                replicas.c.adler32,
                replicas.c.bytes,
                rules.c.id.label("rule_id"),
                rules.c.state.label("rule_state"),
            ]
        )
        .select_from(
            locks.join(
                replicas,
                (locks.c.scope == replicas.c.scope)
                & (locks.c.name == replicas.c.name)
                & (locks.c.rse_id == replicas.c.rse_id),
            )
            .join(rules)
            .join(rses)
        )
        .where(
            ((replicas.c.state == "C") | (replicas.c.state == "U"))
            & (rses.c.rse_type == "TAPE")
            & ((rules.c.state == "S") | (rules.c.state == "U"))
            & (
                rules.c.error
                == "RequestErrMsg.TRANSFER_FAILED:DESTINATION [17] Destination file exists and overwrite is not enabled"
            )
            & (locks.c.state == "S")
        )
        .limit(args.limit)
    )
    if args.rse is not None:
        qry = qry.where(rses.c.rse == args.rse)
    return qry


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Verbosity",
    )
    parser.add_argument(
        "--account",
        type=str,
        default="transfer_ops",
        help="Account to run under (default: %(default)s)",
    )
    parser.add_argument(
        "--connection",
        type=str,
        help="DB connection secret path",
        required=True,
    )
    parser.add_argument(
        "--dry",
        action="store_true",
        help="Dry-run (don't update replica status)",
    )
    parser.add_argument(
        "--rse",
        type=str,
        help="Limit search to a specific RSE",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Limit number of files (default: %(default)s)",
    )

    args = parser.parse_args()
    loglevel = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    logging.basicConfig(
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        level=loglevel[min(2, args.verbose)],
    )
    client = Client(account=args.account)
    ctx = gfal2.creat_context()
    with open(args.connection) as fin:
        engine = create_engine(fin.read())

    qry = build_query(engine, args)

    updated_replicas = defaultdict(list)
    updated_rules = set()
    for row in map(dict, qry.execute(engine)):
        lfn = row["scope"] + ":" + row["name"]
        row["rule_id"] = row["rule_id"].hex()
        logger.debug(f"Checking status for {row}")
        pfn = client.lfns2pfns(row["rse"], [lfn])[lfn]
        try:
            stat = ctx.stat(pfn)
        except gfal2.GError as ex:
            logger.info(f"Exception during stat of {pfn}: {str(ex)}")
            continue
        logger.debug(f"statinfo = {stat}")
        if stat.st_size != row["bytes"]:
            logger.info(
                f"Size mismatch for {pfn}: expected {row['bytes']} got {stat.st_size}"
            )
            continue
        checksum = ctx.checksum(pfn, "adler32")
        logger.debug(f"adler32 = {checksum}")
        if checksum != row["adler32"]:
            logger.info(
                f"Checksum mismatch for {pfn}: expected {row['adler32']} got {checksum}"
            )
            continue
        status = ctx.getxattr(pfn, "user.status")
        logger.debug(f"xattr user.status = {status}")
        if not (
            "NEARLINE" in status or lfn.startswith("cms:/store/test/loadtest/source/")
        ):
            logger.info(
                f"Tape archival not completed for {pfn}: xattr user.status = {status}"
            )
            continue
        logger.debug("Everything ok, can run client.update_replicas_states")
        updated_replicas[row["rse"]].append(
            {"scope": row["scope"], "name": row["name"], "state": "A"}
        )
        if row["rule_state"] == "U":
            logger.debug("Rule was suspended, updating it to stuck")
            updated_rules.add(row["rule_id"])

    nrep = sum(map(len, updated_replicas.values()))
    logger.info(
        f"Ready to update {nrep} replicas at {len(updated_replicas)} sites, and un-suspend {len(updated_rules)} rules"
    )
    if not args.dry:
        for rse, replicas in updated_replicas.items():
            logger.info(f"Working on {rse}, {len(replicas)} to update")
            client.update_replicas_states(rse, replicas)
        for rule_id in updated_rules:
            logger.info(f"Updating rule {rule_id} to stuck")
            client.update_replication_rule(rule_id, {"state": "STUCK"})
