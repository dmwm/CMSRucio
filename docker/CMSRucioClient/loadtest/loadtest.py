#!/usr/bin/env python3
"""Load test daemon for rucio

This daemon creates a rule for each link between a configurable set of
source and destination RSEs, and periodically resets replica states for
the replicas corresponding to those rules to force transfers to happen.
The rate is configured by the number of files in the loadtest dataset and
the rate parameter embedded in the rule comments. All state information
is embedded in rucio, so this daemon can act statelessly each cycle.

Each RSE gets its own loadtest dataset DID, which is autmatically created
by this script with a configurable file size.

Nick Smith <nick.smith@cern.ch>
"""
import argparse
import datetime
import logging
import os
import random
import re
import threading
import time

from rucio.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import (
    DataIdentifierNotFound,
    DestinationNotAccessible,
    DuplicateRule,
    InvalidRSEExpression,
    NoFilesUploaded,
    ReplicaNotFound,
    RSEWriteBlocked,
    ServiceUnavailable,
    SourceNotFound,
    ReplicationRuleCreationTemporaryFailed,
)
from rucio.rse import rsemanager

logger = logging.getLogger(__name__)
ALLOWED_FILESIZES = {
    # motivation: one file every 6h = 100kbps avg. rate
    "270MB": 270000000,
    "2700MB": 2700000000,
}
LOADTEST_DATASET_FMT = "/LoadTestSource/{rse}/TEST#{filesize}"
LOADTEST_LFNDIR_FMT = "/store/test/loadtest/source/{rse}/"
LOADTEST_LFNBASE_FMT = "urandom.{filesize}.file{filenumber:04d}"
FILENUMBER_SEARCH = "/store/test/loadtest/source/{rse}/urandom.{filesize}.file*"
FILENUMBER_RE = re.compile(r".*\.file(\d+)$")
DEFAULT_RULE_COMMENT = "rate:100kbps"
TARGET_CYCLE_TIME = 60 * 5
ACTIVE = True


def generate_file(basename, nbytes):
    """Generates and writes a file

    Parameters
    ----------
        basename: str
        nbytes: int
    """
    with open("/dev/urandom", "rb") as fin:
        with open(basename, "wb") as fout:
            while nbytes > 0:
                b = min(1024 * 1024, nbytes)
                fout.write(fin.read(b))
                nbytes -= b


def prepare_upload_item(rse, filesize, filenumber):
    """Prepare a pseudorandom test file to upload to an RSE

    Parameters
    ----------
        rse: str
        filesize: str
        filenumber: int
    """
    dataset = LOADTEST_DATASET_FMT.format(rse=rse, filesize=filesize)
    dirname = LOADTEST_LFNDIR_FMT.format(rse=rse)
    basename = LOADTEST_LFNBASE_FMT.format(filesize=filesize, filenumber=filenumber)
    generate_file(basename, ALLOWED_FILESIZES[filesize])
    return {
        "path": basename,
        "rse": rse,
        "did_scope": "cms",
        "did_name": dirname + basename,
        "dataset_scope": "cms",
        "dataset_name": dataset,
        "register_after_upload": True,
    }


def ensure_rse_self_expression(client, rse):
    """Ensure one can use RSE name in expression

    RSE expressions with just the RSE name are resolved to a single RSE
    by having an attribute with the same name set to true for that RSE
    and not set on any other. This function ensures that fact.
    """
    try:
        matching = list(client.list_rses(rse_expression=rse))
        found = False
        for item in matching:
            if item["rse"] == rse:
                found = True
            else:
                logger.warning(
                    f"Found extraneous RSE {item['rse']} when checking RSE self-expression on {rse}"
                )
                client.delete_rse_attribute(item["rse"], rse)
        if not found:
            logger.info(f"Repairing RSE self-expression for {rse}")
            client.add_rse_attribute(rse, rse, True)
    except InvalidRSEExpression as ex:
        if ex.message == "RSE Expression resulted in an empty set.":
            logger.info(f"Repairing RSE self-expression for {rse}")
            client.add_rse_attribute(rse, rse, True)
        else:
            raise ex


def next_available_filenumber(client, rse, filesize):
    did_search = FILENUMBER_SEARCH.format(rse=rse, filesize=filesize)
    max_fn = -1
    for did in client.list_dids("cms", {"name": did_search}, type="file"):
        m = FILENUMBER_RE.match(did)
        if m:
            max_fn = max(max_fn, int(m.groups()[0]))
        else:
            logger.error(f"Failed to match filenumber regex to DID {did}")
            # we'll return what we have anyway, since the upload will fail later
    return max_fn + 1


def upload_source_data(client, uploader, rse, filesize, filenumber):
    item = prepare_upload_item(rse, filesize, filenumber)
    try:
        # Load test operations should be independent of site availability
        uploader.upload([item], ignore_availability=True)
        return True
    except InvalidRSEExpression:
        logger.error(f"RSE {rse} is missing self-expression")
    except NoFilesUploaded:
        logger.error(f"RSE {rse} was unable to upload loadtest file {item}")
    except RSEWriteBlocked:
        logger.error(f"RSE {rse} is disabled for writes")
    except DestinationNotAccessible:
        logger.error(f"RSE {rse} has permission issues")
    except ServiceUnavailable:
        logger.error(f"RSE {rse} host appears down")
    except ReplicationRuleCreationTemporaryFailed:
        # TODO: will return to this and probably prefer creating rules on the existing replica
        logger.error(f"RSE {rse} already has a LoadTest replica")
    return False


def parse_rate(comment):
    si_prefix = {"k": 1e3, "M": 1e6, "G": 1e9}
    if comment is None:
        comment = DEFAULT_RULE_COMMENT
    if comment.startswith("rate:") and comment.endswith("bps"):
        number = comment[5:-3]
        if number[-1] in si_prefix:
            return float(number[:-1]) * si_prefix[number[-1]]
        else:
            return float(number)
    raise ValueError(f"Rule comment {comment} not parseable")


def delete_replicas(client, dest_rse, replicas):
    rse_settings = rsemanager.get_rse_info(dest_rse)
    # we would expect "delete" operation but tape sites have that disabled for safety
    protocol_delete = rsemanager.create_protocol(
        rse_settings, operation="read", domain="wan", logger=logger.log
    )
    lfns = [lfn["scope"] + ":" + lfn["name"] for lfn in replicas]
    pfns = client.lfns2pfns(dest_rse, lfns, operation="read")
    protocol_delete.connect()
    for pfn in pfns.values():
        logger.debug(f"Deleting PFN {pfn} from destination RSE {dest_rse}")
        try:
            protocol_delete.delete(pfn)
        except SourceNotFound:
            pass


def update_loadtest(
    client, source_rse, dest_rse, source_files, rule, dataset, account, activity
):
    links = client.get_distance(source_rse, dest_rse)
    if len(links) == 0 and rule is not None:
        logger.info(
            f"No link between {source_rse} and {dest_rse}, removing rule {rule['id']}"
        )
        client.update_replication_rule(rule["id"], {"lifetime": 0})
        return None
    elif len(links) == 0:
        logger.info(f"No link between {source_rse} and {dest_rse}, skipping load test")
        return None
    elif len(links) > 1:
        logger.error(
            "I have no idea what it means to have multiple links, carrying on..."
        )
        return None
    if rule is None:
        logger.info(
            f"New link between {source_rse} and {dest_rse}, creating a load test rule this cycle"
        )
        rule = {
            "dids": [{"scope": "cms", "name": dataset}],
            "copies": 1,
            "rse_expression": dest_rse,
            "source_replica_expression": source_rse,
            "account": account,
            "activity": activity,
            "purge_replicas": True,
            "ignore_availability": True,
            "grouping": "DATASET",
            "comment": DEFAULT_RULE_COMMENT,
        }
        logger.debug("Creating rule: %r" % rule)
        try:
            client.add_replication_rule(**rule)
        except DuplicateRule:
            logger.error(f"Found a duplicate rule while making {rule}, removing")
            for other_rule in client.list_replication_rules(
                {
                    "scope": "cms",
                    "name": dataset,
                    "rse_expression": dest_rse,
                    "copies": 1,
                }
            ):
                client.update_replication_rule(other_rule["id"], {"lifetime": 0})
        return False
    if rule["state"] == "SUSPENDED":
        logger.debug(
            f"Existing link between {source_rse} and {dest_rse} with load test rule {rule['id']} is suspended, resetting to stuck"
        )
        client.update_replication_rule(rule["id"], {"state": "STUCK"})
    elif rule["state"] != "OK":
        logger.debug(
            f"Existing link between {source_rse} and {dest_rse} with load test rule {rule['id']} is in state {rule['state']}, will skip load test replica update"
        )
        return False
    update_dt = (datetime.datetime.utcnow() - rule["updated_at"]).total_seconds()
    # judge-repairer will re-transfer after 2h-8h depending on rule creation time
    # so max rate would eventually be filesize * nfiles / (8*3600) = 75 kbps for defaults
    # we eventually want to calibrate this to resubmit targeting the desired avg. rate
    try:
        target_rate = parse_rate(rule["comments"])
    except ValueError as ex:
        logger.error(f"Error parsing loadtest rule {rule['id']}: {ex.message}")
        return False
    data_volume = 8.0 * sum(file["bytes"] for file in source_files)
    delay_time = max(data_volume / target_rate, 0)
    delay_jitter = max(0.2 * delay_time, 3600.0)
    min_time = delay_time - delay_jitter
    if update_dt < min_time or random.random() > TARGET_CYCLE_TIME / delay_jitter:
        return False
    logger.info(
        f"Link between {source_rse} and {dest_rse} with load test rule {rule['id']} last updated {update_dt}s ago (target={delay_time}), marking destination replicas unavailable"
    )
    replicas = [
        {"scope": file["scope"], "name": file["name"], "state": "U"}
        for file in source_files
    ]
    logger.debug("Updating status for replicas: %r at RSE %s" % (replicas, dest_rse))
    # Rules made to tape RSEs are locked by default, so this is a way to check if it is tape
    # if so, we need to physically remove the replica because the FTS job will not overwrite
    # the previous file, unlike for disk. If the deletion fails, try again later
    try:
        if rule["locked"]:
            delete_replicas(client, dest_rse, replicas)
    except ServiceUnavailable:
        return False
    try:
        client.update_replicas_states(dest_rse, replicas)
    except ReplicaNotFound:
        # Race condition between adding new DID to the dataset and judge-evaluator updating the rule
        return False
    return True


def run(source_rse_expression, dest_rse_expression, account, activity, filesize):
    if filesize not in ALLOWED_FILESIZES:
        raise ValueError(f"File size {filesize} not allowed")

    client = Client(account=account)
    uploader = UploadClient(_client=client, logger=logger)

    while ACTIVE:
        cycle_start = datetime.datetime.utcnow()
        source_rses = [item["rse"] for item in client.list_rses(source_rse_expression)]
        dest_rses = [item["rse"] for item in client.list_rses(dest_rse_expression)]

        for source_rse in source_rses:
            dataset = LOADTEST_DATASET_FMT.format(rse=source_rse, filesize=filesize)
            try:
                source_files = list(client.list_files("cms", dataset))
            except DataIdentifierNotFound:
                logger.info(f"RSE {source_rse} has no source files, will create one")
                source_files = []

            # here we might consider requiring a minimum number of source files to achieve a target rate
            if len(source_files) < 1:
                next_filenumber = next_available_filenumber(
                    client, source_rse, filesize
                )
                success = upload_source_data(
                    client, uploader, source_rse, filesize, next_filenumber
                )
                if not success:
                    logger.error(
                        f"RSE {source_rse} has no source files and could not upload, skipping"
                    )
                    continue
                source_files = list(client.list_files("cms", dataset))

            dest_rules = client.list_replication_rules(
                {
                    "scope": "cms",
                    "name": dataset,
                    "account": account,
                    "activity": activity,
                }
            )
            dest_rules = {
                rule["rse_expression"]: rule
                for rule in dest_rules
                if rule["source_replica_expression"] == source_rse
            }

            for dest_rse in dest_rses:
                if dest_rse == source_rse:
                    continue
                dest_rule = dest_rules.get(dest_rse, None)
                update_loadtest(
                    client,
                    source_rse,
                    dest_rse,
                    source_files,
                    dest_rule,
                    dataset,
                    account,
                    activity,
                )

        cycle_time = (datetime.datetime.utcnow() - cycle_start).total_seconds()
        logger.info(f"Completed loadtest cycle in {cycle_time}s")
        while cycle_time < TARGET_CYCLE_TIME and ACTIVE:
            dt = min(1, TARGET_CYCLE_TIME - cycle_time + 1e-3)
            time.sleep(dt)
            cycle_time += dt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create periodic transfers between RSEs to test links"
    )
    parser.add_argument(
        "--source_rse_expression",
        type=str,
        help="Source RSEs to test links from",
        required=True,
    )
    parser.add_argument(
        "--dest_rse_expression",
        type=str,
        help="Destination RSEs to test links to",
        required=True,
    )
    parser.add_argument(
        "--account",
        type=str,
        default="transfer_ops",
        help="Account to run tests under (default: %(default)s)",
    )
    parser.add_argument(
        "--activity",
        type=str,
        default="Functional Test",
        help="Activity to submit transfers (default: %(default)s)",
    )
    parser.add_argument(
        "--filesize",
        type=str,
        default="270MB",
        help="Size of load test files (default: %(default)s)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Verbosity",
    )

    args = parser.parse_args()

    loglevel = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    logging.basicConfig(
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        level=loglevel[min(2, args.verbose)],
    )

    # UploadClient doesn't seem to pay attention to the client's account setting
    os.environ["RUCIO_ACCOUNT"] = args.account

    thread = threading.Thread(
        target=run,
        args=(
            args.source_rse_expression,
            args.dest_rse_expression,
            args.account,
            args.activity,
            args.filesize,
        ),
    )
    thread.start()
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    ACTIVE = False
    thread.join()
    exit(0)
