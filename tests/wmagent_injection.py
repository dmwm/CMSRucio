#!/usr/bin/env python
from __future__ import print_function
from rucio.client.client import Client
from rucio.api.did import add_did
import uuid
import json
import random
import datetime


client = Client(account="wma_prod")


def make_block(mock_request, did_metadata, rse, n_files):
    # Open a new block
    blockname = mock_request["outputdataset"] + "#" + str(uuid.uuid1())
    # client.add_dataset(scope="cms", name=blockname, meta=did_metadata)
    add_did(scope="cms", name=blockname, type="DATASET", issuer="wma_prod", account="wma_prod", meta=did_metadata)

    # Here we place a rule with a lifetime so that the block will be deleted.
    # The file source replicas will not go away until a rule is placed on them
    # and subsequently expired: the transition lock count 1 -> 0 sets a "tombstone"
    # which marks the replica as candidate for deletion.
    # The container rule will still collect the output before these files are removed
    client.add_replication_rule(
        dids=[{"scope": "cms", "name": blockname}],
        copies=1,
        rse_expression=rse,
        comment="Production block protection",
        meta=json.dumps(
            {
                # Here it may be useful to save some metadata that ties the block to the workflow
                "workflow": "workflow_name",
                "agent": "agent_name",
            }
        ),
        # Activity is probably optional as the rule should not cause any transfer requests
        activity="Production Output",
        # The lifetime is somewhat arbitrary, as even if the rule expires, any replicas that
        # are needed for transfer (to satsify the container rule) will not be removed until
        # the transfers complete, so there is no race condition.
        # *update* since TaskChains may force these blocks to stay at a site for indefinite period,
        # we let WMAgent hold onto the rule and release it when they deem appropriate
        # lifetime=datetime.timedelta(days=7).total_seconds(),
    )

    # For the container rule to be updated, we have to attach the block to the container
    client.add_datasets_to_container(
        scope="cms",
        name=mock_request["outputdataset"],
        dsns=[{"scope": "cms", "name": blockname}],
    )

    # Inject some file DIDs and source replicas into an RSE
    lfns = []
    for _ in range(n_files):
        lfn = "/store/mc/{campaign}/{pd}/AODSIM/{ps}/{uuid}.root".format(
            campaign=mock_request["campaign"],
            pd=mock_request["primarydataset"],
            ps=mock_request["processingstring"],
            uuid=uuid.uuid1(),
        )
        # This creates a DID and adds replica in one call
        client.add_replica(
            rse=rse,
            scope="cms",
            name=lfn,
            bytes=int(random.gauss(3e9, 2e8)),
            adler32="12341234",
        )
        lfns.append({"scope": "cms", "name": lfn})

    # Add the files to the block
    client.attach_dids(scope="cms", name=blockname, dids=lfns)

    # Here we close the block, saying no more files will be added
    client.close(scope="cms", name=blockname)


def mock_injection(mock_request, injection_sites):
    # Common metadata for container and block
    did_metadata = {
        'campaign': mock_request['campaign'],
        #'datatype': 'AODSIM', # character varying(50)
        'phys_group': mock_request['prepid'].split('-')[0], # character varying(25)
        'prod_step': 'idk', # character varying(50)
        'project': mock_request['project'], # character varying(50)
        'provenance': 'XX', # character varying(2)
        'stream_name': mock_request['primarydataset'], # character varying(70)
        'version': 'unknown', # character varying(50)
    }

    # Register output container despite having no files in it
    client.add_container(scope="cms", name=mock_request["outputdataset"], meta=did_metadata)

    if "NonCustodialSites" in mock_request:
        # Create a rule that initially matches no data but will update as files are injected
        # (this is done asynchronously by rucio-judge-evaluator)
        client.add_replication_rule(
            dids=[{"scope": "cms", "name": mock_request["outputdataset"]}],
            grouping="ALL",
            copies=mock_request["NonCustodialCopies"],
            rse_expression=mock_request["NonCustodialSites"],
            activity="Production Output",
            comment="something useful",
            meta=json.dumps(
                {"campaign": mock_request["campaign"], "prepid": mock_request["prepid"]}
            ),
        )

    # These should be able to run concurrently
    for site in injection_sites:
        make_block(mock_request, did_metadata, site, random.randint(1, 20))


mock_injection(
    {
        "campaign": "RunIIBlah",
        "outputdataset": "/ASample5/RunIIBlah-something/AODSIM",
        "primarydataset": "Asample5",
        "processingstring": "something",
        "prepid": "BPH-RunIIBlah-00001",
        "NonCustodialSites": "T1_US_FNAL_Mock",  # treating this as an RSE expression
        "NonCustodialCopies": 1,
        "project": "Production",  # synonymous with WMAgent "Team"
    },
    ["T2_US_Florida_Mock", "T2_CH_CERN_Mock"],
)

mock_injection(
    {
        "campaign": "RelValCMSSW",
        "outputdataset": "/MinBias/RelValCMSSW-something/AODSIM",
        "primarydataset": "MinBias",
        "processingstring": "something",
        "prepid": "PPD-RelVal-00001",
        "NonCustodialSites": "T1_US_FNAL_Mock",  # treating this as an RSE expression
        "NonCustodialCopies": 1,
        "project": "RelVal",  # synonymous with WMAgent "Team"
    },
    ["T1_US_FNAL_Mock", "T2_CH_CERN_Mock"],
)

mock_injection(
    {
        "campaign": "RelValCMSSW",
        "outputdataset": "/MinBias/RelValCMSSW-something/NANOAODSIM",
        "primarydataset": "MinBias",
        "processingstring": "something",
        "prepid": "PPD-RelVal-00001",
        "project": "RelVal",
    },
    ["T1_US_FNAL_Mock", "T2_CH_CERN_Mock"],
)

mock_injection(
    {
        "campaign": "01Apr2020",
        "outputdataset": "/EGamma/01Apr2020-something/NANOAOD",
        "primarydataset": "EGamma",
        "processingstring": "something",
        "prepid": "PPD-01Apr2020-00001",
        "project": "Production",
    },
    ["T2_US_Wisconsin_Mock", "T2_CH_CERN_Mock"],
)
