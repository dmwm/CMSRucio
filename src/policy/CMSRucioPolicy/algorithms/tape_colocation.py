'''
Tape colocation algorithm for placement of CMS data on tape
'''

import json
import logging
from typing import Any, Optional, Union

from rucio.common.exception import DataIdentifierNotFound
from rucio.common.types import InternalScope
from rucio.core.did import get_did, list_content, list_parent_dids
from rucio.db.sqla.constants import DIDType
from rucio.transfertool.fts3_plugins import FTS3TapeMetadataPlugin

logger = logging.getLogger(__name__)

class CMSTapeColocation(FTS3TapeMetadataPlugin):
    policy_algorithm = "tape_metadata"

    # Logic for tape colocation
    allowed_types = ['data', 'hidata', 'mc', 'himc', 'relval', 'hirelval']
    parking_name = "parking"
    raw_name = "raw"
    hiraw_name = "hiraw"
    
    # Schema version as of June 26, 2025
    schema_version = 1

    def __init__(self, policy: Union[str, None] = None) -> None:

        super().__init__(self.policy_algorithm)

        logger.info("Initialized plugin %s", self.policy_algorithm)

    @classmethod
    def _module_init_(cls) -> None:
        logger.info("Registered plugin %s", cls.policy_algorithm)
        cls.register(
            cls.policy_algorithm, 
            func=lambda x: cls.tape_metadata(x)
        )

    @staticmethod
    def _encode(name: Any) -> Optional[str]: 
        try: 
            json.dumps(name)
            return name
        except json.JSONDecodeError:
            return None

    @staticmethod
    def parent_container(scope, name): 
        # Custom logic for CMS
        # If dataset - look for the parent container
        # If file - look for the parent dataset and then the parent container
        if not isinstance(scope, InternalScope): 
            scope = InternalScope(scope)
        try:
            is_file = get_did(scope=scope, name=name)['type'] == DIDType.FILE
        except DataIdentifierNotFound: 
            logger.warning("DID not found for %s:%s", scope, name)
            return None
        try:
            if is_file:
                parent_dataset = [parent
                    for parent
                    in list_parent_dids(scope=scope, name=name)
                ][0]
                containers = [
                    parent['name']
                    for parent
                    in list_parent_dids(scope=scope, name=parent_dataset['name'])
                    if parent['type'] == DIDType.CONTAINER
                ]
            else: 
                containers = [
                    parent['name']
                    for parent
                    in list_parent_dids(scope=scope, name=name)
                    if parent['type']==DIDType.CONTAINER
                ]
            container = CMSTapeColocation._encode(containers[0])
            if container is None: 
                logger.debug("Could not encode container for %s", name)
            return container

        except (IndexError, DataIdentifierNotFound): 
            logger.debug("No parent container found for %s:%s", scope, name)

    @staticmethod
    def _is_raw(name):
        # Raw always contains "RAW" in the name
        return any(i=="RAW" for i in name.split('/'))

    @staticmethod
    def _is_parking(name):
        # Parking is denoted by having ParkingXXXX in the lfn
        try:
            return any(n.startswith("Parking") for n in name.split('/'))
        except IndexError:
            return False

    @staticmethod
    def data_type(name):
        data_type = name.removeprefix('/store/').split('/')[0] # First index that isn't `store`
        # Custom logic: Use parking or raw over "data", use hiraw if heavy ion and raw 
        if data_type not in CMSTapeColocation.allowed_types:
            data_type = "n/a"
        elif CMSTapeColocation._is_parking(name):
            data_type = CMSTapeColocation.parking_name
        elif CMSTapeColocation._is_raw(name):
            if data_type.startswith("hi"):
                data_type = CMSTapeColocation.hiraw_name
            else:
                data_type = CMSTapeColocation.raw_name
        
        return data_type

    @staticmethod
    def data_tier(name):
        try:
            tier = name.removeprefix('/store/').split('/')[3]
            if CMSTapeColocation._encode(tier) is None:
                logger.debug("Could not encode data tier for %s", name)
            return tier
        except IndexError:
            logger.debug("Could not determine data tier for %s", name)

    @staticmethod
    def era(name):
        try:
            era = name.removeprefix('/store/').split('/')[1]
            if CMSTapeColocation._encode(era) is None:
                logger.debug("Could not encode era for %s", name)
            return era
        except IndexError:
            logger.debug("Could not determine era for %s", name)

    @staticmethod
    def _get_container_stats(scope, name):
        size = 0
        length = 0
        if not isinstance(scope, InternalScope): 
            scope = InternalScope(scope)
        try:
            contents = list_content(scope, name)
            for item in contents:
                if item['type'] == DIDType.FILE:
                    size += item.get('bytes', 0)
                    length += 1
                elif item['type'] in [DIDType.DATASET, DIDType.CONTAINER]:
                    # Recursively get size of nested datasets/containers
                    sub_length, sub_size = CMSTapeColocation._get_container_stats(scope, item['name'])
                    length += sub_length
                    size += sub_size
        except DataIdentifierNotFound:
            logger.warning("DID not found for container %s:%s", scope, name)

        return length, size

    @classmethod
    def tape_metadata(cls, hints):
        """
        https://github.com/dmwm/CMSRucio/issues/753
        https://github.com/dmwm/CMSRucio/issues/323
        https://its.cern.ch/jira/browse/CMSDM-315
        
        Tape Colocation: 
            Level 0
            Data/MC/HIData/HiMC (from /store/(data/mc/hi/data/himc) plus RAW and HIRAW, and Parking.
            
            Level 1
            Data tier - either in the LFN or the end of the parent container

            Level 2
            Era (which for MC is the Campaign)

            Level 3
            Parent Container (parent container of dataset if file)
            
            
            Examples:
                * Parking data:
                    /store/data/Run2024C/ParkingVBF5/RAW/v1/000/380/115/00000/b4c0513e-f732-42b1-858d-572c86ce4b97.root
                    -->  {'0': 'parking', '1': 'RAW', '2': 'Run2024C', '3': '/ParkingVBF5/Run2024C-v1/RAW'}
                * Raw:
                    /store/hidata/HIRun2024B/HIPhysicsRawPrime5/RAW/v1/000/388/624/00000/fa0795b5-633b-461c-bc21-02d40a118dd2.root
                    -->  {'0': 'hiraw', '1': 'RAW', '2': 'HIRun2024B', '3': '/HIPhysicsRawPrime5/HIRun2024B-v1/RAW'}

        File Metadata: 
            As given by Rucio hints:
            * Size
            * MD5
            * Adler32

        Additional Hints:
            * Activity (default to "default" if not given)
            * Level 3 length and size (if parent container exists)
                - Length - total number of files in the parent container, including nested datasets/containers
                - Size - total size of all files in the parent container, including nested datasets/containers
                        
        """

        lfn = hints['name']
        data_type = cls.data_type(lfn)
        colocation = {
            "0": data_type,
        }

        if data_type != "n/a":
            tier = cls.data_tier(lfn)
            era = cls.era(lfn)
            parent = cls.parent_container(hints['scope'], hints['name'])
            if tier is not None:
                colocation['1'] = tier
            if era is not None:
                colocation['2'] = era
            if parent is not None:
                colocation['3'] = parent
        else:
            parent = None
            logger.debug("Could not determine data type for %s", lfn)

        logger.debug("Setting colocation hints %s", colocation)

        additional_hints = {
            "activity": hints.get("activity", "default"),
        }
        if parent is not None:
            length, size = CMSTapeColocation._get_container_stats(hints['scope'], parent)
            additional_hints['3'] = {
                "length": length, #The number of files in the parent container
                "size": size,   #The total size of the parent container
            }
        logger.debug("Setting additional hints %s", additional_hints)

        metadata = {
            "size": hints['metadata'].get('filesize', 0),  # File size
            "md5": hints['metadata'].get("md5"),  # MD5 checksum
            "adler32": hints['metadata'].get("adler32"), # Adler32 checksum
        }
        logger.debug("File metadata: %s", metadata)

        return {
            "collocation_hints": colocation,
            "additional_hints": additional_hints,
            "file_metadata": metadata,
            "schema_version": cls.schema_version
        }