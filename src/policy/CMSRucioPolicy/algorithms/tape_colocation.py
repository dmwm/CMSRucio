'''
Tape colocation algorithm for placement of CMS data on tape
'''

from typing import Any, Optional, Union
from rucio.common.exception import DataIdentifierNotFound
from rucio.transfertool.fts3_plugins import FTS3TapeMetadataPlugin
from rucio.core.did import list_parent_dids, get_did
from rucio.db.sqla.constants import DIDType
from rucio.common.types import InternalScope
import logging 
import json

logger = logging.getLogger(__name__)

class CMSTapeColocation(FTS3TapeMetadataPlugin): 
    policy_algorithm = "tape_colocation"

    allowed_types = ['data', 'hidata', 'mc', 'himc', 'relval', 'hirelval']
    parking_name = "parking"
    raw_name = "raw"
    hiraw_name = "hiraw"

    def __init__(self, policy: Union[str, None] = None) -> None:

        super().__init__(self.policy_algorithm)

        logger.info("Initialized plugin %s", self.policy_algorithm)

    @classmethod
    def _module_init_(cls) -> None:
        logger.info("Registered plugin %s", cls.policy_algorithm)
        cls.register(
            cls.policy_algorithm, 
            func=lambda x: cls.cms_colocation(x)
        ) 

    @staticmethod
    def _encode(name: Any) -> Optional[str]: 
        try: 
            json.dumps(name)
            return name
        except json.JSONDecodeError:
            return None

    @staticmethod
    def parent_container(name): 
        # Custom logic for CMS
        # If dataset - look for the parent container
        # If file - look for the parent dataset and then the parent container
        scope = InternalScope("cms")
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

    @classmethod
    def cms_colocation(cls, hints):
        """
        https://github.com/dmwm/CMSRucio/issues/753
        https://github.com/dmwm/CMSRucio/issues/323

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

        """

        lfn = hints['name']
        data_type = cls.data_type(lfn)
        
        colocation = {
            "0": data_type,
        }

        if data_type != "n/a":
            tier = cls.data_tier(lfn)
            era = cls.era(lfn)
            parent = cls.parent_container(hints['name'])
            if tier is not None: 
                colocation['1'] = tier
            if era is not None: 
                colocation['2'] = era
            if parent is not None: 
                colocation['3'] = parent
        else: 
            logger.debug("Could not determine data type for %s", lfn)

        # TODO Speak with FTS3 Team about these headers
        logger.debug("Setting colocation hints %s", colocation)
        return {"collocation_hints": colocation}
