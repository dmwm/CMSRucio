'''
Tape Collocation algorithm for placement of CMS data on tape
'''

from rucio.core.did import list_parent_dids, get_did
from rucio.db.sqla.constants import DIDType

from rucio.transfertool.fts3_plugins import FTS3TapeMetadataPlugin


class CMSTapeCollocation(FTS3TapeMetadataPlugin): 
    def __init__(self, policy_algorithm) -> None:
        super().__init__(policy_algorithm)

        self.register(
            "tape_collocation", 
            func= lambda x: self._collocation(self.cms_collocation, x), 
            init_func=self.instance_init
        )
    
    def instance_init(self): 

        # Top level name spaces this plugin operates on 
        self.allowed_types = ['data', 'hidata', 'mc', 'himc', 'relval', 'hirelval']
        self.parking_name = "parking"
        self.raw_name = "raw"
        self.hiraw_name = "hiraw"

    def parent_container(self, scope, name): 
        # Custom logic for CMS
        # If dataset - look for the parent container
        # If file - look for the parent dataset and then the parent container
        is_file = get_did(scope=scope, name=name, session=self.session)['type'] == DIDType.FILE

        try: 
            if is_file:
                parent_dataset = [parent
                    for parent 
                    in list_parent_dids(scope=scope, name=name) 
                    if parent['scope'].external == 'cms'
                ][0]
                containers = [
                    parent['name']
                    for parent 
                    in list_parent_dids(scope=parent_dataset['scope'], name=parent_dataset['name']) 
                    if (parent['type'] == DIDType.CONTAINER) and (parent['scope'].external == 'cms')
                ]
            else: 
                containers = [
                    parent['name']
                    for parent 
                    in list_parent_dids(scope=scope, name=name) 
                    if (parent['type']==DIDType.CONTAINER) and (parent['scope'].external == 'cms')
                ]
            return containers[0]

        except IndexError: 
            pass

    def _is_raw(self, name): 
        # Raw always contains "RAW" in the name
        return any(i=="RAW" for i in name.split('/'))

    def _is_parking(self, name):
        # Parking is denoted by having ParkingXXXX in the lfn
        try: 
            return any(n.startswith("Parking") for n in name.split('/'))
        except IndexError:
            False

    def data_type(self, name): 
        data_type = name.removeprefix('/store/').split('/')[0] # First index that isn't `store`
        
        # Custom logic: Use parking or raw over "data", use hiraw if heavy ion and raw 
        if data_type not in self.allowed_types: 
            data_type = "n/a"
        elif self._is_parking(name): 
            data_type = self.parking_name
        elif self._is_raw(name):
            if data_type.startswith("hi"): 
                data_type = self.hiraw_name
            else: 
                data_type = self.raw_name
        
        return data_type

    def data_tier(self, name): 
        try: 
            return name.removeprefix('/store/').split('/')[3]
        except IndexError: 
            pass  # Can't get the tier

    def era(self, name): 
        try: 
            return name.removeprefix('/store/').split('/')[1]
        except IndexError: 
            pass  # Can't get the era


    def cms_collocation(self, **hints):
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
        data_type = self.data_type(lfn)
        
        collocation = {
            "0": data_type,
        }

        if data_type != "n/a":
            tier = self.data_tier(lfn)
            era = self.era(lfn)
            parent = self.parent_container(hints['scope'], hints['name'])

            if tier is not None: 
                collocation['1'] = tier 
            if era is not None: 
                collocation['2'] = era
            if parent is not None: 
                collocation['3'] = parent
        return collocation
    
CMSTapeCollocation(policy_algorithm="def") # Registering the plugin