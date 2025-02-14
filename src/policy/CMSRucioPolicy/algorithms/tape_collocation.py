'''

'''
from rucio.transfertool.fts3_plugins import FTS3TapeMetadataPlugin
from rucio.core.did import list_parent_dids
from rucio.db.sqla.session import get_session 

class CMSTapeCollocation(FTS3TapeMetadataPlugin): 
    def __init__(self, policy_algorithm) -> None:
        super().__init__(policy_algorithm)

        self.register(
            "tape_collocation", 
            func= lambda x: self._collocation(self.cms_collocation, x), 
            init_func=self.instance_init
        )
    
    def instance_init(self): 
        self.session = get_session()

        # Top level name spaces this plugin operates on 
        self.allowed_types = ['data', 'hidata', 'mc', 'himc', 'relval', 'hirelval']

    def parent_container(self, scope, name): 
        containers = [
            f"{parent['scope']}:{parent['name']}"
            for parent 
            in list_parent_dids(scope=scope, name=name, session=self.session) 
            if parent['type']=='CONTAINER'
        ]
        try: 
            return containers[0]
        except IndexError: 
            pass

    def data_type(self, name): 
        data_type = name.lstrip('/store/').split('/')[0] # First index that isn't `store`
        if data_type not in self.allowed_types: 
            return "n/a"
        return data_type

    def data_tier(self, data_type, name): 
        try: 
            if data_type in self.allowed_types: 
                return name.split('/')[4]
        except IndexError: 
            pass  # Can't get the tier

    def era(self, data_type, name): 
        try: 
            if data_type in self.allowed_types: 
                return name.lstrip('/store/').split('/')[1]
        except IndexError: 
            pass  # Can't get the era


    def cms_collocation(self, *hints):
        """
        https://github.com/dmwm/CMSRucio/issues/753
        https://github.com/dmwm/CMSRucio/issues/323

        Level 0
        Data/MC/HIData/HiMC (from /store/(data/mc/hi/data/himc) plus RAW and HIRAW from data_tier

        Level 1
        Data tier - either in the LFN or the end of the parent container

        Level 2
        Era (which for MC is the Campaign)

        Level 3 
        Parent Container (can either get this explicitly or get the parent dataset and lop off the hash mark and the hash following it)
        """

        # Notes - Add it to the cms-rucio-common yaml in dmwcore/rucio-flux
        # Levels common < daemons < prod-deamons 
        # put in PR 

        lfn = hints['name']
        data_type = self.data_type(lfn)
        tier = self.data_tier(data_type, lfn)
        era = self.era(data_type, lfn)
        parent = self.parent_container(hints['scope'], hints['name'])

        collocation = {
            "0": data_type,
        }

        if tier is not None: 
            collocation['1'] = tier 
        if era is not None: 
            collocation['2'] = era
        if parent is not None: 
            collocation['3'] = parent
        return collocation
    
CMSTapeCollocation(algorithm="def") # Registering the plugin
