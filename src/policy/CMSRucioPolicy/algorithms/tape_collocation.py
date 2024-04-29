'''

'''
from configparser import DuplicateSectionError

from rucio.transfertool.fts3_plugins import FTS3TapeMetadataPlugin
from rucio.common.config import config_set, config_add_section, config_get_list
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
        # Add the plugin to the config
        try: 
            config_add_section("transfers")
        except DuplicateSectionError: 
            pass 

        active_plugins = config_get_list("transfers", "fts3tape_metadata_plugins", raise_exception=False, default=[])
        if "tape_collocation" not in active_plugins: 
            active_plugins.append("tape_collocation")
            config_set("transfers", "fts3tape_metadata_plugins", active_plugins)
    
    def instance_init(self): 
        self.session = get_session()

        # Top level name spaces this plugin operates on 
        self.allowed_types = ['data', 'hidata', 'mc', 'himc', 'relval', 'hirelval']

    def parent_container(self, scope, name): 
        parent_container = [
            f"{parent['scope']}:{parent['name']}"
            for parent 
            in list_parent_dids(scope=scope, name=name, session=self.session) 
            if parent['type']=='CONTAINER'
        ][0]
        return parent_container

    def data_type(self, name): 
        data_type = name.lstrip('/store/').split('/')[0] # First index that isn't `store`
        if data_type not in self.allowed_types: 
            return "n/a"
        return data_type

    def data_tier(self, data_type, name): 
        if data_type in self.allowed_types: 
            return name.split('/')[4]

    def era(self, data_type, name): 
        if data_type in self.allowed_types: 
            return name.lstrip('/store/').split('/')[1]

    def cms_collocation(self, *hints):
        """
        https://github.com/dmwm/CMSRucio/issues/753
        https://github.com/dmwm/CMSRucio/issues/323

        Level 0
        Data/MC/HIData/HiMC (from /store/(data/mc/hi/data/himc) plus RAW and HIRAW from data_tier

        Level 1
        Data tier - either in the LFN or the end of the parent container

        Level 2
        Era (which I think for MC is the Campaign)

        Level 3 
        Parent Container (can either get this explicitly or get the parent dataset and lop off the hash mark and the hash following it)
        """

        lfn = hints['name']
        data_type = self.data_type(lfn)
        tier = self.data_tier(data_type, lfn)
        era = self.era(data_type, lfn)

        collocation = {
            "0": data_type,
            "3": self.parent_container(hints['scope'], hints['name'])
        }

        if tier is not None: 
            collocation['1'] = tier 
        if era is not None: 
            collocation['2'] = era

        return collocation
    
CMSTapeCollocation(algorithm="def") # Registering the plugin and adding it to the config 
