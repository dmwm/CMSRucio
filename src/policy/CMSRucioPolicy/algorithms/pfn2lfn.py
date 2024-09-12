"""
PFN to scope:name translation algorithm for CMS Rucio policy
"""
from collections.abc import Mapping

def cms_pfn2lfn(parsed_pfn: Mapping[str, str]) -> tuple[str, str]:
    """
    This function converts a PFN into scope:name pair.
    It ignores user scopes and sets all scopes to cms.
    """
    # Ignore user scopes for now
    scope = "cms"
    name = parsed_pfn['path'] + parsed_pfn['name']
    return name, scope