"""
PFN to scope:name translation algorithm for CMS Rucio policy
"""
from collections.abc import Mapping

def cms_pfn2scope_name(parsed_pfn: Mapping[str, str]) -> tuple[str, str]:
    """
    TODO: Write a description
    """
    # Ignore user scopes for now
    scope = "cms"
    name = parsed_pfn['path'] + parsed_pfn['name']
    print("Inside cms function")
    print(f"name: {name}")
    return name, scope