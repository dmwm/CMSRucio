# CMSPolicyPackage
#
# Eric Vaandering <ewv@fnal.gov>, 2022

from CMSRucioPolicy.algorithms import lfn2pfn, auto_approve, pfn2scope_name

SUPPORTED_VERSION = ["32", "33", "34"]


def get_algorithms():
    """
    Get the algorithms for the policy package
    """
    return {
        'lfn2pfn': {
            'cmstfc': lfn2pfn.cmstfc,
        },
        'auto_approve': {
            'global': auto_approve.global_approval,
        },
        'pfn2scope_name': {
            'cms': pfn2scope_name.cms_pfn2scope_name,
        }
    }
