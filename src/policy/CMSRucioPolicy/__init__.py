# CMSPolicyPackage
#
# Eric Vaandering <ewv@fnal.gov>, 2022

from CMSRucioPolicy.algorithms import lfn2pfn, auto_approve, pfn2lfn

SUPPORTED_VERSION = ["32", "33", "34", "35"]


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
        'pfn2lfn': {
            'cms_pfn2lfn': pfn2lfn.cms_pfn2lfn,
        }
    }
