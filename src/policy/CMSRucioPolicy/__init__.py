# CMSPolicyPackage
#
# Eric Vaandering <ewv@fnal.gov>, 2022

from CMSRucioPolicy.algorithms import lfn2pfn, auto_approve, pfn2lfn, tape_collocation

SUPPORTED_VERSION = [">= 36.0"]


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
        'fts3tape_metadata_plugins': {
            'tape_collocation': tape_collocation
        },
        'pfn2lfn': {
            'cms_pfn2lfn': pfn2lfn.cms_pfn2lfn,
        }
    }
