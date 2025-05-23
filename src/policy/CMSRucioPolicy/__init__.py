# CMSPolicyPackage
#
# Eric Vaandering <ewv@fnal.gov>, 2022

SUPPORTED_VERSION = [">= 36.0"]


def get_algorithms():
    """
    Get the algorithms for the policy package
    """
    from CMSRucioPolicy.algorithms import lfn2pfn, auto_approve, pfn2lfn
    from CMSRucioPolicy.algorithms.tape_colocation import CMSTapeColocation
    CMSTapeColocation._module_init_()
    
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

