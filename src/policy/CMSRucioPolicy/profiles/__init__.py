"""rse-decommissioner profiles for the CMS policy package.

Registered under the 'rse_decommisioner_profile_plugin' algorithm type
(rucio/rucio#8581; the spelling 'decommisioner' is Rucio's).

This module must stay cheap and free of daemon imports: get_algorithms() runs in
every Rucio process that loads a policy algorithm (servers, probes, all daemons).
The real profile in .cms imports daemon code, so it's imported on first call.
That also avoids a circular import, because the daemon's profiles package is what
loads the policy package in the first place.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from rucio.daemons.rsedecommissioner.profiles.types import DecommissioningProfile

ALGORITHM_TYPE = 'rse_decommisioner_profile_plugin'


def cms_decommissioner(rse: dict[str, Any], config: dict[str, Any]) -> 'DecommissioningProfile':
    from CMSRucioPolicy.profiles.cms import cms_decommissioner as _cms_decommissioner
    return _cms_decommissioner(rse, config)


def get_profiles() -> dict[str, 'Callable[[dict[str, Any], dict[str, Any]], DecommissioningProfile]']:
    """Return the decommissioning profiles, or {} if this Rucio can't load them.

    Until 8581 is in a Rucio release, only patched pods accept the algorithm type.
    Rucio raises InvalidPolicyPackageAlgorithmType for an unknown type, which would
    break every unpatched pod using this image.
    """
    from rucio.common.constants import POLICY_ALGORITHM_TYPES

    if ALGORITHM_TYPE not in POLICY_ALGORITHM_TYPES:
        return {}
    return {'cms_decommissioner': cms_decommissioner}
