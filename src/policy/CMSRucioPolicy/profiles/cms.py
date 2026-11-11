# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""CMS decommissioning profile for the rse-decommissioner daemon.

Ported from the cms.py of rucio/rucio#7636 

This module imports daemon code, so it must only be imported lazily, from inside
the profile function (see ``CMSRucioPolicy.profiles.cms_decommissioner``). Never
import it from ``get_algorithms()`` at module level.

Workarounds for bugs in Rucio's core decommissioner code are marked ``WORKAROUND``.
Each can be removed once the matching upstream fix is in the Rucio release we run.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy.exc import NoResultFound

from rucio.common.exception import InvalidRSEExpression, RSENotFound, RucioException
from rucio.core.lock import get_replica_locks_for_rule_id
from rucio.core.rse import add_protocol, add_rse_attribute, get_rse_attribute
from rucio.core.rse_expression_parser import parse_expression
from rucio.daemons.rsedecommissioner.profiles import generic
from rucio.daemons.rsedecommissioner.profiles.types import DecommissioningProfile, HandlerOutcome
from rucio.db.sqla.constants import DatabaseOperationType
from rucio.db.sqla.session import db_session

if TYPE_CHECKING:
    from rucio.common.types import LoggerFunction

DECOMMISSION_ATTR = 'decommission'

# WORKAROUND (core bug: attr_to_config splits on every '='): a move destination that
# contains '=' (e.g. 'cms_type=real&tier=2') cannot be put in the 'decommission'
# attribute. Put it in this separate RSE attribute instead.
DESTINATION_ATTR = 'decommission_destination'

WM_ACCOUNTS = frozenset([
    'wma_prod', 'wmcore_output', 'wmcore_pileup', 'wmcore_transferor', 'wmccorma',
])

FUNCTIONAL_TEST_ACTIVITY = 'Functional Test'
CRAB_TAPERECALL_ACTIVITY = 'Analysis TapeRecall'

MOCK_DELETION_PROTOCOL = {
    'scheme': 'mock',
    'hostname': 'mock',
    'prefix': '/',
    'impl': 'rucio.rse.protocols.mock.Default',
    'domains': {
        'wan': {
            'read': 0,
            'write': 0,
            'delete': 1,
            'third_party_copy_read': 0,
            'third_party_copy_write': 0,
        },
    },
}


def cms_decommissioner(rse: dict[str, Any], config: dict[str, Any]) -> DecommissioningProfile:
    """Return the CMS decommissioning profile.

    Lists all rules locking replicas at the RSE and handles them in this order:

    - Locked rules: operator attention.
    - Rules already expiring or being moved: counted as processed.
    - 'Functional Test' rules: deleted.
    - CRAB 'Analysis TapeRecall' rules: operator attention (CRAB must be told first).
    - WM rules: operator attention. WM tracks rule IDs, and moving a rule changes its ID.
      Wait for them to expire.
    - Rules with a trivial RSE expression (the RSE name, or 'rse=<name>'): moved to the
      configured destination.
    - Rules with any other expression: moved to '(<expression>)\\<RSE>', if that still
      resolves to enough writable RSEs for the rule's copies.
    - Anything else: operator attention.

    The destination for trivial rules is read from the RSE attribute
    'decommission_destination' if set, otherwise from 'move_dest' or 'destination' in
    the 'decommission' attribute. Without one, trivial rules need operator attention.

    :param rse: RSE to decommission.
    :param config: Decommissioning configuration dictionary.
    :returns: A decommissioning profile.
    """
    mover = CMSRuleMover(_default_destination(rse, config))

    return DecommissioningProfile(
        rse=rse,
        initializer=_cms_initialize,
        discoverer=generic._generic_discover,
        handlers=[
            (_is_locked, generic._call_for_attention),
            (generic._is_being_deleted, generic._count_as_processed),
            (generic._has_child_rule_id, generic._count_as_processed),
            (_is_test_rule, generic._delete_rule),
            (_is_crab_taperecall_rule, generic._call_for_attention),
            (_is_wm_rule, generic._call_for_attention),
            (_has_replicas_on_rse, mover),
            # WORKAROUND (core bug: DecommissioningProfile.process() logs rule['rse'],
            # which rule dicts don't have, so a rule that matches no handler raises
            # KeyError and aborts the cycle for the RSE). Never fall through.
            (_always, generic._call_for_attention),
        ],
        finalizer=_cms_finalize,
    )


def _default_destination(rse: dict[str, Any], config: dict[str, Any]) -> Optional[str]:
    destination = None
    try:
        destination = get_rse_attribute(rse['id'], DESTINATION_ATTR, use_cache=False)
    except RSENotFound:
        pass
    if not destination:
        destination = config.get('move_dest') or config.get('destination')
    return str(destination).strip() if destination else None


# Initializer and finalizer

def _cms_initialize(
    rse: dict[str, Any],
    *,
    logger: "LoggerFunction" = logging.log,
) -> None:
    """Prepare the RSE for decommissioning.

    On top of the generic initializer (read=True, write=False, delete=True,
    greedyDeletion=True), sets update_from_json=False, loadtest=False,
    skip_site_availability_update=True and reaper=True, and adds a mock deletion
    protocol so the reaper can clear replicas whose storage is gone.

    Runs every cycle, so every step must be idempotent.
    """
    for key, value in (
        ('update_from_json', False),
        ('loadtest', False),
        ('skip_site_availability_update', True),
    ):
        logger(logging.INFO, '(%s) Setting %s=%s.', rse['rse'], key, value)
        add_rse_attribute(rse['id'], key, value)

    generic._generic_initialize(rse, logger=logger)

    logger(logging.INFO, '(%s) Setting reaper=True.', rse['rse'])
    add_rse_attribute(rse['id'], 'reaper', True)

    try:
        # add_protocol pops 'domains', so hand it a copy.
        add_protocol(rse['id'], {**MOCK_DELETION_PROTOCOL,
                                 'domains': {d: dict(ops) for d, ops in MOCK_DELETION_PROTOCOL['domains'].items()}})
        logger(logging.INFO, '(%s) Added mock deletion protocol.', rse['rse'])
    except RucioException as error:
        # Duplicate on every cycle after the first.
        logger(logging.DEBUG, '(%s) Mock deletion protocol not added: %s', rse['rse'], error)


def _cms_finalize(
    rse: dict[str, Any],
    *,
    logger: "LoggerFunction" = logging.log,
) -> bool:
    """Run the generic finalizer, and set status=done ourselves when it succeeds.

    WORKAROUND (core bug: config_to_attr calls config['profile'].value on a str, so the
    daemon's set_status() raises AttributeError). The daemon only calls set_status(DONE)
    when the finalizer returns True, so we write the status ourselves and return False.
    Once fixed upstream, replace this with generic._generic_finalize.
    """
    if not generic._generic_finalize(rse, logger=logger):
        return False

    attr = get_rse_attribute(rse['id'], DECOMMISSION_ATTR, use_cache=False) or ''
    items = [kv for kv in str(attr).split(',') if kv.strip() and not kv.strip().startswith('status=')]
    items.append('status=done')
    add_rse_attribute(rse['id'], DECOMMISSION_ATTR, ','.join(items))
    logger(logging.INFO, '(%s) Decommissioning complete; set status=done.', rse['rse'])
    return False


# Conditions

def _always(rule: dict[str, Any], rse: dict[str, Any], *, logger: "LoggerFunction" = logging.log) -> bool:
    return True


def _is_locked(rule: dict[str, Any], rse: dict[str, Any], *, logger: "LoggerFunction" = logging.log) -> bool:
    """Check if the rule is locked.

    WORKAROUND (core bug: generic._is_locked logs "is locked" for every rule).
    """
    if rule['locked']:
        logger(logging.INFO, '(%s) Rule %s for %s:%s is locked',
               rse['rse'], rule['id'], rule['scope'], rule['name'])
        return True
    return False


def _is_test_rule(rule: dict[str, Any], rse: dict[str, Any], *, logger: "LoggerFunction" = logging.log) -> bool:
    """Check if the rule is a functional test rule.

    Test rules that use this RSE only as a source are not discovered and not handled.
    """
    return rule['activity'] == FUNCTIONAL_TEST_ACTIVITY


def _is_crab_taperecall_rule(rule: dict[str, Any], rse: dict[str, Any], *, logger: "LoggerFunction" = logging.log) -> bool:
    """Check if the rule is a CRAB tape recall (created with the user's account).

    'Analysis Input' rules (crab_input account) are handled like any other rule.
    """
    return rule['activity'] == CRAB_TAPERECALL_ACTIVITY


def _is_wm_rule(rule: dict[str, Any], rse: dict[str, Any], *, logger: "LoggerFunction" = logging.log) -> bool:
    """Check if the rule was created by a WM account."""
    return str(rule['account']) in WM_ACCOUNTS


def _has_replicas_on_rse(rule: dict[str, Any], rse: dict[str, Any], *, logger: "LoggerFunction" = logging.log) -> bool:
    """Check if any replica locked by the rule is on the RSE."""
    try:
        locks = get_replica_locks_for_rule_id(rule['id'])
    except NoResultFound:
        # No replica is locked to begin with -> treat as having the last replica on this RSE
        return True
    return rse['id'] in {lock['rse_id'] for lock in locks}


def _has_trivial_rse_expression(rule: dict[str, Any], rse: dict[str, Any]) -> bool:
    return rule['rse_expression'] in (rse['rse'], f"rse={rse['rse']}")


# Action

class CMSRuleMover(generic.RuleMover):
    """Move a rule to a destination chosen per rule.

    Trivial expressions go to the default destination. Other expressions go to the
    same expression minus the decommissioned RSE.
    """

    def __init__(self, default_destination: Optional[str]) -> None:
        super().__init__(default_destination or '')
        self.default_destination = default_destination

    def __call__(
        self,
        rule: dict[str, Any],
        rse: dict[str, Any],
        *,
        logger: "LoggerFunction" = logging.log
    ) -> HandlerOutcome:
        destination = self._destination_for(rule, rse, logger=logger)
        if destination is None:
            return HandlerOutcome.NEED_ATTENTION

        # The parent class reads self.destination. Set it per rule; the 7636 version
        # only set it for non-trivial rules, so trivial rules that followed one went to
        # the previous rule's expression.
        self.destination = destination
        try:
            return super().__call__(rule, rse, logger=logger)
        except RucioException as error:
            # WORKAROUND (core bug: RuleMover only catches RuleNotFound and
            # RuleReplaceFailed; anything else from move_rule, e.g. RSEWriteBlocked or
            # InsufficientTargetRSEs, aborts the cycle for the whole RSE).
            logger(logging.ERROR, '(%s) Failed to move rule %s to %s: %s',
                   rse['rse'], rule['id'], destination, error)
            return HandlerOutcome.NEED_ATTENTION

    def _destination_for(
        self,
        rule: dict[str, Any],
        rse: dict[str, Any],
        *,
        logger: "LoggerFunction"
    ) -> Optional[str]:
        if _has_trivial_rse_expression(rule, rse):
            if not self.default_destination:
                logger(logging.WARNING,
                       '(%s) No destination configured for rule %s with trivial expression "%s"',
                       rse['rse'], rule['id'], rule['rse_expression'])
                return None
            return self.default_destination

        destination = f"({rule['rse_expression']})\\{rse['rse']}"
        try:
            # parse_expression needs an explicit session from Rucio 41 on.
            with db_session(DatabaseOperationType.READ) as session:
                resolved = parse_expression(destination, filter_={'availability_write': True}, session=session)
        except (InvalidRSEExpression, RSENotFound) as error:
            logger(logging.WARNING, '(%s) Rule %s: "%s" resolves to no writable RSE: %s',
                   rse['rse'], rule['id'], destination, error)
            return None

        if len(resolved) < rule['copies']:
            logger(logging.WARNING,
                   '(%s) Rule %s: "%s" resolves to %d writable RSE(s), but %d copies are required',
                   rse['rse'], rule['id'], destination, len(resolved), rule['copies'])
            return None
        return destination
