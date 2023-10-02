"""
Auto approve algorithm for CMS Rucio policy
"""
import logging
from configparser import NoOptionError, NoSectionError
from datetime import datetime

from rucio.common.config import config_get
from sqlalchemy.sql import func


def global_approval(rule, did, session) -> bool:
    """
    Auto approves rules by users that satisfy the following conditions:
    - Activity is User AutoApprove
        A use of separate activity will help us utilise the current monitoring
        without needing extra effort
    - User is under the global usage threshold
    - User is not banned from creating rules
    - Rule lifetime is less than a threshold
    - Rule is not locked (Locked rules do not get deleted until they are unlocked)


    :param did: The DID being approved
    :type did DataIdentifier

    :param rule: The rule being approved
    :type rule: ReplicationRule

    :param session: The database session in use

    :returns: True if the rule should be auto approved, False otherwise
    """

    from rucio.core.account import has_account_attribute
    from rucio.core.did import list_files
    from rucio.core.rse_expression_parser import parse_expression
    from rucio.core.rule import list_rules
    from rucio.db.sqla.models import ReplicaLock, ReplicationRule

    def _get_rule_size(rules):
        rule_size = 0
        for rule in rules:
            scope = rule['scope']
            name = rule['name']
            rule_files = list_files(scope, name, session=session)
            rule_size += sum([file['bytes'] for file in rule_files])
        return rule_size

    account = rule['account']
    try:
        global_usage_all_accounts = float(config_get(
            'rules', 'global_usage_all_accounts', raise_exception=True, default=1e16))
    except (NoOptionError, NoSectionError, RuntimeError):
        global_usage_all_accounts = 1e16

    try:
        global_usage_per_account = float(config_get(
            'rules', 'global_usage_per_account', raise_exception=True, default=1e15))
    except (NoOptionError, NoSectionError, RuntimeError):
        global_usage_per_account = 1e15

    try:
        rule_lifetime_threshold = int(config_get('rules', 'rule_lifetime_threshold',
                                      raise_exception=True, default=2592000))
    except (NoOptionError, NoSectionError, RuntimeError):
        rule_lifetime_threshold = 2592000

    try:
        single_rse_rule_size_threshold = float(config_get(
            'rules', 'single_rse_rule_size_threshold', raise_exception=True, default=50e12))
    except (NoOptionError, NoSectionError, RuntimeError):
        single_rse_rule_size_threshold = 50e12

    auto_approve_activity = 'User AutoApprove'

    # Check if the account is banned
    if has_account_attribute(account, 'rule_banned', session=session):
        return False

    # Check activity is User AutoApprove
    if rule['activity'] != auto_approve_activity:
        return False

    # Check if the rule is locked
    if rule['locked']:
        return False

    if rule['expires_at'] is None:
        return False

    lifetime = (rule['expires_at'] - datetime.utcnow()).total_seconds()
    if lifetime > rule_lifetime_threshold:
        return False

    size_of_rule = sum([file['bytes'] for file in list_files(did['scope'], did['name'], session=session)])

    # Limit single RSE rules to 50 TB
    # This does not mean that the total locks size at a RSE will be limited to 50 TB
    # as other rules that are spread over multiple RSEs may claim the same space
    # This is just a simple check to avoid a single RSE rules from being too large
    rse_expression = rule['rse_expression']
    rses = parse_expression(rse_expression, filter_={'availability_write': True}, session=session)
    if len(rses) == 1:
        this_rse_autoapprove_rules = list_rules(
            filters={'account': account, 'activity': auto_approve_activity, 'rse_expression': rse_expression},
            session=session)
        this_rse_autoapprove_usage = _get_rule_size(this_rse_autoapprove_rules)
        if this_rse_autoapprove_usage + size_of_rule > single_rse_rule_size_threshold:
            logging.warning('Single RSE usage exceeded for auto approve rules for account %s and RSE %s',
                            account, rse_expression)
            return False

    # Check global usage of the account under this activity
    all_auto_approve_rules_by_account = list_rules(
        filters={'account': account, 'activity': auto_approve_activity}, session=session)
    global_auto_approve_usage_by_account = _get_rule_size(all_auto_approve_rules_by_account)
    if global_auto_approve_usage_by_account + size_of_rule > global_usage_per_account:
        logging.warning('Global usage exceeded for auto approve rules for account %s', account)
        return False

    # Check global usage under the AutoApprove category by all accounts
    query = session.query(
        func.sum(ReplicaLock.bytes)).join(
        ReplicationRule, ReplicaLock.rule_id == ReplicationRule.id).filter(
        ReplicationRule.activity == 'User AutoApprove')
    current_auto_approve_usage = query.scalar()
    if current_auto_approve_usage is None:
        current_auto_approve_usage = 0
    if current_auto_approve_usage + size_of_rule > global_usage_all_accounts:
        logging.warning('Global usage exceeded for auto approve rules')
        return False
    return True
