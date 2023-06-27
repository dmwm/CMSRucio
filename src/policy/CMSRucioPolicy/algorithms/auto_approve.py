"""
Auto approve algorithm for CMS Rucio policy
"""
from configparser import NoOptionError, NoSectionError


def global_approval(did, rule_attributes, session):
    """
    Auto approves rules by users that satisfy the following conditions:
    - Activity is User AutoApprove
        A use of separate activity will help us utilise the current monitoring 
        without needing extra effort
    - User is under the global usage threshold
    - User is not banned from creating rules
    - Rule lifetime is less than a month
    - Rule is not locked (Locked rules do not get deleted until they are unlocked)
    - RSE expression belongs to the set of expressions that are allowed to be used by the user
        This helps us taint RSEs in various sets without needing to change code
        for e.g. one could taint all primary sites with data_brokering_rse_set=1


    :param did: The DID being approved
    :type did dict: {
        'scope': 'cms|user.username|group.groupname',
        'name': 'lfn',
        'account': 'account which registered the DID',
        'did_type': 'FILE, DATASET, CONTAINER',
        'is_open': 'True | False',
        'bytes': 'size in bytes (NULL for a CONTAINER)',
        }

    :param rule_attributes: The attributes of the rule being approved  
    :type rule_attributes dict: {
        'account': account,
        'lifetime': lifetime,
        'rse_expression': rse_expression,
        'copies': copies,
        'weight': weight,
        'grouping': grouping,
        'locked': locked,
        'comment': comment,
        'meta': meta,
        'ignore_availability': ignore_availability,
        }
    :param session: The database session in use

    :returns: True if the rule should be auto approved, False otherwise
    """
    from rucio.common.config import config_get
    from rucio.core.account import has_account_attribute
    from rucio.core.rule import list_rules
    from rucio.core.did import list_files
    from rucio.core.rse_expression_parser import parse_expression

    def _get_rules_size(rules):
        rule_size = 0
        for rule in rules:
            scope = rule['scope']
            name = rule['name']
            rule_files = list_files(scope, name, session=session)
            rule_size += sum([file['bytes'] for file in rule_files])
        return rule_size

    account = rule_attributes['account']


    try:
        global_usage_threshold = float(config_get('rules', 'global_usage_threshold', raise_exception=True, default=1e15))
    except (NoOptionError, NoSectionError, RuntimeError):
        global_usage_threshold = 1e15

    try:
        rule_lifetime_threshold = int(config_get('rules', 'rule_lifetime_threshold', raise_exception=True, default=2592000))
    except (NoOptionError, NoSectionError, RuntimeError):
        rule_lifetime_threshold = 2592000

    try:
        single_rse_rule_size_threshold = float(config_get('rules', 'single_rse_rule_size_threshold', raise_exception=True, default=50e12))
    except (NoOptionError, NoSectionError, RuntimeError):
        single_rse_rule_size_threshold = 50e12

    auto_approve_activity = 'User AutoApprove'

    # Check if the account is banned
    if has_account_attribute(account, 'rule_banned', session=session):
        return False

    # Check activity is User AutoApprove
    if rule_attributes['activity'] != auto_approve_activity:
        return False

    # Check if the rule is locked
    if rule_attributes['locked']:
        return False

    # Check if the rule lifetime is less than a month
    if rule_attributes['lifetime'] > rule_lifetime_threshold:
        return False

    size_of_rule = sum([file['bytes'] for file in list_files(did['scope'], did['name'], session=session)])

    # Limit single RSE rules to 50 TB
    # This does not mean that the total locks size at a RSE will be limited to 50 TB
    # as other rules that are spread over multiple RSEs may claim the same space
    # This is just a simple check to avoid a single RSE rules from being too large
    rse_expression = rule_attributes['rse_expression']
    rses = parse_expression(rse_expression, filter_={'availability_write': True}, session=session)
    if len(rses) == 1:
        this_rse_autoapprove_rules = list_rules(filters={'account': account, 'activity': auto_approve_activity, 'rse_expression': rse_expression}, session=session)
        this_rse_autoapprove_usage = _get_rules_size(this_rse_autoapprove_rules)
        if this_rse_autoapprove_usage + size_of_rule > single_rse_rule_size_threshold:
            return False

    # Check global usage of the account under this activity
    all_auto_approve_rules_by_account = list_rules(filters={'account': account, 'activity': auto_approve_activity}, session=session)
    global_auto_approve_usage_by_account = _get_rules_size(all_auto_approve_rules_by_account)
    if global_auto_approve_usage_by_account + size_of_rule > global_usage_threshold:
        return False

    return True
