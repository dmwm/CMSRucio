"""
Auto approve algorithm for CMS Rucio policy
"""
import re
from configparser import NoOptionError, NoSectionError


def global_approval(did, rule_attributes, session):
    """
    Auto approves rules by users that satisfy the following conditions:
    - Activity is Data Brokering
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
    from rucio.core.did import list_files
    from rucio.core.rule import list_rules


    try:
        GLOBAL_USAGE_THRESHOLD = float(config_get('rules', 'global_usage_threshold', raise_exception=True, default=1e15))
    except (NoOptionError, NoSectionError, RuntimeError):
        GLOBAL_USAGE_THRESHOLD = 1e15

    try:
        RULE_LIFETIME_THRESHOLD = int(config_get('rules', 'rule_lifetime_threshold', raise_exception=True, default=2592000))
    except (NoOptionError, NoSectionError, RuntimeError):
        RULE_LIFETIME_THRESHOLD = 2592000

    account = rule_attributes['account']

    # Check if the account is banned
    if has_account_attribute(account, 'rule_banned', session=session):
        return False

    # Check activity is Data Brokering
    if rule_attributes['activity'] != 'Data Brokering':
        return False

    # Check if the rule is locked
    if rule_attributes['locked']:
        return False

    # Check if the rule lifetime is less than a month
    if rule_attributes['lifetime'] > RULE_LIFETIME_THRESHOLD:
        return False

    # Check global usage of the account under this activity
    data_brokering_usage = 0
    data_brokering_rules = list_rules(filters={'account': account, 'activity': 'Data Brokering'}, session=session)
    for db_rule in data_brokering_rules:
        scope = db_rule['scope']
        name = db_rule['name']
        rule_files = list_files(scope, name, session=session)
        rule_size = sum([file['bytes'] for file in rule_files])
        data_brokering_usage += rule_size

    this_rule_size = sum([file['bytes'] for file in list_files(did['scope'], did['name'], session=session)])
    if data_brokering_usage + this_rule_size > GLOBAL_USAGE_THRESHOLD:
        return False

    return True
