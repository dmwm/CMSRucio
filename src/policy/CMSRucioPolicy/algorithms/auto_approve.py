"""
Auto approve algorithm for CMS Rucio policy
"""


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

    auto_approve_activity = 'User AutoApprove'

    # All checks are performed at rule creation
    # The approval conditions are define in _check_for_auto_approve_eligibility function in the permissions module
    # Check activity is User AutoApprove

    if rule['activity'] == auto_approve_activity:
        return True

    return False
