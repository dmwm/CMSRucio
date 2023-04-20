from rucio.client.ruleclient import RuleClient


def create_rule(dids, options, dry_run=False):

    rule_account = options["rule_account"]

    rule_client = RuleClient(account=rule_account)

    rse_expression = options["dest_rse_expression"]
    lifetime = options["lifetime"]
    copies = options["copies"]
    ask_approval = options["ask_approval"]

    if dry_run:
        print(f"A rule will be created on {dids} at {rse_expression} for { lifetime if lifetime else 'Infinity'}")
        return ["DryRun"]

    return rule_client.add_replication_rule(
        dids=dids,
        copies=copies,
        rse_expression=rse_expression,
        grouping="ALL",
        activity="User Subscriptions",
        lifetime=lifetime,
        ask_approval=ask_approval
    )
