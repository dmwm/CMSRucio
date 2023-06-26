
def setLocalUsersQuota(rclient, group_accounts, rse, quota, logger, dry_run):
    """
    Set quota for local users on an RSE's local account
    """

    for local_account in [ f"{rse.lower()}_local_users", f"{rse.lower()}_local"]:
        if local_account in group_accounts:
            current_quota = rclient.get_local_account_limit(account=local_account, rse=rse)[rse] or 0.0

            if current_quota != quota:
                if dry_run:
                    logger.info(f"Setting quota for {local_account} from {current_quota*1e-12:.2f}TB to {quota*1e-12:.2f}TB, dry_run=True")
                else:
                    logger.info(f"Setting quota for {local_account} from {current_quota*1e-12:.2f}TB to {quota*1e-12:.2f}TB")
                    rclient.set_local_account_limit(account=local_account, rse=rse, bytes=quota)

            break
