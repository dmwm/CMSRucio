#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File        : rucio_local_usage.py
Description : Daily monitoring of local/user space usage per production RSE.

CMS production RSEs host both centrally-managed and local/user data. Each site
reserves a fraction of its disk (typically 10-15 %) for local use, but the
limit is not enforced. This job measures actual local usage so it can be
compared against the limit in Grafana.

Definition (agreed with DM ops):
  local usage(RSE) = deduplicated size of all file locks held at that RSE by
    - local accounts  (account name contains '_local'), and
    - user  accounts  (Rucio ACCOUNT_TYPE == 'USER'),
  excluding locks whose rule was created with activity
  'User AutoApprove' or 'Analysis TapeRecall'.

The per-site local limit is the disk_local_use value that site admins set in
the cmssst SiteCapacity page: setSiteCapacity writes it into Rucio as the
account limit of the site's local group account, and we read it back from
the account_limits daily dump (see get_local_limits). RSEs without such a
limit get no local_limit_bytes.

Documents pushed (one batch per day):
  doc='rse'     : one per RSE  — local_user_bytes (dedup), local_bytes,
                  user_bytes, n_accounts, local_limit_bytes,
                  limit_used_fraction, over_limit.
  doc='account' : one per (RSE, account) with bytes and file count —
                  the drill-down "who is using the space".
"""

import click
from pyspark.sql.functions import (col, countDistinct, hex as _hex, lower,
                                   max as _max, sum as _sum, when)
from pyspark.sql.types import LongType

from monitor_utils import (date_timestamp, get_accounts, get_disk_rses, get_locks, get_rules,
                           get_spark, latest_dump_date, push_docs, read_dump)

EXCLUDED_ACTIVITIES = ['User AutoApprove', 'Analysis TapeRecall']


def get_local_limits(spark, date, rses):
    """Per-RSE local-space limit.

    setSiteCapacity (CMSRucio/docker/rucio_client/scripts) takes the site's
    disk_local_use from the cmssst SiteCapacity metric and writes it to Rucio
    as the account limit of the <rse>_local_users (or <rse>_local) group
    account on that RSE. The account_limits table is in the daily dumps, so
    the authoritative limit is read from there — no API call needed.

    The account is NOT required to be named after the RSE: some sites name it
    after the site instead (t1_es_pic_local_users on RSE T1_ES_PIC_Disk), and
    demanding <rse>_local_users reported those sites as having no quota. Any
    '_local' account holding a limit on the RSE counts; if several do, the
    largest wins.
    """
    al = (read_dump(spark, date, 'account_limits')
          .withColumn('rse_id', lower(_hex(col('RSE_ID'))))
          .select('rse_id', col('ACCOUNT').alias('account_name'),
                  col('BYTES').cast(LongType()).alias('limit_bytes')))
    return (
        al.join(rses, 'rse_id')
        .filter(lower(col('account_name')).contains('_local'))
        .groupBy('rse').agg(_max('limit_bytes').alias('local_limit_bytes'))
    )


@click.command()
@click.option('--creds', default=None, help='etc/secrets/amq.json (omit for dry run)')
@click.option('--date', default=None, help='Rucio dump date YYYY-MM-DD (default: latest)')
@click.option('--amq-batch-size', default=100, show_default=True)
@click.option('--dry-run-out', default='local_usage_docs.json', show_default=True,
              help='JSON-lines output file when running without --creds')
def main(creds, date, amq_batch_size, dry_run_out):
    spark = get_spark('cmsmonit-rucio-local-usage')
    date = date or latest_dump_date(
        spark, tables=('rses', 'locks', 'rules', 'accounts', 'account_limits'))
    timestamp = date_timestamp(date)
    print('dump date:', date)

    rses = get_disk_rses(spark, date)
    rules = get_rules(spark, date)
    accounts = get_accounts(spark, date)

    # Keep only local/user locks before joining rules: they are a small
    # fraction of all locks, and the rules join is otherwise the biggest shuffle.
    sel = (
        get_locks(spark, date)
        .join(rses, 'rse_id')
        .join(accounts, 'account_name', 'left')
        .withColumn('acct_class',
                    when(col('account_name').contains('_local'), 'local')
                    .when(col('account_type') == 'USER', 'user'))
        .filter(col('acct_class').isNotNull())
        .join(rules, 'rule_id', 'left')
        .filter(col('activity').isNull() | ~col('activity').isin(EXCLUDED_ACTIVITIES))
        .select('rse', 'f_name', 'f_size', 'account_name', 'acct_class')
        .cache()
    )

    # Per-RSE: dedup at file level so two user rules on the same replica count once
    per_rse = (
        sel.select('rse', 'f_name', 'f_size').distinct()
        .groupBy('rse').agg(_sum('f_size').alias('local_user_bytes'))
    )
    local_b = (sel.filter(col('acct_class') == 'local').select('rse', 'f_name', 'f_size')
               .distinct().groupBy('rse').agg(_sum('f_size').alias('local_bytes')))
    user_b = (sel.filter(col('acct_class') == 'user').select('rse', 'f_name', 'f_size')
              .distinct().groupBy('rse').agg(_sum('f_size').alias('user_bytes')))
    n_acc = sel.groupBy('rse').agg(countDistinct('account_name').alias('n_accounts'))

    summary = (per_rse.join(local_b, 'rse', 'left').join(user_b, 'rse', 'left')
               .join(n_acc, 'rse', 'left')
               .join(get_local_limits(spark, date, rses), 'rse', 'left'))

    rse_docs = []
    for r in summary.collect():
        d = r.asDict()
        d.update({'doc': 'rse', 'dump_date': date, 'timestamp': timestamp})
        limit = d.get('local_limit_bytes')
        if limit:
            d['limit_used_fraction'] = round(d['local_user_bytes'] / limit, 4)
            d['over_limit'] = d['local_user_bytes'] > limit
        rse_docs.append(d)

    acct_docs = [
        dict(r.asDict(), doc='account', dump_date=date, timestamp=timestamp)
        for r in (sel.select('rse', 'account_name', 'acct_class', 'f_name', 'f_size')
                  .distinct()
                  .groupBy('rse', 'account_name', 'acct_class')
                  .agg(_sum('f_size').alias('bytes'),
                       countDistinct('f_name').alias('n_files'))
                  .collect())
    ]

    dry = None if creds else dry_run_out
    push_docs(rse_docs, creds, 'rucio_local_usage', amq_batch_size, dry)
    push_docs(acct_docs, creds, 'rucio_local_usage', amq_batch_size, dry)


if __name__ == '__main__':
    main()
