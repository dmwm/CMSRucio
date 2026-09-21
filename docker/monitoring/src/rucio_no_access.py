#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File        : rucio_no_access.py
Description : Daily monitoring of CMS disk data with no observed access.

Combines the daily access roll-ups (access_daily_rollup.py; condor classads +
cmssw popularity), the daily residency roll-ups (residency_daily_rollup.py;
cmsmonit Rucio tally) and the Rucio dumps of the evaluation day to compute,
for trailing windows of 90/180/365 days, the set of containers that
  - were created before the window start, and
  - had no access observed in the window,
restricted to data on production disk RSEs. Monitoring version of the
"CMS Disk Data with No Access" report (methodology validated there; windows
here are day-based, the report used calendar months).

Documents pushed per run (window in {3m, 6m, 12m}):
  doc='summary'  : 1/window — no-access bytes & containers, locked/unlocked
                   split, total disk population, and the residency
                   weightings: everyday_bytes (on disk every tally day of the
                   window), twa_bytes (time-averaged over the window),
                   fractional_bytes (replica volume weighted by the fraction
                   of the window the replica existed, from CREATED_AT).
  doc='tier'     : per (window, data tier).
  doc='rse'      : per (window, RSE) — locked/unlocked bytes.
  doc='category' : per (window, locking category) — DM policies / Production /
                   Crab / Local / User, with-overlap replica accounting.
  doc='top'      : top --topn containers by no-access disk volume (12m only),
                   with the locking accounts.
Drill-down documents (12m window only, per-rule attribution — a replica
locked by N rules counts once per rule, so totals slightly exceed the
distinct-locked volume):
  doc='category_rules' : per category — attributed bytes, bytes under
                         never-expiring rules, rule/dataset counts.
  doc='subscription'   : DM-policies bytes by driving subscription
                         ('(manual)' = transfer_ops rules with none).
  doc='prod_tape'      : Production rule bytes by account × tape-backup class
                         (tape 100% OK -> disk copy should be deleted / tape
                         >=99% / tape <99% / no tape rule).
  doc='account'        : top locking accounts per category (replica-dedup
                         volume, not rule-attributed).
"""

import datetime

import click
from pyspark.sql.functions import (coalesce, col, collect_set, count as _count,
                                   countDistinct, element_at, greatest, least,
                                   lit, lower, hex as _hex, max as _max, split,
                                   sum as _sum, upper as _upper, when)
from pyspark.sql.types import LongType

from monitor_utils import (date_timestamp, get_disk_rses, get_locks, get_rules, get_spark,
                           hdfs_glob_nonempty, latest_dump_date, push_docs,
                           read_dump)

WINDOWS = {'3m': 90, '6m': 180, '12m': 365}
DRILL_WINDOW = '12m'
TOP_ACCOUNTS_PER_CATEGORY = 25

# Locking-account categories (same as the no-access report)
DM_ACCTS = ['transfer_ops']
PROD_ACCTS = ['wma_prod', 'wmcore_transferor', 'wmcore_output', 'tier0_prod',
              'wmcore_pileup']
CRAB_ACCTS = ['crab_input']
CRAB_ACTS = ['Analysis TapeRecall']


def category(acct, act):
    return (when(acct.isin(DM_ACCTS), 'DM policies')
            .when(acct.isin(PROD_ACCTS), 'Production')
            .when(acct.isin(CRAB_ACCTS) | act.isin(CRAB_ACTS), 'Crab')
            .when(lower(acct).contains('local'), 'Local')
            .otherwise('User'))


def get_dbs_mapping(spark, eval_date, max_back=7):
    """DBS dataset id -> container name (for joining the residency tally)."""
    d0 = datetime.date.fromisoformat(eval_date)
    for i in range(max_back):
        d = (d0 - datetime.timedelta(days=i)).isoformat()
        path = '/project/awg/cms/dbs/PROD_GLOBAL/{}/DATASETS/part*.gz'.format(d)
        if hdfs_glob_nonempty(spark, path):
            print('DBS DATASETS dump:', d)
            return (spark.read.csv(path)
                    .select(col('_c0').cast(LongType()).alias('f_dataset_id'),
                            col('_c1').alias('container'))
                    .dropDuplicates(['container']))
    print('!! no DBS DATASETS dump found - residency weightings skipped')
    return None


def get_rules_full(spark, eval_date):
    """Rules with drill-down columns (guarded against dump schema changes)."""
    want = ['ID', 'SUBSCRIPTION_ID', 'ACCOUNT', 'NAME', 'STATE', 'RSE_EXPRESSION',
            'EXPIRES_AT', 'LOCKS_OK_CNT', 'LOCKS_REPLICATING_CNT',
            'LOCKS_STUCK_CNT']
    raw = read_dump(spark, eval_date, 'rules')
    sel = [c for c in want if c in raw.columns]
    missing = [c for c in want if c not in raw.columns]
    if missing:
        print('!! rules dump missing columns:', missing)
    return (raw.select(*sel)
            .withColumn('rule_id', lower(_hex(col('ID'))))
            .withColumn('sub_id', when(col('SUBSCRIPTION_ID').isNotNull(),
                                       lower(_hex(col('SUBSCRIPTION_ID')))))
            .withColumn('rcn', element_at(split(col('NAME'), '#'), 1)))


@click.command()
@click.option('--creds', default=None, help='etc/secrets/amq.json (omit for dry run)')
@click.option('--date', default=None, help='Rucio dump date YYYY-MM-DD (default: latest)')
@click.option('--rollup', default='/user/dmtops/access_daily', show_default=True,
              help='HDFS path of the daily access roll-ups')
@click.option('--resid', default='/user/dmtops/residency_daily', show_default=True,
              help='HDFS path of the daily residency roll-ups')
@click.option('--topn', default=100, show_default=True)
@click.option('--amq-batch-size', default=100, show_default=True)
@click.option('--dry-run-out', default='no_access_docs.json', show_default=True)
def main(creds, date, rollup, resid, topn, amq_batch_size, dry_run_out):
    spark = get_spark('cmsmonit-rucio-no-access')
    eval_date = date or latest_dump_date(
        spark, tables=('rses', 'dids', 'contents', 'replicas', 'locks', 'rules'))
    eval_d = datetime.date.fromisoformat(eval_date)
    timestamp = date_timestamp(eval_date)
    eval_ms = timestamp * 1000
    dry = None if creds else dry_run_out
    print('eval date:', eval_date)

    wstart = {w: (eval_d - datetime.timedelta(days=n)).isoformat()
              for w, n in WINDOWS.items()}
    wstart_ms = {w: date_timestamp(s) * 1000 for w, s in wstart.items()}
    min_start = min(wstart.values())

    # Last observed access per container over the widest window
    acc = (spark.read.parquet(rollup)
           .where((col('access_date') >= lit(min_start)) &
                  (col('access_date') <= lit(eval_date)))
           .groupBy('container').agg(_max('access_date').alias('last_access')))

    rses = get_disk_rses(spark, eval_date).cache()
    rse_ids = [r.rse_id for r in rses.collect()]

    dids = (read_dump(spark, eval_date, 'dids')
            .filter(col('SCOPE') == 'cms').filter(col('DELETED_AT').isNull()))
    cont = read_dump(spark, eval_date, 'contents').filter(col('SCOPE') == 'cms')
    c2d = (cont.filter((col('DID_TYPE') == 'C') & (col('CHILD_TYPE') == 'D'))
           .select(col('NAME').alias('cn'), col('CHILD_NAME').alias('dn')))
    d2f = (cont.filter((col('DID_TYPE') == 'D') & (col('CHILD_TYPE') == 'F'))
           .select(col('NAME').alias('dn'), col('CHILD_NAME').alias('f_name')))
    ctr = (dids.filter(col('DID_TYPE') == 'C')
           .select(col('NAME').alias('cn'),
                   col('CREATED_AT').cast(LongType()).alias('created_ms')))
    rep = (read_dump(spark, eval_date, 'replicas')
           .filter(col('SCOPE') == 'cms')
           .withColumn('rse_id', lower(_hex(col('RSE_ID'))))
           .filter(col('rse_id').isin(rse_ids))
           .select('rse_id', col('NAME').alias('f_name'),
                   col('BYTES').cast(LongType()).alias('bytes'),
                   col('LOCK_CNT').alias('lock_cnt'),
                   col('CREATED_AT').cast(LongType()).alias('rc')))

    cont_files = ctr.join(c2d, 'cn').join(d2f, 'dn').select('cn', 'created_ms', 'f_name')

    # Per (container, RSE): disk bytes, locked bytes, and per-window
    # fractional-residency bytes (replica weighted by the fraction of the
    # window it existed, from replica CREATED_AT — report §5.3 'fractional').
    def frac_expr(w):
        w0, wlen = wstart_ms[w], float(eval_ms - wstart_ms[w])
        f = least(lit(1.0), greatest(lit(0.0),
                                     (lit(eval_ms) - greatest(col('rc'), lit(w0))) / lit(wlen)))
        return _sum(col('bytes') * f).alias('frac_bytes_' + w)

    percont = (
        cont_files.join(rep, 'f_name')
        .groupBy('cn', 'rse_id')
        .agg(_max('created_ms').alias('created_ms'),
             _sum('bytes').alias('bytes'),
             _sum(when(col('lock_cnt') > 0, col('bytes')).otherwise(0)).alias('locked_bytes'),
             *[frac_expr(w) for w in WINDOWS])
        .join(acc.withColumnRenamed('container', 'cn'), 'cn', 'left')
        .withColumn('tier', element_at(split(col('cn'), '/'), -1))
        .join(rses, 'rse_id')
        .cache()
    )

    pop = percont.agg(_sum('bytes').alias('b'), countDistinct('cn').alias('n')).collect()[0]
    print('population: {} containers, {:.1f} PB'.format(pop['n'], pop['b'] / 1e15))

    colds = {}
    for w in WINDOWS:
        colds[w] = percont.filter(
            (col('created_ms') < lit(wstart_ms[w])) &
            (col('last_access').isNull() | (col('last_access') < lit(wstart[w])))
        ).cache()

    # Locks joined once on the widest cold superset (3m ⊇ 6m ⊇ 12m)
    cold3_cn = colds['3m'].select('cn').distinct()
    locks = get_locks(spark, eval_date).filter(col('rse_id').isin(rse_ids))
    lr = (cont_files.join(cold3_cn, 'cn', 'leftsemi')
          .join(locks, 'f_name')
          .join(get_rules(spark, eval_date), 'rule_id')
          .withColumn('cat', category(col('account_name'), col('activity')))
          .select('cn', 'f_name', 'rse_id', 'f_size', 'rule_id', 'account_name', 'cat')
          .cache())
    lock_cat = lr.select('cn', 'f_name', 'rse_id', 'f_size', 'cat').distinct()

    # Residency inputs (graceful degradation if roll-ups/DBS unavailable)
    dbs = get_dbs_mapping(spark, eval_date)
    resid_df = None
    if hdfs_glob_nonempty(spark, resid + '/date=*'):
        resid_df = (spark.read.parquet(resid)
                    .where((col('date') >= lit(min_start)) & (col('date') <= lit(eval_date))))
    else:
        print('!! no residency roll-ups at', resid, '- residency weightings skipped')

    docs = {k: [] for k in ('summary', 'tier', 'rse', 'category', 'top',
                            'category_rules', 'subscription', 'prod_tape', 'account')}

    for w in WINDOWS:
        cold = colds[w]
        base = {'doc': None, 'window': w, 'window_days': WINDOWS[w],
                'eval_date': eval_date, 'timestamp': timestamp}

        s = cold.agg(_sum('bytes').alias('b'), _sum('locked_bytes').alias('l'),
                     countDistinct('cn').alias('n'),
                     _sum('frac_bytes_' + w).alias('f')).collect()[0]
        summary = dict(base, doc='summary',
                       no_access_bytes=s['b'] or 0,
                       locked_bytes=s['l'] or 0,
                       unlocked_bytes=(s['b'] or 0) - (s['l'] or 0),
                       n_containers=s['n'],
                       fractional_bytes=int(s['f'] or 0),
                       population_bytes=pop['b'],
                       population_containers=pop['n'])

        # Residency weightings: every-day + time-averaged (report §5.3)
        if resid_df is not None and dbs is not None:
            rw = resid_df.where(col('date') >= lit(wstart[w]))
            days_avail = rw.select('date').distinct().count()
            stats = (rw.groupBy('f_dataset_id')
                     .agg(countDistinct('date').alias('days_present'),
                          (_sum('disk_bytes') / days_avail).alias('twa_b')))
            coldcn = cold.groupBy('cn').agg(_sum('bytes').alias('snap_b'))
            j = (coldcn.join(dbs, coldcn.cn == dbs.container, 'left')
                 .join(stats, 'f_dataset_id', 'left'))
            r = j.agg(
                _sum(when(col('days_present') == lit(days_avail), col('snap_b'))).alias('ed'),
                _count(when(col('days_present') == lit(days_avail), lit(1))).alias('ed_n'),
                _sum('twa_b').alias('twa'),
                _sum(when(col('days_present').isNull(), col('snap_b'))).alias('miss'),
            ).collect()[0]
            summary.update(everyday_bytes=int(r['ed'] or 0),
                           everyday_containers=r['ed_n'],
                           twa_bytes=int(r['twa'] or 0),
                           tally_days=days_avail,
                           tally_missing_bytes=int(r['miss'] or 0))
        docs['summary'].append(summary)

        for r in (cold.groupBy('tier').agg(_sum('bytes').alias('bytes'),
                                           countDistinct('cn').alias('n_containers'))
                  .collect()):
            docs['tier'].append(dict(base, doc='tier', **r.asDict()))

        for r in (cold.groupBy('rse').agg(_sum('bytes').alias('bytes'),
                                          _sum('locked_bytes').alias('locked_bytes'))
                  .collect()):
            d = r.asDict()
            d['unlocked_bytes'] = d['bytes'] - d['locked_bytes']
            docs['rse'].append(dict(base, doc='rse', **d))

        cold_cn = cold.select('cn').distinct()
        for r in (lock_cat.join(cold_cn, 'cn', 'leftsemi')
                  .groupBy('cat').agg(_sum('f_size').alias('locked_bytes'))
                  .collect()):
            docs['category'].append(dict(base, doc='category', category=r['cat'],
                                         locked_bytes=r['locked_bytes']))

    # ---- 12m-only: top containers (with locking accounts) + drill-down ----
    wbase = {'doc': None, 'window': DRILL_WINDOW, 'window_days': WINDOWS[DRILL_WINDOW],
             'eval_date': eval_date, 'timestamp': timestamp}
    cold12 = colds[DRILL_WINDOW]
    cold12_cn = cold12.select('cn').distinct()
    lr12 = lr.join(cold12_cn, 'cn', 'leftsemi').cache()

    top = (cold12.groupBy('cn', 'tier')
           .agg(_sum('bytes').alias('bytes'), _sum('locked_bytes').alias('locked_bytes'))
           .orderBy(col('bytes').desc()).limit(topn).cache())
    top_accts = {r['cn']: sorted(r['accts']) for r in
                 (top.select('cn').join(lr12, 'cn', 'inner')
                  .groupBy('cn').agg(collect_set('account_name').alias('accts'))
                  .collect())}
    for rank, r in enumerate(top.collect(), 1):
        docs['top'].append(dict(wbase, doc='top', rank=rank, container=r['cn'],
                                tier=r['tier'], bytes=r['bytes'],
                                locked_bytes=r['locked_bytes'],
                                accounts=','.join(top_accts.get(r['cn'], []))))

    # Per-rule attribution (report §5.4.2.1)
    rf = get_rules_full(spark, eval_date)
    prb = (lr12.select('rule_id', 'cat', 'f_name', 'rse_id', 'f_size').distinct()
           .groupBy('rule_id', 'cat').agg(_sum('f_size').alias('cb'))
           .join(rf, 'rule_id', 'left')
           .cache())
    print('attributed locked no-access total: {:.2f} PB'.format(
        (prb.agg(_sum('cb')).collect()[0][0] or 0) / 1e15))

    for r in (prb.groupBy('cat')
              .agg(_sum('cb').alias('attributed_bytes'),
                   _sum(when(col('EXPIRES_AT').isNull(), col('cb')).otherwise(0))
                   .alias('never_expires_bytes'),
                   _count('*').alias('n_rules'),
                   countDistinct('rcn').alias('n_datasets'))
              .collect()):
        docs['category_rules'].append(dict(wbase, doc='category_rules',
                                           category=r['cat'], **{k: r[k] for k in
                                           ('attributed_bytes', 'never_expires_bytes',
                                            'n_rules', 'n_datasets')}))

    # DM policies: by driving subscription
    dm = prb.filter(col('cat') == 'DM policies')
    subs = None
    if hdfs_glob_nonempty(spark, '/project/awg/cms/rucio/{}/subscriptions/part*.avro'.format(eval_date)):
        subs = (read_dump(spark, eval_date, 'subscriptions')
                .withColumn('sub_id', lower(_hex(col('ID'))))
                .select('sub_id', col('NAME').alias('subscription')))
        dm = dm.join(subs, 'sub_id', 'left')
    else:
        print('!! no subscriptions dump - using raw subscription ids')
        dm = dm.withColumn('subscription', col('sub_id'))
    for r in (dm.withColumn('subscription',
                            when(col('sub_id').isNull(), '(manual)')
                            .otherwise(coalesce(col('subscription'), col('sub_id'))))
              .groupBy('subscription')
              .agg(_sum('cb').alias('bytes'), countDistinct('rcn').alias('n_datasets'))
              .collect()):
        docs['subscription'].append(dict(wbase, doc='subscription', **r.asDict()))

    # Production: tape-backup class per rule (best tape rule on the container)
    tape = (rf.filter(_upper(col('RSE_EXPRESSION')).contains('TAPE'))
            .withColumn('cnt', coalesce(col('LOCKS_OK_CNT'), lit(0)) +
                        coalesce(col('LOCKS_REPLICATING_CNT'), lit(0)) +
                        coalesce(col('LOCKS_STUCK_CNT'), lit(0)))
            .withColumn('frac_ok', when(col('cnt') > 0, col('LOCKS_OK_CNT') / col('cnt')))
            .groupBy('rcn')
            .agg(_max(when(_upper(col('STATE')).startswith('O'), lit(1)).otherwise(lit(0)))
                 .alias('has_ok_tape'),
                 _max('frac_ok').alias('best_frac_ok')))
    tape_class = (when(col('has_ok_tape') == 1, 'tape_100pct_ok')
                  .when(col('best_frac_ok') >= 0.99, 'tape_ge99pct')
                  .when(col('best_frac_ok').isNotNull(), 'tape_lt99pct')
                  .otherwise('no_tape_rule'))
    for r in (prb.filter(col('cat') == 'Production').join(tape, 'rcn', 'left')
              .withColumn('tape_class', tape_class)
              .groupBy('ACCOUNT', 'tape_class')
              .agg(_sum('cb').alias('bytes'), countDistinct('rcn').alias('n_datasets'))
              .collect()):
        docs['prod_tape'].append(dict(wbase, doc='prod_tape', account=r['ACCOUNT'],
                                      tape_class=r['tape_class'], bytes=r['bytes'],
                                      n_datasets=r['n_datasets']))

    # Top locking accounts per category (replica-dedup volume)
    acct_rows = (lr12.select('cat', 'account_name', 'cn', 'f_name', 'rse_id', 'f_size').distinct()
                 .groupBy('cat', 'account_name')
                 .agg(_sum('f_size').alias('bytes'), countDistinct('cn').alias('n_datasets'))
                 .collect())
    by_cat = {}
    for r in acct_rows:
        by_cat.setdefault(r['cat'], []).append(r)
    for cat, rows in by_cat.items():
        for r in sorted(rows, key=lambda x: -x['bytes'])[:TOP_ACCOUNTS_PER_CATEGORY]:
            docs['account'].append(dict(wbase, doc='account', category=cat,
                                        account=r['account_name'], bytes=r['bytes'],
                                        n_datasets=r['n_datasets']))

    for k, batch in docs.items():
        push_docs(batch, creds, 'rucio_no_access', amq_batch_size, dry)


if __name__ == '__main__':
    main()
