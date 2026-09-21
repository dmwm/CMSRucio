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

Every document is computed for each variant, i.e. each combination of
  definition (field 'definition'): which disk copies count
    whole_window : copies on the RSE since before the window start and
                   available (state A) — "on disk all year" for 12m; a copy
                   that arrived during the window (recall, new placement,
                   transfer hop) does not count yet
    snapshot     : every copy on disk on the evaluation day (the report's
                   definition, and the only one with residency weightings)
  sources (field 'sources'): the reads that make a container "accessed", one
    of the SOURCE_PRESETS combinations of
    condor                      CRAB analysis jobs (HTCondor classads)
    cmssw_pop                   CMSSW file reads (production and analysis)
    rucio_accessed_at_no_touch  Rucio read traces (today WMAgent job reports)
    rucio_touch                 explicit Rucio touches
2 definitions x 3 presets = 6 variants, ~4,000 documents per run.

Documents per variant (window in {3m, 6m, 12m}):
  doc='summary'  : 1/window — no-access bytes & containers, locked/unlocked
                   split, total disk population, days of Rucio traces in the
                   window (rucio_days), and for snapshot the residency
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
DEFINITIONS = ('whole_window', 'snapshot')
SOURCE_PRESETS = {
    'condor+cmssw_pop+rucio_accessed_at_no_touch+rucio_touch':
        ('condor', 'cmssw_pop', 'rucio_accessed_at_no_touch', 'rucio_touch'),
    'condor+cmssw_pop+rucio_accessed_at_no_touch':
        ('condor', 'cmssw_pop', 'rucio_accessed_at_no_touch'),
    'condor+cmssw_pop': ('condor', 'cmssw_pop'),
}

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


def drill_down(cold12, lr12, rf, subs, tape, topn, wbase, docs):
    """12m-only documents of one variant: top containers with their locking
    accounts, and the per-rule drill-down."""
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
    top.unpersist()

    # Per-rule attribution (report §5.4.2.1)
    prb = (lr12.select('rule_id', 'cat', 'f_name', 'rse_id', 'f_size').distinct()
           .groupBy('rule_id', 'cat').agg(_sum('f_size').alias('cb'))
           .join(rf, 'rule_id', 'left')
           .cache())
    print('  attributed locked no-access total: {:.2f} PB'.format(
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
    if subs is not None:
        dm = dm.join(subs, 'sub_id', 'left')
    else:
        dm = dm.withColumn('subscription', col('sub_id'))
    for r in (dm.withColumn('subscription',
                            when(col('sub_id').isNull(), '(manual)')
                            .otherwise(coalesce(col('subscription'), col('sub_id'))))
              .groupBy('subscription')
              .agg(_sum('cb').alias('bytes'), countDistinct('rcn').alias('n_datasets'))
              .collect()):
        docs['subscription'].append(dict(wbase, doc='subscription', **r.asDict()))

    # Production: tape-backup class per rule (best tape rule on the container)
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
    prb.unpersist()

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


@click.command()
@click.option('--creds', default=None, help='etc/secrets/amq.json (omit for dry run)')
@click.option('--date', default=None, help='Rucio dump date YYYY-MM-DD (default: latest)')
@click.option('--rollup', default='/user/dmtops/access_daily', show_default=True,
              help='HDFS path of the daily access roll-ups')
@click.option('--rucio-rollup', default='/user/dmtops/rucio_access_daily', show_default=True,
              help='HDFS path of the daily Rucio trace roll-ups')
@click.option('--resid', default='/user/dmtops/residency_daily', show_default=True,
              help='HDFS path of the daily residency roll-ups')
@click.option('--topn', default=100, show_default=True)
@click.option('--doc-type', default='rucio_no_access_v2', show_default=True,
              help='MONIT document type (selects the index)')
@click.option('--amq-batch-size', default=100, show_default=True)
@click.option('--dry-run-out', default='no_access_docs.json', show_default=True)
def main(creds, date, rollup, rucio_rollup, resid, topn, doc_type, amq_batch_size, dry_run_out):
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

    # Last observed access per container and source over the widest window
    in_window = (col('access_date') >= lit(min_start)) & (col('access_date') <= lit(eval_date))
    acc = (spark.read.parquet(rollup).where(in_window)
           .groupBy('container')
           .agg(_max(when(col('cond_jobs') > 0, col('access_date'))).alias('condor'),
                _max(when(col('pop_files') > 0, col('access_date'))).alias('cmssw_pop')))
    rucio_days = {w: 0 for w in WINDOWS}
    if hdfs_glob_nonempty(spark, rucio_rollup + '/date=*'):
        rt = spark.read.parquet(rucio_rollup).where(in_window)
        days = [r[0] for r in rt.select('access_date').distinct().collect()]
        rucio_days = {w: sum(d >= wstart[w] for d in days) for w in WINDOWS}
        acc = acc.join(rt.groupBy('container').agg(
            _max(when(col('kind') == 'read', col('access_date'))).alias('rucio_accessed_at_no_touch'),
            _max(when(col('kind') == 'touch', col('access_date'))).alias('rucio_touch')),
            'container', 'full_outer')
    else:
        print('!! no Rucio trace roll-ups at', rucio_rollup, '- Rucio sources left empty')
        acc = (acc.withColumn('rucio_accessed_at_no_touch', lit(None).cast('string'))
               .withColumn('rucio_touch', lit(None).cast('string')))
    print('days of Rucio traces per window:', rucio_days)

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
                   col('CREATED_AT').cast(LongType()).alias('rc'),
                   col('STATE').alias('st')))

    cont_files = ctr.join(c2d, 'cn').join(d2f, 'dn').select('cn', 'created_ms', 'f_name')

    def whole_window(w):
        """The copy has been on this RSE since before the window start, and is available."""
        return (col('rc') < lit(wstart_ms[w])) & (col('st') == 'A')

    # Per (container, RSE): disk bytes, locked bytes, per-window fractional-
    # residency bytes (replica weighted by the fraction of the window it
    # existed, from replica CREATED_AT — report §5.3 'fractional') and the
    # per-window bytes / locked bytes of whole-window copies.
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
             *[frac_expr(w) for w in WINDOWS],
             *[_sum(when(whole_window(w), col('bytes')).otherwise(0)).alias('ww_bytes_' + w)
               for w in WINDOWS],
             *[_sum(when(whole_window(w) & (col('lock_cnt') > 0), col('bytes')).otherwise(0))
               .alias('ww_locked_bytes_' + w) for w in WINDOWS])
        .join(acc.withColumnRenamed('container', 'cn'), 'cn', 'left')
        .withColumn('tier', element_at(split(col('cn'), '/'), -1))
        .join(rses, 'rse_id')
        .cache()
    )

    pop = percont.agg(_sum('bytes').alias('b'), countDistinct('cn').alias('n')).collect()[0]
    print('population: {} containers, {:.1f} PB'.format(pop['n'], pop['b'] / 1e15))

    # No-access rows per (window, source preset): container created before the
    # window start and no read by any of the preset's sources in the window
    colds = {}
    for w in WINDOWS:
        for p, srcs in SOURCE_PRESETS.items():
            last = greatest(*[col(src) for src in srcs])
            colds[w, p] = percont.filter(
                (col('created_ms') < lit(wstart_ms[w])) &
                (last.isNull() | (last < lit(wstart[w])))
            ).cache()

    def counted(cold, w, d):
        """The copies a definition counts: whole_window keeps the bytes of
        whole-window copies only."""
        if d == 'snapshot':
            return cold
        return (cold.withColumn('bytes', col('ww_bytes_' + w))
                .withColumn('locked_bytes', col('ww_locked_bytes_' + w))
                .where(col('bytes') > 0))

    # Locks joined once on the widest cold superset (3m ⊇ 6m ⊇ 12m, and the
    # preset with fewest sources sees the fewest reads), with each locked
    # replica's whole-window flags
    widest = colds['3m', min(SOURCE_PRESETS, key=lambda p: len(SOURCE_PRESETS[p]))]
    locks = get_locks(spark, eval_date).filter(col('rse_id').isin(rse_ids))
    lr = (cont_files.join(widest.select('cn').distinct(), 'cn', 'leftsemi')
          .join(locks, 'f_name')
          .join(rep.select('f_name', 'rse_id', 'rc', 'st'), ['f_name', 'rse_id'], 'left')
          .join(get_rules(spark, eval_date), 'rule_id')
          .withColumn('cat', category(col('account_name'), col('activity')))
          .select('cn', 'f_name', 'rse_id', 'f_size', 'rule_id', 'account_name', 'cat',
                  *[coalesce(whole_window(w), lit(False)).alias('ww_' + w) for w in WINDOWS])
          .cache())
    lock_cat = lr.select('cn', 'f_name', 'rse_id', 'f_size', 'cat',
                         *['ww_' + w for w in WINDOWS]).distinct()

    def counted_locks(rows, cold, w, d):
        rows = rows.join(cold.select('cn').distinct(), 'cn', 'leftsemi')
        return rows if d == 'snapshot' else rows.where(col('ww_' + w))

    # Residency inputs (graceful degradation if roll-ups/DBS unavailable)
    dbs = get_dbs_mapping(spark, eval_date)
    resid_stats = {}
    if not hdfs_glob_nonempty(spark, resid + '/date=*'):
        print('!! no residency roll-ups at', resid, '- residency weightings skipped')
    elif dbs is not None:
        resid_df = (spark.read.parquet(resid)
                    .where((col('date') >= lit(min_start)) & (col('date') <= lit(eval_date))))
        for w in WINDOWS:
            rw = resid_df.where(col('date') >= lit(wstart[w]))
            days_avail = rw.select('date').distinct().count()
            resid_stats[w] = (days_avail,
                              rw.groupBy('f_dataset_id')
                              .agg(countDistinct('date').alias('days_present'),
                                   (_sum('disk_bytes') / days_avail).alias('twa_b'))
                              .cache())

    # Drill-down inputs shared by all variants
    rf = get_rules_full(spark, eval_date)
    subs = None
    if hdfs_glob_nonempty(spark, '/project/awg/cms/rucio/{}/subscriptions/part*.avro'.format(eval_date)):
        subs = (read_dump(spark, eval_date, 'subscriptions')
                .withColumn('sub_id', lower(_hex(col('ID'))))
                .select('sub_id', col('NAME').alias('subscription')))
    else:
        print('!! no subscriptions dump - using raw subscription ids')
    tape = (rf.filter(_upper(col('RSE_EXPRESSION')).contains('TAPE'))
            .withColumn('cnt', coalesce(col('LOCKS_OK_CNT'), lit(0)) +
                        coalesce(col('LOCKS_REPLICATING_CNT'), lit(0)) +
                        coalesce(col('LOCKS_STUCK_CNT'), lit(0)))
            .withColumn('frac_ok', when(col('cnt') > 0, col('LOCKS_OK_CNT') / col('cnt')))
            .groupBy('rcn')
            .agg(_max(when(_upper(col('STATE')).startswith('O'), lit(1)).otherwise(lit(0)))
                 .alias('has_ok_tape'),
                 _max('frac_ok').alias('best_frac_ok'))
            .cache())

    docs = {k: [] for k in ('summary', 'tier', 'rse', 'category', 'top',
                            'category_rules', 'subscription', 'prod_tape', 'account')}

    for p in SOURCE_PRESETS:
        for d in DEFINITIONS:
            variant = {'definition': d, 'sources': p}
            print('== {} | {}'.format(d, p), flush=True)
            for w in WINDOWS:
                cold = counted(colds[w, p], w, d)
                base = dict(variant, doc=None, window=w, window_days=WINDOWS[w],
                            eval_date=eval_date, timestamp=timestamp)

                s = cold.agg(_sum('bytes').alias('b'), _sum('locked_bytes').alias('l'),
                             countDistinct('cn').alias('n'),
                             _sum('frac_bytes_' + w).alias('f')).collect()[0]
                summary = dict(base, doc='summary',
                               no_access_bytes=s['b'] or 0,
                               locked_bytes=s['l'] or 0,
                               unlocked_bytes=(s['b'] or 0) - (s['l'] or 0),
                               n_containers=s['n'],
                               population_bytes=pop['b'],
                               population_containers=pop['n'],
                               rucio_days=rucio_days[w])

                # Residency weightings: every-day + time-averaged (report §5.3)
                if d == 'snapshot':
                    summary['fractional_bytes'] = int(s['f'] or 0)
                if d == 'snapshot' and w in resid_stats:
                    days_avail, stats = resid_stats[w]
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
                print('  {:>3}: {:.2f} PB no access, {:.2f} PB locked'.format(
                    w, summary['no_access_bytes'] / 1e15, summary['locked_bytes'] / 1e15), flush=True)

                for r in (cold.groupBy('tier').agg(_sum('bytes').alias('bytes'),
                                                   countDistinct('cn').alias('n_containers'))
                          .collect()):
                    docs['tier'].append(dict(base, doc='tier', **r.asDict()))

                for r in (cold.groupBy('rse').agg(_sum('bytes').alias('bytes'),
                                                  _sum('locked_bytes').alias('locked_bytes'))
                          .collect()):
                    row = r.asDict()
                    row['unlocked_bytes'] = row['bytes'] - row['locked_bytes']
                    docs['rse'].append(dict(base, doc='rse', **row))

                for r in (counted_locks(lock_cat, colds[w, p], w, d)
                          .groupBy('cat').agg(_sum('f_size').alias('locked_bytes'))
                          .collect()):
                    docs['category'].append(dict(base, doc='category', category=r['cat'],
                                                 locked_bytes=r['locked_bytes']))

            # ---- 12m-only: top containers (with locking accounts) + drill-down ----
            wbase = dict(variant, doc=None, window=DRILL_WINDOW,
                         window_days=WINDOWS[DRILL_WINDOW],
                         eval_date=eval_date, timestamp=timestamp)
            lr12 = counted_locks(lr, colds[DRILL_WINDOW, p], DRILL_WINDOW, d).cache()
            drill_down(counted(colds[DRILL_WINDOW, p], DRILL_WINDOW, d), lr12,
                       rf, subs, tape, topn, wbase, docs)
            lr12.unpersist()

    for k, batch in docs.items():
        push_docs(batch, creds, doc_type, amq_batch_size, dry)


if __name__ == '__main__':
    main()
