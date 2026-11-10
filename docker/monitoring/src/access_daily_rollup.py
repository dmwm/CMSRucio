#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File        : access_daily_rollup.py
Description : Self-healing daily roll-up of CMS dataset (container) accesses.

For each day, reduces the raw access streams on HDFS
  - condor classads  /project/monitoring/archive/condor/raw/metric/YYYY/MM/DD
  - cmssw popularity /project/monitoring/archive/cmssw_pop/raw/metric/YYYY/MM/DD
to a tiny per-container table (a few thousand rows/day):
  container | cond_jobs | pop_files | pop_read_TB | access_date

It also reduces the Rucio traces (the messages Rucio's kronos daemon turns
into replica accessed_at), /project/monitoring/archive/cms_rucio/raw/tracer,
to a second table, one row per container, sender and kind:
  container | sender | kind | n | access_date
kind 'read' = traces kronos would use for accessed_at (get*/download with a
remoteSite), kind 'touch' = explicit touches; sender = the trace 'account'
(fwjr = WMAgent job reports, or the person who touched). Traces are archived
from 2025-10-10; per-sender totals are printed for every day written.

Output layout on HDFS: <out>/date=YYYY-MM-DD/ (parquet, _SUCCESS marker),
same for <rucio-out>.

The job is stateless in the operational sense: on every run it scans the
trailing --lookback days, finds partitions that are missing, and recomputes
exactly those from the raw sources. Partitions are immutable and individually
reproducible, so there is no database and no state to repair by hand — an
interrupted or skipped run is healed by the next one. (~2 min/day measured on
the analytix cluster; a normal daily run processes 1 day.)

Days are processed up to today-2: the most recent raw partitions may not be
fully flushed yet.

This is the only expensive part of the no-access monitoring; the daily
aggregation job (rucio_no_access.py) reads these roll-ups instead of
re-scanning a year of raw data.
"""

import datetime
import time

import click
from pyspark.sql.functions import (coalesce, col, concat_ws, count as _count,
                                   countDistinct, input_file_name, lit,
                                   regexp_extract, size, split, sum as _sum,
                                   trim, when)
from pyspark.sql.types import LongType, StringType, StructField, StructType

from monitor_utils import get_spark, hdfs_handles, prune_old_partitions

CONDOR_RAW = '/project/monitoring/archive/condor/raw/metric'
CMSSWPOP = '/project/monitoring/archive/cmssw_pop/raw/metric'
RUCIO_TRACES = '/project/monitoring/archive/cms_rucio/raw/tracer'
RUCIO_CHUNK_DAYS = 10   # trace days read in one pass (many small gzip files)

CONDOR_SCHEMA = StructType([StructField('data', StructType([
    StructField('CMSPrimaryPrimaryDataset', StringType()),
    StructField('CMSPrimaryProcessedDataset', StringType()),
    StructField('CMSPrimaryDataTier', StringType()),
    StructField('GlobalJobId', StringType()),
]))])
POP_SCHEMA = StructType([StructField('data', StructType([
    StructField('file_lfn', StringType()),
    StructField('read_bytes', LongType()),
]))])
TRACE_SCHEMA = StructType([StructField('data', StructType([
    StructField('account', StringType()),
    StructField('eventType', StringType()),
    StructField('remoteSite', StringType()),
    StructField('scope', StringType()),
    StructField('filename', StringType()),
    StructField('dataset', StringType()),
]))])


def _day_path(root, date):
    y, m, d = date.split('-')
    return '{}/{}/{}/{}'.format(root, y, m, d)


def rollup_day(spark, date):
    """Per-container accessed set for one day; tolerates one missing source."""
    Path, fs = hdfs_handles(spark)
    cond = pop = None
    cday, pday = _day_path(CONDOR_RAW, date), _day_path(CMSSWPOP, date)
    if fs.exists(Path(cday)):
        cond = (spark.read.schema(CONDOR_SCHEMA).json(cday).select('data.*')
                .where(col('CMSPrimaryPrimaryDataset').isNotNull() &
                       (col('CMSPrimaryPrimaryDataset') != 'Unknown'))
                .withColumn('container', concat_ws('/', lit(''),
                            'CMSPrimaryPrimaryDataset', 'CMSPrimaryProcessedDataset',
                            'CMSPrimaryDataTier'))
                .groupBy('container')
                .agg(countDistinct('GlobalJobId').alias('cond_jobs')))
    if fs.exists(Path(pday)):
        pr = split(col('file_lfn'), '/')
        pop = (spark.read.schema(POP_SCHEMA).json(pday).select('data.*')
               .where(col('file_lfn').rlike('^/store/(data|mc)/') & (size(pr) >= 8))
               .withColumn('container', concat_ws('/', lit(''),
                           pr.getItem(4), concat_ws('-', pr.getItem(3), pr.getItem(6)),
                           pr.getItem(5)))
               .groupBy('container')
               .agg(countDistinct('file_lfn').alias('pop_files'),
                    (_sum('read_bytes') / 1e12).alias('pop_read_TB')))
    if cond is None and pop is None:
        return None
    if pop is None:
        out = cond.withColumn('pop_files', lit(0)).withColumn('pop_read_TB', lit(0.0))
    elif cond is None:
        out = pop.withColumn('cond_jobs', lit(0))
    else:
        out = (cond.join(pop, 'container', 'full_outer')
               .na.fill(0, ['cond_jobs', 'pop_files', 'pop_read_TB']))
    return (out.select('container', 'cond_jobs', 'pop_files', 'pop_read_TB')
            .withColumn('access_date', lit(date)))


def rucio_rows(spark, dates):
    """Per (day, container, sender, kind) trace counts for several days in one
    pass. Mirrors kronos: reads are get*/sm_get*/download events (not *_es)
    with a scope and a non-empty remoteSite; touches are 'touch' events, on a
    file or on a block. Files map to containers as for cmssw_pop (plus HI and
    RelVal areas), blocks by cutting the name at '#'."""
    paths = [_day_path(RUCIO_TRACES, d) for d in dates]
    day = r'/(\d{4})/(\d{2})/(\d{2})/'
    et = coalesce(col('eventType'), lit(''))
    is_read = ((et.startswith('get') | et.startswith('sm_get') | (et == 'download')) &
               ~et.endswith('_es') & col('scope').isNotNull() &
               (trim(coalesce(col('remoteSite'), lit(''))) != ''))
    pr = split(col('filename'), '/')
    container = (when(col('filename').rlike('^/store/(data|mc|hidata|himc|relval)/') &
                      (size(pr) >= 8),
                      concat_ws('/', lit(''), pr.getItem(4),
                                concat_ws('-', pr.getItem(3), pr.getItem(6)), pr.getItem(5)))
                 .when(col('dataset').contains('#'), split(col('dataset'), '#').getItem(0)))
    src = input_file_name()
    return (spark.read.schema(TRACE_SCHEMA).json(paths)
            .select('data.*', concat_ws('-', regexp_extract(src, day, 1),
                                        regexp_extract(src, day, 2),
                                        regexp_extract(src, day, 3)).alias('access_date'))
            .withColumn('kind', when(is_read, 'read').when(et == 'touch', 'touch'))
            .withColumn('container', container)
            .where(col('kind').isNotNull() & col('container').isNotNull())
            .groupBy('access_date', 'container', col('account').alias('sender'), 'kind')
            .agg(_count('*').alias('n')))


def fill_rucio(spark, out, days):
    """Write the missing per-day Rucio trace partitions, RUCIO_CHUNK_DAYS days
    per read; days with no trace archive are reported and retried next run."""
    Path, fs = hdfs_handles(spark)
    part = '{}/date={}'.format
    todo = [d for d in days if not fs.exists(Path(part(out, d) + '/_SUCCESS'))]
    have = [d for d in todo if fs.exists(Path(_day_path(RUCIO_TRACES, d)))]
    for d in sorted(set(todo) - set(have)):
        print(d, 'NO TRACES', flush=True)
    done = failed = 0
    for i in range(0, len(have), RUCIO_CHUNK_DAYS):
        chunk = have[i:i + RUCIO_CHUNK_DAYS]
        t0 = time.time()
        rows = rucio_rows(spark, chunk).cache()
        try:
            for d in chunk:
                day_rows = rows.where(col('access_date') == d)
                day_rows.coalesce(1).write.mode('overwrite').parquet(part(out, d))
                senders = (day_rows.groupBy('sender', 'kind').agg(_sum('n').alias('n'))
                           .orderBy(col('n').desc()).collect())
                print(d, 'rucio traces:', ', '.join('{}/{}={}'.format(r['sender'], r['kind'], r['n'])
                                                    for r in senders) or 'none', flush=True)
                done += 1
            print('{} days in {:.0f}s'.format(len(chunk), time.time() - t0), flush=True)
        except Exception as e:
            failed += 1
            print('{}..{}  FAIL  {}'.format(chunk[0], chunk[-1], str(e)[:140]), flush=True)
        finally:
            rows.unpersist()
    print('rucio rollup: computed={} up-to-date={} no-traces={} failed-chunks={}'.format(
        done, len(days) - len(todo), len(todo) - len(have), failed))


@click.command()
@click.option('--out', default='/user/dmtops/access_daily', show_default=True,
              help='HDFS directory holding the per-day roll-up partitions')
@click.option('--rucio-out', default='/user/dmtops/rucio_access_daily', show_default=True,
              help='HDFS directory holding the per-day Rucio trace partitions')
@click.option('--lookback', default=30, show_default=True,
              help='Days to scan for missing partitions (set ~400 to backfill a year)')
@click.option('--keep-days', default=0, show_default=True,
              help='Delete days older than this (0 = never; minimum 370). Set in production.')
def main(out, rucio_out, lookback, keep_days):
    spark = get_spark('cmsmonit-access-daily-rollup')
    Path, fs = hdfs_handles(spark)
    end = datetime.date.today() - datetime.timedelta(days=2)
    done = skipped = failed = 0
    for i in range(lookback, -1, -1):
        date = (end - datetime.timedelta(days=i)).isoformat()
        part = '{}/date={}'.format(out, date)
        if fs.exists(Path(part + '/_SUCCESS')):
            skipped += 1
            continue
        t0 = time.time()
        try:
            df = rollup_day(spark, date)
            if df is None:
                print(date, 'NO DATA (both sources missing)', flush=True)
                failed += 1
                continue
            df.coalesce(1).write.mode('overwrite').parquet(part)
            done += 1
            print('{}  ok  {:.0f}s'.format(date, time.time() - t0), flush=True)
        except Exception as e:
            failed += 1
            print('{}  FAIL  {}'.format(date, str(e)[:140]), flush=True)
    print('rollup: computed={} up-to-date={} failed={}'.format(done, skipped, failed))
    prune_old_partitions(spark, out, keep_days)

    fill_rucio(spark, rucio_out, [(end - datetime.timedelta(days=i)).isoformat()
                                  for i in range(lookback, -1, -1)])
    prune_old_partitions(spark, rucio_out, keep_days)


if __name__ == '__main__':
    main()
