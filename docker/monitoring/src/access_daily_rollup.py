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

Output layout on HDFS: <out>/date=YYYY-MM-DD/ (parquet, _SUCCESS marker).

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
from pyspark.sql.functions import (col, concat_ws, countDistinct, lit, size,
                                   split, sum as _sum)
from pyspark.sql.types import LongType, StringType, StructField, StructType

from monitor_utils import get_spark, hdfs_handles, prune_old_partitions

CONDOR_RAW = '/project/monitoring/archive/condor/raw/metric'
CMSSWPOP = '/project/monitoring/archive/cmssw_pop/raw/metric'

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


@click.command()
@click.option('--out', default='/user/dmtops/access_daily', show_default=True,
              help='HDFS directory holding the per-day roll-up partitions')
@click.option('--lookback', default=30, show_default=True,
              help='Days to scan for missing partitions (set ~400 to backfill a year)')
@click.option('--keep-days', default=0, show_default=True,
              help='Delete days older than this (0 = never; minimum 370). Set in production.')
def main(out, lookback, keep_days):
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


if __name__ == '__main__':
    main()
