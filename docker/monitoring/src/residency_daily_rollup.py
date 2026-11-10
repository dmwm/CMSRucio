#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File        : residency_daily_rollup.py
Description : Self-healing daily roll-up of per-dataset disk residency.

For each day, reduces the cmsmonit daily Rucio tally
  /cms/rucio_daily/rucio/YYYY/MM/DD  (parquet; per (dataset, RSE, day) rows,
  f_dataset_id = DBS dataset id, rep_size bytes)
to one row per dataset with a disk footprint that day:
  f_dataset_id | disk_bytes | n_disk_rses | min_create_day | max_create_day

Output layout on HDFS: <out>/date=YYYY-MM-DD/ (parquet, _SUCCESS marker).
Same operational model as access_daily_rollup.py: immutable per-day
partitions, each run recomputes only the missing ones (self-healing, no DB).

These roll-ups feed the residency weightings (on-disk-every-day /
time-averaged) of the no-access monitoring (rucio_no_access.py), mirroring
the §5.3 methodology of the no-access report.
Note: the tally has occasional gaps (e.g. sparse Aug 2025); missing source
days are reported and simply reduce the tally-day count downstream.
"""

import datetime
import time

import click
from pyspark.sql.functions import (col, count as _count, hex as _hex, lower,
                                   max as _max, min as _min, sum as _sum)

from monitor_utils import (get_disk_rses, get_spark, hdfs_handles, latest_dump_date,
                           prune_old_partitions)

TALLY = '/cms/rucio_daily/rucio/{:%Y/%m/%d}'


@click.command()
@click.option('--out', default='/user/dmtops/residency_daily', show_default=True,
              help='HDFS directory holding the per-day residency partitions')
@click.option('--lookback', default=30, show_default=True,
              help='Days to scan for missing partitions (set ~400 to backfill a year)')
@click.option('--keep-days', default=0, show_default=True,
              help='Delete days older than this (0 = never; minimum 370). Set in production.')
def main(out, lookback, keep_days):
    spark = get_spark('cmsmonit-residency-daily-rollup')
    Path, fs = hdfs_handles(spark)
    dump_date = latest_dump_date(spark, tables=('rses',))
    disk_ids = [r.rse_id for r in get_disk_rses(spark, dump_date).collect()]
    assert len(disk_ids) > 50, 'disk RSE list suspiciously small'
    print('disk RSEs ({}): {}'.format(dump_date, len(disk_ids)))

    end = datetime.date.today() - datetime.timedelta(days=1)
    done = skipped = missing = failed = 0
    for i in range(lookback, -1, -1):
        day = end - datetime.timedelta(days=i)
        part = '{}/date={}'.format(out, day.isoformat())
        src = TALLY.format(day)
        if fs.exists(Path(part + '/_SUCCESS')):
            skipped += 1
            continue
        if not fs.exists(Path(src)):
            print(day.isoformat(), 'NO TALLY on HDFS', flush=True)
            missing += 1
            continue
        t0 = time.time()
        try:
            (spark.read.parquet(src)
             .filter(col('SCOPE') == 'cms').filter(col('f_dataset_id').isNotNull())
             .withColumn('rse_id', lower(_hex(col('RSE_ID'))))
             .filter(col('rse_id').isin(disk_ids))
             .groupBy('f_dataset_id')
             .agg(_sum('rep_size').alias('disk_bytes'),
                  _count('*').alias('n_disk_rses'),
                  _min('create_day').alias('min_create_day'),
                  _max('create_day').alias('max_create_day'))
             .repartition(1).write.mode('overwrite').parquet(part))
            done += 1
            print('{}  ok  {:.0f}s'.format(day.isoformat(), time.time() - t0), flush=True)
        except Exception as e:
            failed += 1
            print('{}  FAIL  {}'.format(day.isoformat(), str(e)[:140]), flush=True)
    print('residency rollup: computed={} up-to-date={} no-tally={} failed={}'.format(
        done, skipped, missing, failed))
    prune_old_partitions(spark, out, keep_days)


if __name__ == '__main__':
    main()
