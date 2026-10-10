#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File        : monitor_utils.py
Description : Shared helpers for the DM monitoring Spark jobs
              (no-access data + local/user space usage).

Readers for the daily Rucio dumps on the analytix HDFS
(/project/awg/cms/rucio/<YYYY-MM-DD>/<table>/part*.avro), HDFS helpers for
the daily roll-up tables, and push_docs, which sends documents to CERN MONIT
via AMQ (CMSMonitoring.amq_sender) or, without --creds, writes them to a
local JSON-lines file (dry run).
"""

import datetime
import json
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, hex as _hex
from pyspark.sql.types import LongType

RUCIO_DUMP = '/project/awg/cms/rucio/{date}/{table}/part*.avro'


def get_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    return spark


def hdfs_handles(spark):
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    return Path, fs


def prune_old_partitions(spark, root, keep_days):
    """Delete <root>/date=YYYY-MM-DD partitions older than keep_days.

    keep_days=0 disables pruning. Values below 370 are refused: the
    no-access job reads the last 365 days of these tables.
    """
    if not keep_days:
        return 0
    if keep_days < 370:
        raise ValueError('keep_days={} would drop data the 365-day window needs'.format(keep_days))
    Path, fs = hdfs_handles(spark)
    if not fs.exists(Path(root)):
        return 0
    cutoff = (datetime.date.today() - datetime.timedelta(days=keep_days)).isoformat()
    removed = 0
    for st in fs.listStatus(Path(root)):
        m = re.fullmatch(r'date=(\d{4}-\d{2}-\d{2})', st.getPath().getName())
        if st.isDirectory() and m and m.group(1) < cutoff:
            fs.delete(st.getPath(), True)
            removed += 1
    print('pruned {} partitions older than {} from {}'.format(removed, cutoff, root))
    return removed


def hdfs_glob_nonempty(spark, pattern):
    Path, fs = hdfs_handles(spark)
    st = fs.globStatus(Path(pattern))
    return st is not None and len(st) > 0


def latest_dump_date(spark, tables=('rses', 'locks', 'rules', 'accounts'), max_back=7):
    """Most recent date for which the Rucio dumps of all required tables have
    actual part files (a bare date directory can exist while sqoop is still
    writing — checking the directory alone is not enough)."""
    today = datetime.date.today()
    for i in range(max_back):
        d = (today - datetime.timedelta(days=i)).isoformat()
        if all(hdfs_glob_nonempty(spark, RUCIO_DUMP.format(date=d, table=t))
               for t in tables):
            return d
    raise RuntimeError('No complete Rucio dump found in the last {} days'.format(max_back))


def read_dump(spark, date, table):
    return spark.read.format('avro').load(RUCIO_DUMP.format(date=date, table=table))


def get_disk_rses(spark, date):
    """Production disk RSEs: rse_id (lowercase hex), rse name."""
    return (
        read_dump(spark, date, 'rses')
        .filter(col('DELETED_AT').isNull())
        .filter(col('RSE_TYPE') == 'DISK')
        .filter(~lower(col('RSE')).contains('test'))
        .filter(~lower(col('RSE')).contains('temp'))
        .withColumn('rse_id', lower(_hex(col('ID'))))
        .select('rse_id', col('RSE').alias('rse'))
    )


def get_locks(spark, date):
    """O/R locks in scope cms: rse_id, f_name, f_size, rule_id, account_name."""
    return (
        read_dump(spark, date, 'locks')
        .filter(col('SCOPE') == 'cms')
        .filter(col('STATE').isin(['O', 'R']))
        .withColumn('rse_id', lower(_hex(col('RSE_ID'))))
        .withColumn('rule_id', lower(_hex(col('RULE_ID'))))
        .select('rse_id', 'rule_id',
                col('NAME').alias('f_name'),
                col('BYTES').cast(LongType()).alias('f_size'),
                col('ACCOUNT').alias('account_name'))
    )


def get_rules(spark, date):
    return (
        read_dump(spark, date, 'rules')
        .withColumn('rule_id', lower(_hex(col('ID'))))
        .select('rule_id', col('ACTIVITY').alias('activity'))
    )


def get_accounts(spark, date):
    return (
        read_dump(spark, date, 'accounts')
        .filter(col('DELETED_AT').isNull())
        .select(col('ACCOUNT').alias('account_name'),
                col('ACCOUNT_TYPE').alias('account_type'))
    )


def date_timestamp(date_str):
    """Epoch seconds at 00:00 UTC of the data date, so each point (including
    backfilled ones) sits on the day it describes rather than the run time.
    Midnight rather than later in the day: a same-day push must never be
    stamped in the future, or Grafana's "until now" range hides it."""
    d = datetime.datetime.fromisoformat(date_str).replace(
        tzinfo=datetime.timezone.utc)
    return int(d.timestamp())


def drop_nulls(d):
    return {k: v for k, v in d.items() if v is not None}


def push_docs(docs, creds, doc_type, batch_size=100, dry_run_path=None):
    """Send documents to MONIT AMQ, or (dry run, creds=None) dump them to a
    local JSON-lines file. Prints the document count and JSON size."""
    docs = [drop_nulls(d) for d in docs]
    payload = sum(len(json.dumps(d)) + 1 for d in docs)
    print('[{}] {} docs, {:.1f} kB JSON'.format(doc_type, len(docs), payload / 1e3))
    if creds:
        from CMSMonitoring.amq_sender import credentials, send_to_amq
        creds_json = credentials(f_name=creds)
        creds_json['type'] = doc_type
        send_to_amq(data=docs, confs=creds_json, batch_size=batch_size,
                    overwrite_meta_ts=True)
    elif dry_run_path:
        with open(dry_run_path, 'a') as f:
            for d in docs:
                f.write(json.dumps(d) + '\n')
        print('[{}] dry run -> appended to {}'.format(doc_type, dry_run_path))
    return len(docs), payload
