#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File        : rucio_account_usage_locks.py
Author      : Panos Paparrigopoulos <panos.paparrigopoulos AT cern [DOT] ch>
Description : Sends aggregated Rucio data to es-cms.
Inspect results: 
"""

import time
import click as click
from pyspark.sql.functions import lit, col, round as _round, sum as _sum, collect_set, size as _size, coalesce
from CMSSpark.spark_utils import get_spark_session
from CMSMonitoring.amq_sender import credentials, send_to_amq, drop_nulls_in_dict
from hadoop_queries import get_df_rses, get_df_locks, get_df_account_limits, get_df_accounts

@click.command()
@click.option("--creds", required=True, help="etc/secrets/amq.json")
@click.option("--amq_batch_size", type=click.INT, required=False, help="AMQ transaction batch size",
              default=100)
def main(creds, amq_batch_size):
    tb_denominator = 10 ** 12
    creds_json = credentials(f_name=creds)
    creds_json['type'] = 'account_usage_locks'
    spark = get_spark_session(app_name='cmsmonit-rucio-account-usage-locks')

    df_rses = get_df_rses(spark)
    df_locks = get_df_locks(spark)
    df_accounts = get_df_accounts(spark)
    df_account_limits = get_df_account_limits(spark).join(df_rses, ['rse_id']).select('RSE', 'account_name', 'account_limit').cache()
    locks = (
        df_locks.join(df_rses, ["rse_id"], how="left")
        .filter(col("rse_kind") == "prod")
        .select(["f_name", "f_size", "RSE", "rse_type", "account_name"])
        .cache()
        )

    aggregated_locks = (
        locks.groupby(['f_name', 'f_size', 'RSE', 'rse_type'])
        .agg(collect_set('account_name').alias('unique_accounts'))
        .select(['f_name', 'f_size', 'RSE', 'rse_type', 'unique_accounts'])
    )

    uniquely_locked = aggregated_locks.where(_size(col("unique_accounts")) == 1).cache()
    unique_aggregated = (
        uniquely_locked .groupby(['RSE', 'rse_type', 'unique_accounts']) 
            .agg(_round(_sum(col('f_size')) / tb_denominator, 5).alias('uniquely_locked')) 
            .withColumn('account_name', col('unique_accounts')[0]) 
            .join(df_accounts, ['account_name'], how='left') 
            .join(df_account_limits, ['account_name', 'RSE'], how='left') 
            .select(['RSE', 'rse_type', 'account_name', 'account_type', 'account_limit', 'uniquely_locked']) 
            .cache()
    )

    total_aggregated = (
        locks .select(['f_name', 'f_size', 'account_name', 'RSE', 'rse_type']) 
            .distinct() 
            .groupby(['account_name', 'RSE', 'rse_type']) 
            .agg(_round(_sum(col('f_size')) / tb_denominator, 5).alias('total_locked')) 
            .join(df_accounts, ['account_name'], how='left') 
            .join(df_account_limits, ['account_name', 'RSE'], how='left') 
            .cache()
    )

    unique_aggregated = unique_aggregated.withColumn("account_limit", coalesce("account_limit", lit(0)))
    total_aggregated = total_aggregated.withColumn("account_limit", coalesce("account_limit", lit(0)))

    timestamp = int(time.time())
    final = (
        total_aggregated.join(unique_aggregated, ['RSE', 'rse_type', 'account_name', 'account_limit', 'account_type'], how='left')
            .withColumnRenamed('RSE', 'rse_name')
            .withColumn('timestamp', lit(timestamp))
            .select(['rse_name', 'account_name', 'account_limit', 'account_type', 'total_locked', 'uniquely_locked', 'rse_type', 'timestamp'])
            .cache()
    )

    # Iterate over list of dicts returned from spark and push to AMQ
    total_size = 0
    for part in final.rdd.mapPartitions(lambda p: [[drop_nulls_in_dict(x.asDict()) for x in p]]).toLocalIterator():
        part_size = len(part)
        print(f"Length of partition: {part_size}")
        send_to_amq(data=part, confs=creds_json, batch_size=amq_batch_size, overwrite_meta_ts=True)
        total_size += part_size
        print(f"Total document size: {total_size}")

if __name__ == "__main__":
    main()
