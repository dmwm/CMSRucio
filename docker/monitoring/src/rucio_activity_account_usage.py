#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File        : rucio_activity_account_usage.py
Author      : Panos Paparrigopoulos <panos.paparrigopoulos AT cern [DOT] ch>
Description : Sends aggregated Rucio data to monit-opensearch-lt.
Inspect results: 
"""

import time
import click as click
from pyspark.sql.functions import lit, col, round as _round, sum as _sum
from CMSMonitoring.amq_sender import credentials, send_to_amq, drop_nulls_in_dict
from CMSSpark.spark_utils import get_spark_session
from hadoop_queries import get_df_rses, get_df_locks, get_df_rules, get_df_accounts

@click.command()
@click.option("--creds", required=True, help="etc/secrets/amq.json")
@click.option("--amq_batch_size", type=click.INT, required=False, help="AMQ transaction batch size",
              default=100)
def main(creds, amq_batch_size):
    creds_json = credentials(f_name=creds)
    creds_json['type'] = 'activity_account_usage'
    tb_denominator = 10 ** 12
    spark = get_spark_session(app_name='cmsmonit-rucio-account-activity-usage')

    df_rses = get_df_rses(spark)
    df_locks = get_df_locks(spark)
    df_accounts = get_df_accounts(spark)
    df_rules = get_df_rules(spark)

    locks = (
        df_locks.join(df_rses, ['rse_id'], how='left') 
            .filter(col('rse_kind') == 'prod') 
            .select(['f_name', 'f_size', 'RSE', 'rse_type', 'account_name', 'r_id']) 
            .cache()
    )

    locks_with_activity = locks.join(df_rules, ['r_id'], how='leftouter').select(['f_name', 'account_name', 'RSE', 'rse_type', 'f_size', 'activity'])

    timestamp = int(time.time())

    # A File locked by the user for two activities is accounted to both activities
    # A File locked by two users for the same activity is accounted to both Users
    user_aggreagated = (
        locks_with_activity.select(['f_name', 'f_size', 'RSE', 'rse_type', 'account_name', 'activity']) 
            .distinct() 
            .groupby(['RSE', 'rse_type', 'account_name', 'activity']) 
            .agg(_round(_sum(col('f_size')) / tb_denominator, 5).alias('total_locked')) 
            .join(df_accounts, ['account_name'], how='left') 
            .withColumnRenamed('RSE', 'rse_name') 
            .withColumn('timestamp', lit(timestamp)) 
            .select(['total_locked', 'rse_name', 'rse_type', 'account_name', 'account_type', 'activity', 'timestamp']) 
            .cache()
    )

    # Iterate over list of dicts returned from spark and push to AMQ
    total_size = 0
    for part in user_aggreagated.rdd.mapPartitions(lambda p: [[drop_nulls_in_dict(x.asDict()) for x in p]]).toLocalIterator():
        part_size = len(part)
        print(f"Length of partition: {part_size}")
        send_to_amq(data=part, confs=creds_json, batch_size=amq_batch_size, overwrite_meta_ts=True)
        total_size += part_size
        print(f"Total document size: {total_size}")

if __name__ == "__main__":
    main()
