#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File        : rucio_subscriptions.py
Author      : Panos Paparrigopoulos <panos.paparrigopoulos AT cern [DOT] ch>
Description : Sends aggregated Rucio data to monit-opensearch.
              The data send consist of Rucio subscriptions .
              Date contain following fields:
                'rse_name': The Rucio RSE
                'size_in_tb': The ammount of data locked by the Subscription
                's_name': The subscription name
                's_comment': The comment field of the subscription
                'timestamp': The time that data were generated
Inspect results: https://monit-opensearch-lt.cern.ch/dashboards/goto/8f25d8d273afc9ae03ff8a954eea18e6
"""

import time
import click as click
from pyspark.sql.functions import lit, col, round as _round, sum as _sum
from CMSSpark.spark_utils import get_spark_session
from CMSMonitoring.amq_sender import credentials, send_to_amq, drop_nulls_in_dict
from hadoop_queries import get_df_rses, get_df_rules, get_df_subscriptions, get_df_locks

@click.command()
@click.option("--creds", required=True, help="etc/secrets/amq.json")
@click.option("--amq_batch_size", type=click.INT, required=False, help="AMQ transaction batch size",
              default=100)
def main(creds, amq_batch_size):
    tb_denominator = 10 ** 12
    creds_json = credentials(f_name=creds)
    creds_json['type'] = 'subscriptions'
    spark = get_spark_session(app_name='cmsmonit-rucio-subscriptions')

    df_rses = get_df_rses(spark)
    df_rules = get_df_rules(spark)
    df_subs = get_df_subscriptions(spark)
    df_rules_subs = df_rules.join(df_subs, ['s_id'], how='inner')
    df_locks = get_df_locks(spark)
    locks = (
        df_locks.join(df_rses, ['rse_id'], how='left') 
            .filter(col('rse_kind') == 'prod') 
            .join(df_rules_subs, ['r_id'], how='inner') 
            .select(['f_name', 'f_size', 'RSE', 's_name', 's_comment']) 
            .distinct() 
            .cache()
    )

    timestamp = int(time.time())
    df = (
        locks .groupby(['RSE', 's_name', 's_comment']) 
        .agg(_round(_sum(col('f_size')) / tb_denominator, 5).alias('size_in_tb')) 
        .withColumnRenamed('RSE', 'rse_name') 
        .withColumn('timestamp', lit(timestamp)) 
        .select(['rse_name',
                'size_in_tb',
                's_name',
                'timestamp',
                's_comment']) 
        .cache()
    )

    # Iterate over list of dicts returned from spark and push to AMQ
    total_size = 0
    for part in df.rdd.mapPartitions(lambda p: [[drop_nulls_in_dict(x.asDict()) for x in p]]).toLocalIterator():
        part_size = len(part)
        print(f"Length of partition: {part_size}")
        send_to_amq(data=part, confs=creds_json, batch_size=amq_batch_size, overwrite_meta_ts=True)
        total_size += part_size
        print(f"Total document size: {total_size}")


if __name__ == "__main__":
    main()