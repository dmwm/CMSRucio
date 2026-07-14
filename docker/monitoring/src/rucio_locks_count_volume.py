"""
File        : rucio_requests_locks_rules_count_volume.py
Author      : Christos Emmanouil <christos.emmanouil AT cern [DOT] ch>
Description : Sends aggregated Rucio data to es-cms.
"""

import time
from datetime import datetime
from itertools import chain
import click as click
from CMSSpark.osearch import osearch
from CMSSpark.spark_utils import get_spark_session
from pyspark.sql.window import Window
from pyspark.sql import functions as F


def get_index_schema(index):
    """
    Creates mapping dictionary for the unified-logs monthly index
    """
    
    return {
        "settings": {"index": {"number_of_shards": "1", "number_of_replicas": "1"}},
        "mappings": {
            "properties": {
                'account':           {"type": "keyword"},
                'activity':          {"type": "keyword"},
                'locks_state':       {"type": "keyword"},
                'dest_rse':          {"type": "keyword"},
                'locks_count':       {"type": "integer"},
                'locks_total_bytes': {"type": "long"},
                "timestamp":         {"format": "epoch_second", "type": "date"}
            }
        }
    }

def drop_nulls_in_dict(d):
    """
    Drops the dict key if the value is None
    ES mapping does not allow None values and drops the document completely.
    """
    return {k: v for k, v in d.items() if v is not None}  # dict

def send(part, opensearch_host, es_secret_file, es_index_template):
    """
    Send given data to OpenSearch
    """
    client = osearch.get_es_client(opensearch_host, es_secret_file, get_index_schema(es_index_template))
    
    # Monthly index format: index_mod="M"
    idx = client.get_or_create_index(timestamp=time.time(), index_template=es_index_template, index_mod="M")
    client.send(idx, part, metadata=None, batch_size=10000, drop_nulls=False)

@click.command()
@click.option('--es_host', required=True, default=None, type=str, help='OpenSearch host name without port: es-cms1.cern.ch/es')
@click.option('--es_secret_file', required=True, default=None, type=str, help='OpenSearch secret file that contains "user:pass" only')
@click.option('--es_index', required=True, default=None, type=str, help='OpenSearch index template (prefix), i.e.: "test-wmarchive-agent-count"')
def main(es_host, es_secret_file, es_index):
    
    spark = get_spark_session(app_name='cmsmonit-rucio-locks-count-volume')
    timestamp = int(time.time())
    
    # ------------------------------
    # Indicate the HDFS Dumps of the Day
    TODAY            = datetime.today().strftime('%Y-%m-%d')
    HDFS_RUCIO_RSES  = "/project/awg/cms/rucio/{}/rses/part*.avro".format(TODAY)
    HDFS_RUCIO_LOCKS = "/project/awg/cms/rucio/{}/locks/part*.avro".format(TODAY)
    HDFS_RUCIO_RULES = "/project/awg/cms/rucio/{}/rules/part*.avro".format(TODAY)


    # ------------------------------
    # Load dataframes
    df_rses = (
        spark.read.format('avro')
        .load(HDFS_RUCIO_RSES)
        .withColumn('RSE_ID', F.lower(F.hex(F.col('ID'))))
        .select(['RSE', 'RSE_ID'])
        .cache()
    )

    df_rules = (
        spark.read.format('avro')
        .load(HDFS_RUCIO_RULES)
        .withColumn('RULE_ID', F.lower(F.hex(F.col('ID'))))
        .select(['RULE_ID', 'ACTIVITY'])
        .cache()
    )

    df_locks = (
        spark.read.format('avro')
        .load(HDFS_RUCIO_LOCKS)
        .withColumnRenamed('NAME', 'FILE_NAME')
        .withColumn('RSE_ID', F.lower(F.hex(F.col('RSE_ID'))))
        .withColumn('RULE_ID', F.lower(F.hex(F.col('RULE_ID'))))
        .withColumnRenamed('STATE', 'LOCK_STATE')
        .select(['FILE_NAME', 'RSE_ID', 'RULE_ID', 'ACCOUNT', 'BYTES', 'LOCK_STATE'])
    )

    df_locks_with_rse_activity = (
        df_locks
        .join(df_rses, df_locks.RSE_ID == df_rses.RSE_ID)
        .join(df_rules, df_locks.RULE_ID == df_rules.RULE_ID)
        .select(df_locks["*"], df_rses["RSE"], df_rules["ACTIVITY"])
        .cache()
    )


    # ------------------------------
    # Calculate the aggregated outputs
    LockState = {
        'R': "REPLICATING",
        'O': "OK",
        'S': "STUCK",    
    }
    mapping_lock_state = F.create_map(*chain.from_iterable([(F.lit(k), F.lit(v)) for k, v in LockState.items()]))

    aggregated_results = (
        df_locks_with_rse_activity
        .groupBy(
            F.col("ACCOUNT").alias("account"), 
            F.col("ACTIVITY").alias("activity"),
            F.col("LOCK_STATE").alias("locks_state"),
            F.col("RSE").alias("dest_rse")
        ).agg(
            F.count("*").alias("locks_count"),
            F.sum("BYTES").alias("locks_total_bytes")
        )
        .orderBy(F.desc("locks_total_bytes"))
        .withColumn("locks_total_bytes", F.when(F.col("locks_total_bytes").isNull(), 0).otherwise(F.col("locks_total_bytes")))
        .withColumn("locks_state", mapping_lock_state[F.col("locks_state")])
        .withColumn('timestamp', F.lit(timestamp))
    )
    
    
    # ------------------------------
    # Send results
    for part in aggregated_results.rdd.mapPartitions(lambda p: [[drop_nulls_in_dict(x.asDict()) for x in p]]).toLocalIterator():
        send(part, opensearch_host=es_host, es_secret_file=es_secret_file, es_index_template=es_index)

if __name__ == "__main__":
    main()