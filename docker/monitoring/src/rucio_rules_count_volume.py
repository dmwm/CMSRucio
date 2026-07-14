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
                "account": {"type": "keyword"},
                "activity": {"type": "keyword"},
                "state": {"type": "keyword"},
                "source_replica_expression": {"type": "keyword"},
                "rse_expression": {"type": "keyword"},
                "rules_count": {"type": "integer"},
                "rules_total_bytes": {"type": "long"},
                "locks_count_OK": {"type": "integer"},
                "locks_bytes_OK": {"type": "long"},
                "locks_count_REPLICATING": {"type": "integer"},
                "locks_bytes_REPLICATING": {"type": "long"},
                "locks_count_STUCK": {"type": "integer"},
                "locks_bytes_STUCK": {"type": "long"},
                "timestamp": {"format": "epoch_second", "type": "date"},
            }
        },
    }


def prepare_data_for_opensearch(data_dict):
    """
    Processes a dictionary to replace None/nulls in 'source_replica_expression'
    with 'NotSpecified', and drops None values for other fields.
    """
    processed_dict = {}
    for k, v in data_dict.items():
        if k == "source_replica_expression":
            processed_dict[k] = v if v is not None else "NotSpecified"
        else:
            if v is not None:
                processed_dict[k] = v
    return processed_dict


def send(part, opensearch_host, es_secret_file, es_index_template):
    """
    Send given data to OpenSearch
    """
    client = osearch.get_es_client(
        opensearch_host, es_secret_file, get_index_schema(es_index_template)
    )

    # Monthly index format: index_mod="M"
    idx = client.get_or_create_index(
        timestamp=time.time(), index_template=es_index_template, index_mod="M"
    )
    client.send(idx, part, metadata=None, batch_size=10000, drop_nulls=False)


@click.command()
@click.option(
    "--es_host",
    required=True,
    default=None,
    type=str,
    help="OpenSearch host name without port: es-cms1.cern.ch/es",
)
@click.option(
    "--es_secret_file",
    required=True,
    default=None,
    type=str,
    help='OpenSearch secret file that contains "user:pass" only',
)
@click.option(
    "--es_index",
    required=True,
    default=None,
    type=str,
    help='OpenSearch index template (prefix), i.e.: "test-wmarchive-agent-count"',
)
def main(es_host, es_secret_file, es_index):

    spark = get_spark_session(app_name="cmsmonit-rucio-rules-count-volume")
    timestamp = int(time.time())

    # ------------------------------
    # Indicate the HDFS Dumps of the Day
    TODAY = datetime.today().strftime("%Y-%m-%d")
    HDFS_RUCIO_CONTENTS = "/project/awg/cms/rucio/{}/contents/part*.avro".format(TODAY)
    HDFS_RUCIO_DIDS = "/project/awg/cms/rucio/{}/dids/part*.avro".format(TODAY)
    HDFS_RUCIO_RULES = "/project/awg/cms/rucio/{}/rules/part*.avro".format(TODAY)
    HDFS_RUCIO_LOCKS = "/project/awg/cms/rucio/{}/locks/part*.avro".format(TODAY)

    # ------------------------------
    # Load rules dataframe
    df_rules = (
        spark.read.format("avro")
        .load(HDFS_RUCIO_RULES)
        .withColumn("RULE_ID", F.lower(F.hex(F.col("ID"))))
        .withColumnRenamed("NAME", "DID_NAME")
        .withColumnRenamed("STATE", "RULE_STATE")
        .select(
            [
                "RULE_ID",
                "ACCOUNT",
                "DID_NAME",
                "RULE_STATE",
                "SOURCE_REPLICA_EXPRESSION",
                "RSE_EXPRESSION",
                "ACTIVITY",
            ]
        )
        .cache()
    )

    # ------------------------------
    # Since rules can be created for open containers and datasets
    # Using DIDs table directly to compute the volume is not possible
    # We have to go through the contents table to compute the current volume

    df_dids = (
        spark.read.format("avro")
        .load(HDFS_RUCIO_DIDS)
        .select("NAME", "DID_TYPE", "BYTES")
        .distinct()
        .cache()
    )

    df_contents = (
        spark.read.format("avro")
        .load(HDFS_RUCIO_CONTENTS)
        .select("NAME", "DID_TYPE", "CHILD_NAME", "CHILD_TYPE")
        .distinct()
        .cache()
    )

    df_files = (
        df_dids.filter((F.col("DID_TYPE") == "F"))
        .withColumnRenamed("NAME", "FILE")
        .select("FILE", "BYTES")
    )

    df_dataset_to_file = (
        df_contents.filter(F.col("DID_TYPE") == "D")
        .filter(F.col("CHILD_TYPE") == "F")
        .select(F.col("NAME").alias("DATASET"), F.col("CHILD_NAME").alias("FILE"))
    )

    df_container_to_dataset = (
        df_contents.filter(F.col("DID_TYPE") == "C")
        .filter(F.col("CHILD_TYPE") == "D")
        .select(F.col("NAME").alias("CONTAINER"), F.col("CHILD_NAME").alias("DATASET"))
    )

    # Attach DID type to rules
    df_rules_with_type = df_rules.join(
        df_dids, df_rules.DID_NAME == df_dids.NAME, "left"
    ).select(df_rules["*"], df_dids["DID_TYPE"])

    # ------------------------------
    # Rules directly on FILEs
    df_rule_file = (
        df_rules_with_type.filter(F.col("DID_TYPE") == "F")
        .join(df_files, df_rules_with_type.DID_NAME == df_files.FILE, "left")
        .select("RULE_ID", "DID_NAME", "BYTES")
    )

    # ------------------------------
    # Rules on DATASETs → FILEs
    df_rule_dataset = (
        df_rules_with_type.filter(F.col("DID_TYPE") == "D")
        .join(
            df_dataset_to_file,
            df_rules_with_type.DID_NAME == df_dataset_to_file.DATASET,
            "left",
        )
        .join(df_files, df_dataset_to_file.FILE == df_files.FILE, "left")
        .select("RULE_ID", df_rules_with_type.DID_NAME.alias("DID_NAME"), "BYTES")
    )

    # ------------------------------
    # Rules on CONTAINERs → (Datasets → Files), including nested containers

    # Nested container-to-dataset: CONTAINER → SUBCONTAINER → DATASET
    df_container_to_container = df_contents.filter(
        (F.col("DID_TYPE") == "C") & (F.col("CHILD_TYPE") == "C")
    ).select(
        F.col("NAME").alias("CONTAINER"), F.col("CHILD_NAME").alias("SUBCONTAINER")
    )

    df_subcontainer_to_dataset = (
        df_container_to_container.alias("parent")
        .join(
            df_container_to_dataset.alias("child"),
            F.col("parent.SUBCONTAINER") == F.col("child.CONTAINER"),
            "inner",
        )
        .select(
            F.col("parent.CONTAINER").alias("PARENT_CONTAINER"), F.col("child.DATASET")
        )
    )

    # Merge both levels
    df_all_container_to_dataset = (
        df_container_to_dataset.select("CONTAINER", "DATASET")
        .union(
            df_subcontainer_to_dataset.select(
                F.col("PARENT_CONTAINER").alias("CONTAINER"), "DATASET"
            )
        )
        .distinct()
    )

    # Resolve CONTAINER rules
    df_rule_container = (
        df_rules_with_type.filter(F.col("DID_TYPE") == "C")
        .join(
            df_all_container_to_dataset,
            df_rules_with_type.DID_NAME == df_all_container_to_dataset.CONTAINER,
            "left",
        )
        .join(df_dataset_to_file, "DATASET", "left")
        .join(df_files, "FILE", "left")
        .select("RULE_ID", df_rules_with_type.DID_NAME.alias("DID_NAME"), "BYTES")
    )

    # ------------------------------
    # Union all and aggregate
    df_all_rule_bytes = (
        df_rule_file.union(df_rule_dataset)
        .union(df_rule_container)
        .groupBy("RULE_ID", "DID_NAME")
        .agg(F.sum("BYTES").alias("TOTAL_BYTES"))
    )

    # Join back with rule metadata
    df_rule_with_volume = (
        df_rules.join(df_all_rule_bytes, on="RULE_ID", how="left")
        .select(df_rules["*"], "TOTAL_BYTES")
        .cache()
    )

    # ------------------------------
    # Union with locks
    df_locks = (
        spark.read.format("avro")
        .load(HDFS_RUCIO_LOCKS)
        .withColumn("RULE_ID", F.lower(F.hex(F.col("RULE_ID"))))
        .withColumnRenamed("STATE", "LOCK_STATE")
        .withColumnRenamed("BYTES", "LOCK_BYTES")
        .select(["RULE_ID", "LOCK_BYTES", "LOCK_STATE"])
        .groupBy(F.col("RULE_ID"), F.col("LOCK_STATE"))
        .agg(
            F.count("*").alias("LOCKS_COUNT"),
            F.sum("LOCK_BYTES").alias("TOTAL_LOCKS_BYTES"),
        )
    )

    df_locks_pivoted = (
        df_locks.groupBy("RULE_ID")
        .pivot("LOCK_STATE", ["O", "R", "S"])
        .agg(
            F.first("LOCKS_COUNT").alias("COUNT"),
            F.first("TOTAL_LOCKS_BYTES").alias("BYTES"),
        )
    )

    df_rule_with_volume_and_locks = df_rule_with_volume.join(
        df_locks_pivoted, on="RULE_ID", how="left"
    )

    # ------------------------------
    # Calculate the aggregated outputs
    RuleState = {
        "R": "REPLICATING",
        "O": "OK",
        "S": "STUCK",
        "U": "SUSPENDED",
        "W": "WAITING_APPROVAL",
        "I": "INJECT",
    }
    mapping_rule_state = F.create_map(
        *chain.from_iterable([(F.lit(k), F.lit(v)) for k, v in RuleState.items()])
    )

    aggregated_results = (
        df_rule_with_volume_and_locks.groupBy(
            F.col("ACCOUNT").alias("account"),
            F.col("ACTIVITY").alias("activity"),
            F.col("RULE_STATE").alias("state"),
            F.col("SOURCE_REPLICA_EXPRESSION").alias("source_replica_expression"),
            F.col("RSE_EXPRESSION").alias("rse_expression"),
        )
        .agg(
            F.count("*").alias("rules_count"),
            F.sum("TOTAL_BYTES").alias("rules_total_bytes"),
            F.sum("O_COUNT").alias("locks_count_OK"),
            F.sum("O_BYTES").alias("locks_bytes_OK"),
            F.sum("R_COUNT").alias("locks_count_REPLICATING"),
            F.sum("R_BYTES").alias("locks_bytes_REPLICATING"),
            F.sum("S_COUNT").alias("locks_count_STUCK"),
            F.sum("S_BYTES").alias("locks_bytes_STUCK"),
        )
        .withColumn("state", mapping_rule_state[F.col("state")])
        .withColumn("timestamp", F.lit(timestamp))
    )

    # ------------------------------
    # Send results
    for part in aggregated_results.rdd.mapPartitions(
        lambda p: [[prepare_data_for_opensearch(x.asDict()) for x in p]]
    ).toLocalIterator():
        send(
            part,
            opensearch_host=es_host,
            es_secret_file=es_secret_file,
            es_index_template=es_index,
        )


if __name__ == "__main__":
    main()