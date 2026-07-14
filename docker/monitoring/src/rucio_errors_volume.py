"""
File        : rucio_requests_locks_rules_count_volume.py
Author      : Christos Emmanouil <christos.emmanouil AT cern [DOT] ch>
Description : Sends aggregated Rucio data to es-cms.
"""

import re
import time
from datetime import datetime
from itertools import chain
import click as click
from CMSSpark.osearch import osearch
from CMSSpark.spark_utils import get_spark_session
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
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
                'source_rse':        {"type": "keyword"},
                'destination_rse':   {"type": "keyword"},
                'rules_state':       {"type": "keyword"},
                'error':             {"type": "keyword"},
                'files_count':       {"type": "integer"},
                'total_bytes':       {"type": "long"},
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
    
def normalize_err_msg(msg):
    if msg is None:
        return None

    file_ext_pattern = r'\.(root|file\d{1,5})'

    # --- PFN and URL normalization ---
    msg = re.sub(r'https://[^\s,\)\]]+?' + file_ext_pattern, '<PFN-HTTPS>', msg, flags=re.IGNORECASE)
    msg = re.sub(r'davs://[^\s,\)\]]+?' + file_ext_pattern, '<PFN-DAVS>', msg, flags=re.IGNORECASE)
    msg = re.sub(r'https://[^\s,\)\]]+', '<URL-HTTPS>', msg, flags=re.IGNORECASE)
    msg = re.sub(r'davs://[^\s,\)\]]+', '<URL-DAVS>', msg, flags=re.IGNORECASE)
    msg = re.sub(r'(?<!https:)(?<!davs:)(?<!//)/[^\s,\)\]]+' + file_ext_pattern, '<LFN>', msg)
    msg = re.sub(r'path=/[^\s,\)\]]+', 'path=<root_dir>+<LFN>', msg, flags=re.IGNORECASE)

    # --- DN and certificate chain normalization ---
    msg = re.sub(r'(CN|O|OU|DC|ST|C|L)=[^,\n\)\:]+', '<DN-COMP>', msg)
    msg = re.sub(r'subjects? DN [^:\n]+', 'subject: <DN>', msg, flags=re.IGNORECASE)
    msg = re.sub(r'certificate with subjects? DN [^:\n]+', 'certificate with subject: <DN>', msg, flags=re.IGNORECASE)
    msg = re.sub(r'problematic certificate subject: [^:\n]+', 'problematic certificate subject: <DN>', msg, flags=re.IGNORECASE)
    msg = re.sub(r'issuer[^:\n]+', 'issuer: <DN>', msg, flags=re.IGNORECASE)
    msg = re.sub(r'<DN-COMP>(?:,\s*<DN-COMP>)+', '<DN>', msg)

    # --- Bracketed content: IPs ---
    msg = re.sub(r'\[(?:\d{1,3}\.){3}\d{1,3}\]', '[<IP>]', msg)
    msg = re.sub(r'\[((?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4})\]', '[<IP6>]', msg)

    # --- Hostnames and IPs ---
    msg = re.sub(r'\b([a-zA-Z0-9\-]+\.)+[a-zA-Z]{2,10}\b', '<HOST>', msg)
    msg = re.sub(r'(?<!\[)(?:\d{1,3}\.){3}\d{1,3}(?!\])', '<IP>', msg)
    msg = re.sub(r'\b(?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}\b', '<IP6>', msg)

    # --- Checksum/content mismatch ---
    msg = re.sub(r'\([0-9a-fA-F]{8}\s*!=\s*[0-9a-fA-F]{8}\)', '(abcd != efgh)', msg)
    msg = re.sub(r'\(expected:\s*\d+;\s*received:\s*\d+\)', '(expected: ...; received: ...)', msg)
    msg = re.sub(r'\(expected=\[[^\]]+\],\s*actual=\[[^\]]+\]\)', '(expected=[...], actual=[...])', msg)

    # --- Timeout details ---
    msg = re.sub(r'Timeout deadline: \d+ MILLISECONDS, actual: \d+ MILLISECONDS', 'Timeout deadline: [...] MILLISECONDS, actual: [...] MILLISECONDS', msg)

    # --- REST diagnostics ---
    msg = re.sub(r'\[net=[^,\]]*,\s*protocol=[^,\]]*,\s*store=[^,\]]*(?:,[^,\]]*)*\]', '[REST-INFO]', msg)

    # --- SSL/curl/HTTP errors ---
    msg = re.sub(r'ssl=0x[0-9a-fA-F]+', 'ssl=<SSL>', msg)
    msg = re.sub(r'curl error \((\d+)\)', r'curl error (\1)', msg)
    msg = re.sub(r'HTTP (?=\d{3})', 'HTTP ', msg)
    msg = re.sub(r'code=\d+', 'code=<CODE>', msg)

    # --- Ports, messages, redirects ---
    msg = re.sub(r':\d{2,5}\b', ':<PORT>', msg)
    msg = re.sub(r'>[\w\-\.]+@[\w\-\.]+<', '><POOL_HOST><', msg)
    msg = re.sub(r'at >[\w\-\.]+<', 'at ><POOL_DOMAIN><', msg)
    msg = re.sub(r'\[?>[\w\-\.]+@[\w\-\.]+<\]?', '[<POOL_HOST>]', msg)
    msg = re.sub(r'message\s+<[-]?\d+:[-]?\d+>', 'message <MSG-ID>', msg)
    msg = re.sub(r'redirects? \[.*?\]', 'redirects [<REDACTED>]', msg)

    # --- Traffic, timeouts, sizes ---
    msg = re.sub(r'<HOST>/<IP>\d+', '<HOST>/<IP>', msg)
    msg = re.sub(r'received \d+ B \([^)]+\) of data', 'received <N_BYTES> of data', msg)
    msg = re.sub(r'received \d+ B of data', 'received <N_BYTES> of data', msg)
    msg = re.sub(r'\(\d+(\.\d+)? (MiB|GiB)\)', '(<SIZE>)', msg)
    msg = re.sub(r'copy timeout of \d+s', 'copy timeout of <SECONDS>s', msg)

    # --- UUIDs and tokens ---
    msg = re.sub(r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b', '<REQUEST-ID>', msg)
    msg = re.sub(r'\b[0-9a-f]{24,}\b', '<TOKEN>', msg)

    # --- Better masking for incomplete LFN tails ---
    msg = re.sub(r'(/[^\s]+){3,}(/[^\s]*)?', '<INCOMPLETE-LFN>', msg)

    # --- Replace numeric time durations like "120 seconds" ---
    msg = re.sub(r'\b\d+\s+(seconds|second|secs|sec)\b', '<TIME>', msg, flags=re.IGNORECASE)

    # --- More generic size masking for expected/received values ---
    msg = re.sub(r'expected:\s*\d{7,};\s*received:\s*\d{7,}', 'expected: <N_BYTES>; received: <N_BYTES>', msg)
    msg = re.sub(r'\(expected:\s*\d{7,};\s*received:\s*\d{7,}', '(expected: <N_BYTES>; received: <N_BYTES>', msg)

    # --- NEW: inline pool hosts ---
    msg = re.sub(r'\b[\w\-]+@[\w\-]+Domain\b', '<POOL_HOST>', msg)

    # --- NEW: fix unmatched redirects with missing close-paren ---
    msg = re.sub(r'\(([^)]+?)redirects \[<REDACTED>\]', r'(\1redirects [<REDACTED>])', msg)

    # --- Final structural cleanup ---
    msg = re.sub(r'\s+', ' ', msg).strip()
    msg = re.sub(r'[\s,:;]+\)', ')', msg)
    msg = re.sub(r'\(<DN>\)', '<DN>', msg)
    msg = re.sub(r'<DN>[,:\s]+<DN>', '<DN>', msg)

    return msg
normalize_err_msg_udf = F.udf(normalize_err_msg, StringType())

@click.command()
@click.option('--es_host', required=True, default=None, type=str, help='OpenSearch host name without port: es-cms1.cern.ch/es')
@click.option('--es_secret_file', required=True, default=None, type=str, help='OpenSearch secret file that contains "user:pass" only')
@click.option('--es_index', required=True, default=None, type=str, help='OpenSearch index template (prefix), i.e.: "test-wmarchive-agent-count"')
def main(es_host, es_secret_file, es_index):
    
    spark = get_spark_session(app_name='cmsmonit-rucio-error-volume')
    timestamp = int(time.time())
    
    # ------------------------------
    # Indicate the HDFS Dumps of the Day
    TODAY                    = datetime.today().strftime('%Y-%m-%d')
    HDFS_RUCIO_RSES          = "/project/awg/cms/rucio/{}/rses/part*.avro".format(TODAY)
    HDFS_RUCIO_RULES         = "/project/awg/cms/rucio/{}/rules/part*.avro".format(TODAY)
    HDFS_RUCIO_LOCKS         = "/project/awg/cms/rucio/{}/locks/part*.avro".format(TODAY)
    HDFS_RUCIO_REQUESTS_HIST = "/project/awg/cms/rucio/{}/requests_history/part*.avro".format(TODAY)

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
        .filter(F.col('SCOPE') == 'cms')
        .withColumn('RULE_ID', F.lower(F.hex(F.col('ID'))))
        .withColumnRenamed('STATE', 'RULE_STATE')
        .select(['RULE_ID', 'ACCOUNT', 'RULE_STATE', 'ACTIVITY'])
        .cache()
    )

    df_locks = (
        spark.read.format('avro')
        .load(HDFS_RUCIO_LOCKS)
        .filter(F.col('SCOPE') == 'cms')
        .withColumn('RULE_ID', F.lower(F.hex(F.col('RULE_ID'))))
        .withColumn('RSE_ID', F.lower(F.hex(F.col('RSE_ID'))))
        .withColumnRenamed('STATE', 'LOCK_STATE')
        .withColumnRenamed('BYTES', 'LOCK_BYTES')
        .withColumnRenamed('NAME', 'FILE_NAME')
        .filter(F.col('LOCK_STATE') != 'O')
        .select(['RULE_ID', 'RSE_ID', 'FILE_NAME', 'LOCK_BYTES'])
        .cache()
    )

    rules_with_their_not_ok_locks = (
        df_rules
        .join(df_locks, on='RULE_ID', how='inner')
    )

    window_spec = Window.partitionBy('RULE_ID', 'FILE_NAME', 'DESTINATION_RSE_ID').orderBy(F.col('UPDATED_AT').desc())

    df_requests = (
        spark.read.format('avro')
        .load(HDFS_RUCIO_REQUESTS_HIST)
        .filter(F.col('SCOPE') == 'cms')
        .withColumnRenamed('NAME', 'FILE_NAME')
        .withColumn('RULE_ID', F.lower(F.hex(F.col('RULE_ID'))))
        .withColumn('RSE_ID', F.lower(F.hex(F.col('DEST_RSE_ID'))))
        .withColumn('SOURCE_RSE_ID', F.lower(F.hex(F.col('SOURCE_RSE_ID'))))
        .withColumn('DESTINATION_RSE_ID', F.lower(F.hex(F.col('DEST_RSE_ID'))))
        .withColumnRenamed('STATE', 'REQUEST_STATE')
        .withColumn('UPDATED_AT', F.to_timestamp(F.from_unixtime(F.col('UPDATED_AT') / 1000, 'yyyy-MM-dd'), 'yyyy-MM-dd'))
        .withColumn('row_num', F.row_number().over(window_spec))
        .filter(F.col('row_num') == 1)
        .drop('row_num')
        .filter((F.col("ERR_MSG").isNotNull()) & (F.col("ERR_MSG") != ""))
        .withColumn("ERR_MSG", normalize_err_msg_udf(F.col("ERR_MSG")))
        .select(['FILE_NAME', 'DESTINATION_RSE_ID', 'RSE_ID', 'SOURCE_RSE_ID', 'RULE_ID', 'ERR_MSG'])
        .join(rules_with_their_not_ok_locks, on=['RULE_ID', 'RSE_ID', 'FILE_NAME'], how='inner')
        .join(df_rses.alias("src"), F.col("SOURCE_RSE_ID") == F.col("src.RSE_ID"), how="left")
        .join(df_rses.alias("dest"), F.col("DESTINATION_RSE_ID") == F.col("dest.RSE_ID"), how="left")
        .select([
            'FILE_NAME',
            'ACCOUNT',
            'ACTIVITY',
            F.col('src.RSE').alias('SOURCE_RSE'),
            F.col('dest.RSE').alias('DESTINATION_RSE'),
            'LOCK_BYTES',
            'RULE_STATE',
            'ERR_MSG'
        ])
        .cache()
    )


    # ------------------------------
    # Calculate the aggregated outputs
    RuleState = {
        'R': "REPLICATING",
        'O': "OK",
        'S': "STUCK",
        'U': "SUSPENDED",
        'W': "WAITING_APPROVAL",
        'I': "INJECT"
    }
    mapping_rule_state = F.create_map(*chain.from_iterable([(F.lit(k), F.lit(v)) for k, v in RuleState.items()]))

    aggregated_results = (
        df_requests
        .groupBy(
            F.col("ACCOUNT").alias("account"),
            F.col("ACTIVITY").alias("activity"),
            F.col("SOURCE_RSE").alias("source_rse"),
            F.col("DESTINATION_RSE").alias("destination_rse"),
            F.col("RULE_STATE").alias("rules_state"),
            F.col("ERR_MSG").alias("error"),
        )
        .agg(
            F.count("*").alias("files_count"),
            F.sum("LOCK_BYTES").alias("total_bytes"),
        )
        .withColumn("rules_state", mapping_rule_state[F.col("rules_state")])
        .withColumn('timestamp', F.lit(timestamp))
    )
    
    
    # ------------------------------
    # Send results
    for part in aggregated_results.rdd.mapPartitions(lambda p: [[drop_nulls_in_dict(x.asDict()) for x in p]]).toLocalIterator():
        send(part, opensearch_host=es_host, es_secret_file=es_secret_file, es_index_template=es_index)

if __name__ == "__main__":
    main()