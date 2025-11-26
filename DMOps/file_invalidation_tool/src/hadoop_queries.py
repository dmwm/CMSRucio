from datetime import datetime, timedelta
from pyspark.sql.functions import (
    col, lower, from_unixtime,
    hex as _hex
)

#Hadoop Queries Functions
def get_df_rse_locks(spark):
    TODAY = datetime.today().strftime('%Y-%m-%d') if datetime.now().hour >= 6 else (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    HDFS_RUCIO_RSES     = "/project/awg/cms/rucio/{}/rses/part*.avro".format(TODAY)
    HDFS_RUCIO_LOCKS    = "/project/awg/cms/rucio/{}/locks/part*.avro".format(TODAY)

    df_rses = spark.read.format('avro').load(HDFS_RUCIO_RSES)\
    .withColumn('ID', lower(_hex(col('ID'))))\
    .select(['ID','RSE'])

    df_locks = spark.read.format('avro').load(HDFS_RUCIO_LOCKS)\
                .withColumn('RULE_ID', lower(_hex(col('RULE_ID'))))\
                .withColumn('RSE_ID', lower(_hex(col('RSE_ID'))))\
                .select(['NAME','RSE_ID','RULE_ID','STATE','BYTES'])
    return df_locks.join(df_rses, df_locks.RSE_ID==df_rses.ID, how='inner').withColumnRenamed('STATE','LOCK_STATE')

def get_df_rse_replicas(spark,rse=None):
    TODAY = datetime.today().strftime('%Y-%m-%d') if datetime.now().hour >= 6 else (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    HDFS_RUCIO_RSES     = "/project/awg/cms/rucio/{}/rses/part*.avro".format(TODAY)
    HDFS_RUCIO_REPLICAS = "/project/awg/cms/rucio/{}/replicas/part*.avro".format(TODAY)

    df_rses = None
    if rse is None:
        df_rses = spark.read.format('avro').load(HDFS_RUCIO_RSES)\
        .withColumn('ID', lower(_hex(col('ID'))))\
        .select(['ID','RSE'])
    else:
        df_rses = spark.read.format('avro').load(HDFS_RUCIO_RSES)\
        .withColumn('ID', lower(_hex(col('ID'))))\
        .filter(col('RSE')==rse)\
        .select(['ID','RSE'])

    df_replicas = spark.read.format('avro').load(HDFS_RUCIO_REPLICAS)\
    .withColumn('RSE_ID', lower(_hex(col('RSE_ID'))))\
    .withColumn('TOMBSTONE',from_unixtime(col('TOMBSTONE')/1000,'yyyy-MM-dd'))\
    .select(['RSE_ID', 'NAME', 'BYTES','STATE','TOMBSTONE']).withColumnRenamed('STATE','REPLICA_STATE')
    return df_replicas.join(df_rses, df_replicas.RSE_ID==df_rses.ID, how='inner').select(['RSE_ID', 'NAME', 'BYTES','REPLICA_STATE','TOMBSTONE','RSE']).alias('r')

def get_df_contents(spark):
    TODAY = datetime.today().strftime('%Y-%m-%d') if datetime.now().hour >= 6 else (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    HDFS_RUCIO_CONTENTS = "/project/awg/cms/rucio/{}/contents/part*.avro".format(TODAY)

    df_contents = spark.read.format('avro').load(HDFS_RUCIO_CONTENTS).select(['NAME','CHILD_NAME','DID_TYPE','CHILD_TYPE'])
    df_containers = df_contents.filter(col('DID_TYPE')=='C').filter(col('CHILD_TYPE')=='D').select(['NAME','CHILD_NAME'])\
                            .withColumnRenamed('NAME','CONTAINER').withColumnRenamed('CHILD_NAME','DATASET').alias('c')
    df_datasets = df_contents.filter(col('DID_TYPE')=='D').filter(col('CHILD_TYPE')=='F').withColumnRenamed('CHILD_NAME','FILENAME').select(['FILENAME','NAME'])
    return df_containers.join(df_datasets,df_containers.DATASET == df_datasets.NAME,how='left')\
                            .select(['CONTAINER','DATASET','FILENAME'])

def get_df_dataset_level_rules(spark):
    TODAY = datetime.today().strftime('%Y-%m-%d') if datetime.now().hour >= 6 else (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    HDFS_RUCIO_RULES = "/project/awg/cms/rucio/{}/rules/part*.avro".format(TODAY)
    #Get the rules for datasets and files
    df_rules = spark.read.format('avro').load(HDFS_RUCIO_RULES).withColumn('ID', lower(_hex(col('ID'))))\
        .filter((col('DID_TYPE')=='D') | (col('DID_TYPE')=='F')).select(['ID','NAME'])
    df_rules.cache()
    return df_rules

def get_df_rules(spark):
    TODAY = datetime.today().strftime('%Y-%m-%d') if datetime.now().hour >= 6 else (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    HDFS_RUCIO_RULES = "/project/awg/cms/rucio/{}/rules/part*.avro".format(TODAY)
    #Get the rules for datasets and files
    df_rules = spark.read.format('avro').load(HDFS_RUCIO_RULES).withColumn('ID', lower(_hex(col('ID'))))\
        .withColumnRenamed('STATE','RULE_STATE').select(['ID','NAME','RULE_STATE', 'DID_TYPE'])
    df_rules.cache()
    return df_rules

def get_df_file_level_rules(spark):
    TODAY = datetime.today().strftime('%Y-%m-%d') if datetime.now().hour >= 6 else (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    HDFS_RUCIO_RULES = "/project/awg/cms/rucio/{}/rules/part*.avro".format(TODAY)
    #Get the rules for datasets and files
    df_rules = spark.read.format('avro').load(HDFS_RUCIO_RULES).withColumn('ID', lower(_hex(col('ID'))))\
        .filter(col('DID_TYPE')=='F').select(['ID','NAME'])
    df_rules.cache()
    return df_rules