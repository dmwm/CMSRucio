from datetime import datetime
from pyspark.sql.window import Window
from CMSSpark import schemas as cms_schemas
from pyspark.sql.types import LongType
from pyspark.sql.functions import (
    col, lower, when, from_unixtime, size,
    count as _count,
    collect_set as _collect_set,
    hex as _hex,
    split as _split
)

def get_df_rses(spark):
    """Get Spark dataframe of RSES
    """
    hdfs_rses_path = '/project/awg/cms/rucio/{}/rses/part*.avro'.format(datetime.today().strftime('%Y-%m-%d'))
    df_rses = spark.read.format("avro").load(hdfs_rses_path) \
        .filter(col('DELETED_AT').isNull()) \
        .withColumn('rse_id', lower(_hex(col('ID')))) \
        .withColumn('rse_tier', _split(col('RSE'), '_').getItem(0)) \
        .withColumn('rse_country', _split(col('RSE'), '_').getItem(1)) \
        .withColumn('rse_kind',
                    when((col("rse").endswith('Temp') | col("rse").endswith('temp') | col("rse").endswith('TEMP')),
                         'temp')
                    .when((col("rse").endswith('Test') | col("rse").endswith('test') | col("rse").endswith('TEST')),
                          'test')
                    .otherwise('prod')
                    ) \
        .select(['rse_id', 'RSE', 'RSE_TYPE', 'rse_tier', 'rse_country', 'rse_kind'])
    return df_rses

def get_df_rules(spark):
    """Get Spark dataframe of rules
    """
    hdfs_rules_path = '/project/awg/cms/rucio/{}/rules/part*.avro'.format(datetime.today().strftime('%Y-%m-%d'))
    return spark.read.format('avro').load(hdfs_rules_path) \
        .withColumnRenamed('name', 'r_name') \
        .withColumn('r_id', lower(_hex(col('ID')))) \
        .withColumn('s_id', lower(_hex(col('SUBSCRIPTION_ID')))) \
        .withColumnRenamed('ACTIVITY', 'activity') \
        .withColumnRenamed('STATE', 'rule_state') \
        .withColumnRenamed('RSE_EXPRESSION', 'rse_expression') \
        .select(['r_name','r_id', 's_id', 'activity', 'rule_state', 'rse_expression']) \
        .cache()

def get_df_subscriptions(spark):
    """Get Spark dataframe of Subscriptions
    """
    hdfs_subs_path = '/project/awg/cms/rucio/{}/subscriptions/part*.avro'.format(datetime.today().strftime('%Y-%m-%d'))
    df_subscriptions = spark.read.format("avro").load(hdfs_subs_path) \
        .withColumn('s_id', lower(_hex(col('ID')))) \
        .withColumnRenamed('NAME', 's_name') \
        .withColumnRenamed('COMMENTS', 's_comment') \
        .select(['s_id', 's_name', 's_comment'])
    return df_subscriptions

def get_df_accounts(spark):
    """Get Spark dataframe of Accounts
    """
    today = datetime.today().strftime('%Y-%m-%d')
    hdfs_rucio_accounts = f'/project/awg/cms/rucio/{today}/accounts/part*.avro'
    df_accounts = spark.read.format("avro").load(hdfs_rucio_accounts) \
        .filter(col('DELETED_AT').isNull()) \
        .withColumnRenamed('ACCOUNT', 'account_name') \
        .withColumnRenamed('ACCOUNT_TYPE', 'account_type') \
        .select(['account_name', 'account_type'])
    return df_accounts

def get_df_account_limits(spark):
    """Get Spark dataframe of Account Limits
    """
    today = datetime.today().strftime('%Y-%m-%d')
    hdfs_rucio_account_limits = f'/project/awg/cms/rucio/{today}/account_limits/part*.avro'
    df_accounts = spark.read.format("avro").load(hdfs_rucio_account_limits) \
        .withColumn('rse_id', lower(_hex(col('RSE_ID')))) \
        .withColumnRenamed('ACCOUNT', 'account_name') \
        .withColumnRenamed('BYTES', 'account_limit') \
        .select(['account_name', 'rse_id', 'account_limit'])
    return df_accounts

def get_df_locks(spark):
    """Get Spark dataframe of Locks
    """
    today = datetime.today().strftime('%Y-%m-%d')
    locks_path = f'/project/awg/cms/rucio/{today}/locks/part*.avro'
    locks = spark.read.format('avro').load(locks_path) \
                .filter(col('SCOPE') == 'cms') \
                .filter(col('STATE').isin(['O', 'R'])) \
                .withColumn('rse_id', lower(_hex(col('RSE_ID')))) \
                .withColumnRenamed('NAME', 'f_name') \
                .withColumnRenamed('ACCOUNT', 'account_name') \
                .withColumnRenamed('BYTES', 'f_size') \
                .withColumn('r_id', lower(_hex(col('RULE_ID')))) \
                .select(['rse_id', 'f_name', 'f_size', 'r_id', 'account_name'])
    return locks

def get_df_replicas(spark):
    """Create main replicas dataframe by selecting only Disk or Tape RSEs in Rucio REPLICAS table
    Columns selected:
        - f_name: file name
        - f_size_replicas: represents size of a file in REPLICAS table
        - rse_id
        - rep_accessed_at
        - rep_created_at
    """
    replicas_path = '/project/awg/cms/rucio/{}/replicas/part*.avro'.format(datetime.today().strftime('%Y-%m-%d'))
    return spark.read.format('avro').load(replicas_path) \
        .withColumn('rse_id', lower(_hex(col('RSE_ID')))) \
        .withColumn('f_size', col('BYTES').cast(LongType())) \
        .withColumnRenamed('NAME', 'f_name') \
        .withColumnRenamed('LOCK_CNT', 'lock_cnt') \
        .filter(col('SCOPE') == 'cms') \
        .select(['f_name', 'rse_id', 'f_size', 'lock_cnt']) \
        .cache()

def get_df_dbs_valid_files(spark):
    """Get Spark dataframe of valid files from DBS
    """
    TODAY = datetime.today().strftime('%Y-%m-%d')
    HDFS_DBS_FILES     = "/project/awg/cms/dbs/PROD_GLOBAL/{}/FILES/*.gz".format(TODAY)

    #Get only valid files
    return spark.read.schema(cms_schemas.schema_files()).csv(HDFS_DBS_FILES).filter(col('f_is_file_valid')==1).select("f_logical_file_name")

def get_df_lost_files_dids(spark,starting_date:datetime = None):
    """ Search for the File DIDs declared as LOST.

    Args:
        starting_date: Date used to filter values higher than
    """
    TODAY = datetime.today().strftime('%Y-%m-%d')
    HDFS_RUCIO_DIDS     = "/project/awg/cms/rucio/{}/dids/part*.avro".format(TODAY)
    if starting_date is not None:
        return spark.read.format('avro').load(HDFS_RUCIO_DIDS).filter(col('AVAILABILITY')=='L').filter(col('DID_TYPE')=='F')\
            .withColumn('UPDATED_AT', from_unixtime(col('UPDATED_AT')/1000,'yyyy-MM-dd'))\
                .filter(col('UPDATED_AT')>=starting_date).select(['NAME','BYTES','UPDATED_AT']).alias('ld')
    else:
        return spark.read.format('avro').load(HDFS_RUCIO_DIDS).filter(col('AVAILABILITY')=='L').filter(col('DID_TYPE')=='F')\
            .withColumn('UPDATED_AT', from_unixtime(col('UPDATED_AT')/1000,'yyyy-MM-dd'))\
                .select(['NAME','BYTES','UPDATED_AT']).alias('ld')


def get_df_contents(spark):
    """Get Spark dataframe of Contents association [container,dataset,filename]
    """
    TODAY = datetime.today().strftime('%Y-%m-%d')
    HDFS_RUCIO_CONTENTS = "/project/awg/cms/rucio/{}/contents/part*.avro".format(TODAY)

    df_contents = spark.read.format('avro').load(HDFS_RUCIO_CONTENTS).select(['NAME','CHILD_NAME','DID_TYPE','CHILD_TYPE'])
    df_containers = df_contents.filter(col('DID_TYPE')=='C').filter(col('CHILD_TYPE')=='D').select(['NAME','CHILD_NAME'])\
                            .withColumnRenamed('NAME','CONTAINER').withColumnRenamed('CHILD_NAME','DATASET').alias('c')
    df_datasets = df_contents.filter(col('DID_TYPE')=='D').filter(col('CHILD_TYPE')=='F').withColumnRenamed('CHILD_NAME','FILENAME').select(['FILENAME','NAME'])
    return df_containers.join(df_datasets,df_containers.DATASET == df_datasets.NAME,how='left')\
                            .select(['CONTAINER','DATASET','FILENAME']).cache()

def get_df_contents_counts(spark):
    """Get Spark dataframe of Contents association [container,dataset,filename] and the count of files and datasets per container and dataset
    """
    df_contents = get_df_contents(spark)

    #Count Datasets and Files per Container
    windowSpec = Window.partitionBy(col('DATASET'))
    df_contents = df_contents.withColumn('BLOCK_VALID_CN',_count(when(col("FILENAME").isNotNull(), 1)).over(windowSpec))
    windowSpec = Window.partitionBy(col('CONTAINER'))
    df_contents = df_contents.withColumn('DATASET_INVALID_CN',_count(when(col("FILENAME").isNull(), 1)).over(windowSpec))
    df_contents = df_contents.withColumn('DATASET_BLOCKS',size(_collect_set("DATASET").over(windowSpec)))

    return df_contents

def get_df_closed_dids(spark):
    """Get Spark dataframe of closed DIDs
    """
    TODAY = datetime.today().strftime('%Y-%m-%d')
    HDFS_RUCIO_DIDS     = "/project/awg/cms/rucio/{}/dids/part*.avro".format(TODAY)
    return spark.read.format('avro').load(HDFS_RUCIO_DIDS)\
        .filter((col('DID_TYPE')=='D') | (col('DID_TYPE')=='C'))\
        .filter(col('IS_OPEN')=='0')\
        .select(['NAME']).alias('ld')

def get_df_file_dids(spark):
    """Get Spark dataframe of DIDs
    """
    dids_path = '/project/awg/cms/rucio/{}/dids/part*.avro'.format(datetime.today().strftime('%Y-%m-%d'))
    return spark.read.format('avro').load(dids_path)\
        .filter((col('DID_TYPE')=='F'))\
        .withColumnRenamed('NAME', 'f_name') \
        .withColumnRenamed('BYTES', 'f_size') \
        .select(['f_name', 'f_size'])