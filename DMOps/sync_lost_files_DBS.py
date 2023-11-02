from datetime import datetime, timedelta
from CMSSpark.spark_utils import get_spark_session
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, from_unixtime, to_date, size, when,
    count as _count,
    collect_set as _collect_set
)

def main():
    spark = get_spark_session(app_name='sync_rucio_lost_files')

    TODAY = datetime.today().strftime('%Y-%m-%d')

    # https://github.com/dmwm/CMSKubernetes/tree/master/docker/sqoop/scripts
    HDFS_RUCIO_CONTENTS = "/project/awg/cms/rucio/{}/contents/part*.avro".format(TODAY)
    HDFS_RUCIO_DIDS     = "/project/awg/cms/rucio/{}/dids/part*.avro".format(TODAY)
    HDFS_DBS_FILES     = "/project/awg/cms/dbs/PROD_GLOBAL/{}/FILES/*.gz".format(TODAY)

    #Add schema to avoid problems with the header
    schema = StructType([
            StructField("f_file_id", LongType(), True),
            StructField("f_logical_file_name", StringType(), True),
            StructField("f_is_file_valid", IntegerType(), True),
            StructField("f_dataset_id", LongType(), True),
            StructField("f_block_id", LongType(), True),
            StructField("f_file_type_id", IntegerType(), True),
            StructField("f_check_sum", StringType(), True),
            StructField("f_event_count", LongType(), True),
            StructField("f_file_size", DoubleType(), True),
            StructField("f_branch_hash_id", IntegerType(), True),
            StructField("f_adler32", StringType(), True),
            StructField("f_md5", StringType(), True),
            StructField("f_auto_cross_section", DoubleType(), True),
            StructField("f_creation_date", DoubleType(), True),
            StructField("f_create_by", StringType(), True),
            StructField("f_last_modification_date", DoubleType(), True),
            StructField("f_last_modified_by", StringType(), True)
        ])

    #Get only valid files
    df_dbs = spark.read.schema(schema).csv(HDFS_DBS_FILES).filter(col('f_is_file_valid')==1).select("f_logical_file_name")

    #Get files declared as lost
    #Declare files as lost script runs once a week, so keep the same time window
    LAST_WEEK = (datetime.today() - timedelta(days=7))
    df_lost_dids = spark.read.format('avro').load(HDFS_RUCIO_DIDS).filter(col('AVAILABILITY')=='L').filter(col('DID_TYPE')=='F')\
        .withColumn('UPDATED_AT',to_date(from_unixtime(col('UPDATED_AT')/1000,'yyyy-MM-dd'),'yyyy-MM-dd'))\
            .filter(col('UPDATED_AT')>=LAST_WEEK).select(['NAME']).alias('ld')

    #Get only the files that require invalidation on DBS
    df_lost_dids = df_lost_dids.join(df_dbs,df_dbs.f_logical_file_name == df_lost_dids.NAME,how='inner').select('ld.*')
    df_lost_dids.toPandas().to_csv('dbs_files_inv.txt',header=False,index=False)

    #Get containers and datasets
    df_contents = spark.read.format('avro').load(HDFS_RUCIO_CONTENTS).select(['NAME','CHILD_NAME','DID_TYPE'])
    df_containers = df_contents.filter(col('DID_TYPE')=='C').filter(col('CHILD_NAME').contains('#')).select(['NAME','CHILD_NAME']).withColumnRenamed('NAME','CONTAINER').withColumnRenamed('CHILD_NAME','DATASET').alias('c')
    df_datasets = df_contents.filter(col('DID_TYPE')=='D').withColumnRenamed('CHILD_NAME','FILENAME').select(['FILENAME','NAME'])
    df_contents = df_containers.join(df_datasets,df_containers.DATASET == df_datasets.NAME,how='left').select(['CONTAINER','DATASET','FILENAME'])

    #Count Datasets and Files per Container
    windowSpec = Window.partitionBy(col('DATASET'))
    df_contents = df_contents.withColumn('BLOCK_REC',_count(when(col("FILENAME").isNull(), 1)).over(windowSpec))
    windowSpec = Window.partitionBy(col('CONTAINER'))
    df_contents = df_contents.withColumn('CONT_REC',_count(when(col("FILENAME").isNull(), 1)).over(windowSpec))
    df_contents = df_contents.withColumn('CONT_DAT',size(_collect_set("DATASET").over(windowSpec)))
    df_contents.cache()

    #Get only the containers that require invalidation on rucio and dbs
    #block_rec = 1 means that the dataset only has 1 record. Filename is null means that the dataset has no files records
    df_contents.filter(col('BLOCK_REC')==0).toPandas().to_csv('/temp/datasets_inv.txt',header=False,index=False)
    df_contents.filter(col('CONT_REC')==col('CONT_DAT')).toPandas().to_csv('/temp/containers_inv.txt',header=False,index=False)

if __name__ == "__main__":
    main()