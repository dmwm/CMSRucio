import click as click
from CMSSpark.spark_utils import get_spark_session
from pyspark.sql.functions import col, collect_list, concat_ws
from hadoop_queries import get_df_rse_locks, get_df_rse_replicas, get_df_contents, get_df_dataset_level_rules
from pyspark.sql.window import Window

@click.command()
@click.option('--filename', required=True, default=None, type=str,
              help='Name of the text file having the datasets names')
@click.option('--rse', required=False, default=None, type=str,
              help='RSE to look at')
def invalidate_datasets(filename,rse):
    spark = get_spark_session(app_name='global_dataset_invalidation')

    #Read the containers to delete
    filename = f'/user/dmtops/{filename}'
    df_delete = spark.read.text(filename)
    df_delete = df_delete.withColumnRenamed('value','DATASET')

    #Get the basic df
    df_locks = get_df_rse_locks(spark)
    df_replicas = get_df_rse_replicas(spark,rse)
    df_contents = get_df_contents(spark).alias('co')
    df_rules = get_df_dataset_level_rules(spark).alias('ru')

    #Get the content of the datasets to delete (content includes filename, dataset and container)
    df_delete = df_delete.join(df_contents,df_delete.DATASET==df_contents.DATASET,how='inner').select(['co.*']).alias('de')

    #Replicas to declare as bad
    df_delete = df_delete.join(df_replicas,df_delete.FILENAME==df_replicas.NAME,how='inner').select(['de.*','RSE','REPLICA_STATE']).alias('de')

    #Rules protecting the replicas
    df_delete = df_delete.join(df_locks,(df_delete.FILENAME==df_locks.NAME) & (df_delete.RSE == df_locks.RSE),how='left')\
        .withColumnRenamed('RULE_ID','LOCK_RULE_ID').select(['de.*','LOCK_RULE_ID'])

    #Rules protecting the datasets  or  children files
    df_delete = df_delete.join(df_rules,  df_delete.LOCK_RULE_ID ==  df_rules.ID, how='left')\
            .withColumnRenamed('ID','RULE_ID').select(['de.*', 'RULE_ID']).alias('de')
    df_delete.cache()

    #Files to invalidate on DBS
    df_delete.select('FILENAME').distinct().toPandas().to_csv('/input/dbs_files_inv.txt',index=False, header = False)

    #Replicas to invalidate on Rucio
    windowSpec = Window.partitionBy('FILENAME')
    df_delete.withColumn("RSES", collect_list(col("RSE")).over(windowSpec)) \
        .select(['FILENAME','RSES']).withColumn("RSES", concat_ws(";", "RSES")).distinct().toPandas().to_csv('/input/rucio_replicas_inv.csv',index=False)

    #RSE is exported in case it's tape and require purge_replicas
    df_delete.filter(col('RULE_ID').isNotNull()).select(['RULE_ID','RSE']).distinct()\
        .toPandas().to_csv('/input/rucio_rules_delete.csv',index=False)

if __name__ == "__main__":
    invalidate_datasets()