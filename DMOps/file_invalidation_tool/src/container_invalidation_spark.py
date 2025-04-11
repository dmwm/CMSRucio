from pyspark.sql.functions import col, collect_list, concat_ws
import click as click
from CMSSpark.spark_utils import get_spark_session
from hadoop_queries import get_df_rse_locks, get_df_rse_replicas, get_df_contents
from pyspark.sql.window import Window

@click.command()
@click.option('--filename', required=True, default=None, type=str,
              help='Name of the text file having the datasets names')
@click.option('--rse', required=False, default=None, type=str,
              help='RSE to look at')
def invalidate_containers(filename,rse):
    spark = get_spark_session(app_name='global_containers_invalidation')

    #Read the containers to delete
    filename = f'/user/dmtops/{filename}'
    df_delete = spark.read.text(filename)
    df_delete = df_delete.withColumnRenamed('value','CONTAINER')

    #Get the basic df
    df_locks = get_df_rse_locks(spark)
    df_replicas = get_df_rse_replicas(spark,rse)
    df_contents = get_df_contents(spark).alias('co')

    #Get the content of the containers to delete (content includes filename, dataset and container)
    df_delete = df_delete.join(df_contents,df_delete.CONTAINER==df_contents.CONTAINER,how='inner').select(['co.*']).alias('de')

    #Replicas to declare as bad
    df_delete = df_delete.join(df_replicas,df_delete.FILENAME==df_replicas.NAME,how='inner').select(['de.*','RSE','REPLICA_STATE']).alias('de')

    #Rules protecting the replicas
    df_delete = df_delete.join(df_locks,(df_delete.FILENAME==df_locks.NAME) & (df_delete.RSE == df_locks.RSE),how='left').select(['de.*','RULE_ID']).alias('de')
    df_delete.cache()

    #Files to invalidate on DBS
    df_delete.select('FILENAME').distinct().toPandas().to_csv('/input/dbs_files_inv.txt',index=False, header = False)

    windowSpec = Window.partitionBy('FILENAME')
    df_delete.withColumn("RSES", collect_list(col("RSE")).over(windowSpec)) \
        .select(['FILENAME','RSES']).withColumn("RSES", concat_ws(";", "RSES")).distinct().toPandas().to_csv('/input/rucio_replicas_inv.csv',index=False)

    #Replicas to erase from Rucio
    df_delete.select('DATASET').distinct().toPandas().to_csv('/input/datasets_inv.txt',index=False,header=False)

    #RSE is exported in case it's tape and require purge_replicas
    df_delete.filter(col('RULE_ID').isNotNull()).select(['RULE_ID','RSE']).distinct()\
        .toPandas().to_csv('/input/rucio_rules_delete.csv',index=False)

if __name__ == "__main__":
    invalidate_containers()