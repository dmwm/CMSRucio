"""
File        : file_invalidation_spark.py
Author      : Andres Manrique <andres.manrique.ardila AT cern [DOT] ch>
Description : Receive a list of files desired to be invalidated in Rucio and DBS.
              Generate files containing the replicas to be declared as bad and the rules that are file levelly protected
Generated files: rucio_replicas_inv.csv - List of replicas to be declared as bad in Rucio
                    "FILENAME": The name of the file to be declared as bad
                    "RSE": Site where the file is stored
                 rucio_rules_stuck.txt - List of rules that are suspended in Rucio and required to be re-evaluated
                    "RULE_ID": The ID of the rule to be re-evaluated
                 rucio_rules_delete.csv - List of file level rules to be deleted from Rucio
                    "RULE_ID": The ID of the rule to be deleted from Rucio
"""
import click as click
from CMSSpark.spark_utils import get_spark_session
from hadoop_queries import get_df_rse_locks, get_df_rse_replicas, get_df_contents, get_df_rules
from pyspark.sql.functions import col, collect_list, concat_ws
from pyspark.sql.window import Window

@click.command()
@click.option('--filename', required=True, default=None, type=str,
              help='Name of the text file having the datasets names')
@click.option('--rse', required=False, default=None, type=str,
              help='RSE to look at')
def invalidate_files(filename, rse):
    """
    Using Spark, and exports files containing the replicas to be declared as bad and the rules that are file levelly protected
    """
    spark = get_spark_session(app_name='global_file_invalidation')

    #Read the files to delete
    df_delete = spark.read.text(filename)
    df_delete = df_delete.withColumnRenamed('value','FILENAME')

    #Get the basic df
    df_locks = get_df_rse_locks(spark)
    df_replicas = get_df_rse_replicas(spark,rse)
    df_contents = get_df_contents(spark).alias('co')
    df_rules = get_df_rules(spark).alias('ru')

    #Get the content of the containers to delete (content includes filename, dataset and container)
    df_delete = df_delete.join(df_contents,df_delete.FILENAME==df_contents.FILENAME,how='inner').select(['co.*']).alias('de')

    #Replicas to declare as bad
    df_delete = df_delete.join(df_replicas,df_delete.FILENAME==df_replicas.NAME,how='left').select(['de.*','RSE','REPLICA_STATE']).alias('de')

    windowSpec = Window.partitionBy('FILENAME')
    df_delete.withColumn("RSES", collect_list(col("RSE")).over(windowSpec)) \
        .select(['FILENAME','RSES']).withColumn("RSES", concat_ws(";", "RSES")).distinct().toPandas().to_csv('/input/rucio_replicas_inv.csv',index=False)

    # Attach the replicas to the rules
    df_delete = df_delete.join(df_locks,(df_delete.FILENAME==df_locks.NAME) & (df_delete.RSE == df_locks.RSE),how='left')\
        .withColumnRenamed('RULE_ID','LOCK_RULE_ID').select(['de.*','LOCK_RULE_ID']).alias('de')
    df_rules = df_delete.join(df_rules,  df_delete.LOCK_RULE_ID ==  df_rules.ID, how='left')\
            .withColumnRenamed('ID','RULE_ID')\
            .select(['de.*', 'RULE_ID', 'RULE_STATE', 'DID_TYPE']).alias('de')
    df_rules.cache()

    #Rules protecting the replicas at File level
    df_rules_delete = df_rules.filter(col('DID_TYPE') == 'F')
    #RSE is exported in case it's tape and require purge_replicas
    df_rules_delete.select(['RULE_ID','RSE']).distinct().toPandas().to_csv('/input/rucio_rules_delete.csv',index=False)

    #Rules that are suspended and require to be re-evaluated
    df_rules_suspended = df_rules.filter(col('RULE_STATE') == 'S')
    df_rules_suspended.select('RULE_ID').distinct().toPandas().to_csv('/input/rucio_rules_stuck.txt',index=False, header = False)


if __name__ == "__main__":
    invalidate_files()