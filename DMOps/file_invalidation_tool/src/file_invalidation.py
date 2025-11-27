"""
File        : file_invalidation.py
Author      : Andres Manrique <andres.manrique.ardila AT cern [DOT] ch>
              Juan Pablo Salas <juan.pablo.salas.galindo AT cern [DOT] ch>
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
import pandas as pd
from rucio.client import Client


@click.command()
@click.option('--filename', required=True, default=None, type=str,
              help='Name of the text file having the datasets names')
@click.option('--rse', required=False, default=None, type=str,
              help='RSE to look at')
@click.option('--mode', required=False, type=click.Choice(['rucio','spark']), default='rucio', type=str,
              help='List generation mode')
def invalidate_files(filename, rse, mode):
    """
    Using Rucio or Spark, and exports files containing the replicas to be declared as bad and the rules that are file levelly protected
    """
    if mode == 'rucio':
        #Start Rucio Client
        rucio_client = Client()
        
        # Read the files to delete
        with open('/input/'+filename, 'r') as file:
            files_to_delete = file.readlines()
        files_to_delete = list(map(str.strip,files_to_delete))


        # Get the list of replicas
        dict_delete = [{'scope':'cms','name':lfn} for lfn in files_to_delete]
        dict_replicas_delete = list(rucio_client.list_replicas(dids=dict_delete,all_states=True))


        #Replicas to declare as bad
        df_delete = pd.DataFrame(dict_replicas_delete)[['name','rses']]
        df_delete['rses'] = df_delete['rses'].apply(lambda dict_rses:';'.join(dict_rses.keys()) if len(dict_rses.keys())>0 else pd.NA)
        df_delete = df_delete.dropna(subset='rses')

        if rse is not None:
            df_delete = df_delete[df_delete.rses.str.contains(rse)]
            df_delete.loc[:,'rses'] = rse

        df_delete = df_delete.rename(columns={'name':'FILENAME','rses':'RSES'})
        df_delete.drop_duplicates().to_csv('/input/rucio_replicas_inv.csv',index=False)

        #Replicas to the rules
        df_rules = pd.DataFrame(columns=['subscription_id', 'rse_expression', 'source_replica_expression', 'ignore_account_limit', 'created_at', 'account', 'copies', 'activity', 'priority', 'updated_at', 'scope', 'expires_at', 'grouping', 'name', 'weight', 'notification', 'comments', 'did_type', 'locked', 'stuck_at', 'child_rule_id', 'state', 'locks_ok_cnt', 'purge_replicas', 'eol_at', 'id', 'error', 'locks_replicating_cnt', 'ignore_availability', 'split_container', 'locks_stuck_cnt', 'meta', 'bytes'])
        rules = [list(rucio_client.list_associated_rules_for_file(scope=d['scope'],name=d['name'])) for d in dict_delete]
        for r in rules:
            if df_rules.shape[0]==0 and len(r)>0:
                df_rules = pd.DataFrame(r)
            else:
                df_rules = pd.concat([df_rules,pd.DataFrame(r)],axis=0)

        if rse is not None:
            df_rules['includes_rse'] = df_rules['rse_expression'].apply(lambda exp: {'rse':rse} in list(rucio_client.list_rses(rse_expression=exp)))
            df_rules = df_rules[df_rules.includes_rse]

        #TODO Check this with rse 
        #Rules protecting the replicas at File level
        df_rules_delete = df_rules[df_rules['did_type']=='FILE']
        #RSE is exported in case it's tape and require purge_replicas
        df_rules_delete[['id','rse_expression']].rename(columns={'id':'RULE_ID','rse_expression':'RSE'}).to_csv('/input/rucio_rules_delete.csv',index=False)

        #Rules that are suspended and require to be re-evaluated
        df_rules_suspended = df_rules[df_rules['state']=='SUSPENDED']
        df_rules_suspended['id'].to_csv('/input/rucio_rules_stuck.txt',index=False, header = False)

    else:
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
        df_rules_suspended = df_rules.filter(col('RULE_STATE') == 'U')
        df_rules_suspended.select('RULE_ID').distinct().toPandas().to_csv('/input/rucio_rules_stuck.txt',index=False, header = False)


if __name__ == "__main__":
    invalidate_files()