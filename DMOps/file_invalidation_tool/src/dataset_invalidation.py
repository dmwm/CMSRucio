"""
File        : dataset_invalidation.py
Author      : Andres Manrique <andres.manrique.ardila AT cern [DOT] ch>
              Juan Pablo Salas <juan.pablo.salas.galindo AT cern [DOT] ch>
Description : Receive a list of datasets desired to be invalidated in Rucio and DBS.
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
from rucio.client import Client
import pandas as pd
from CMSSpark.spark_utils import get_spark_session
from pyspark.sql.functions import col, collect_list, concat_ws
from hadoop_queries import get_df_rse_locks, get_df_rse_replicas, get_df_contents, get_df_dataset_level_rules
from pyspark.sql.window import Window
from file_invalidation import includes_rse_safe

@click.command()
@click.option('--filename', required=True, default=None, type=str,
              help='Name of the text file having the datasets names')
@click.option('--rse', required=False, default=None, type=str,
              help='RSE to look at')
@click.option('--mode', required=False, type=click.Choice(['rucio','spark']), default='rucio',help='List generation mode')
def invalidate_datasets(filename,rse, mode):
    #TODO: Check rse option
    if mode=='rucio':
        #Start Rucio Client
        rucio_client = Client()
        
        # Read the datasets to delete
        with open('/input/'+filename, 'r') as file:
            datasets_to_delete = file.readlines()
        datasets_to_delete = list(map(str.strip,datasets_to_delete))

        # Get the list of datasets
        dict_delete = [{'scope':'cms','name':lfn} for lfn in datasets_to_delete]

        # Files to invalidate on DBS
        df_dataset_files = pd.DataFrame(list(rucio_client.bulk_list_files(dict_delete)))
        df_dataset_files[['name']].rename(columns={'name':'FILENAME'}).drop_duplicates().to_csv('/input/dbs_files_inv.txt',index=False, header = False)

        #Replicas to declare as bad
        file_list = [{'scope':'cms','name':name} for name in df_dataset_files['name']]
        df_replicas = pd.DataFrame(list(rucio_client.list_replicas(file_list,all_states=True)))[['name','states']]
        df_replicas['rses']=df_replicas['states'].apply(dict.keys)
        df_replicas['rses']=df_replicas['rses'].apply(lambda s: ';'.join(s))

        if rse is not None:
            df_replicas = df_replicas[df_replicas.rses.str.contains(rse)]
            df_replicas.loc[:,'rses'] = rse

        df_replicas[['name','rses']].rename(columns={'name':'FILENAME','rses':'RSES'}).drop_duplicates().to_csv('/input/rucio_replicas_inv.csv',index=False)

        #Rules to erase
        #RSE is exported in case it's tape and require purge_replicas
        info_rules = rucio_client.get_locks_for_dids(dict_delete)
        if len(info_rules)>0:
            df_rules = pd.DataFrame(info_rules)
        else:
            df_rules = pd.DataFrame(columns=['rse','rule_id'])

        if rse is not None:
            df_rules['includes_rse'] = df_rules['rse_expression'].apply(lambda exp: includes_rse_safe(rucio_client,exp, rse))
            df_rules = df_rules.loc[df_rules.includes_rse]

        df_rules.columns = df_rules.columns.str.upper()
        df_rules[['RSE','RULE_ID']].drop_duplicates().to_csv('/input/rucio_rules_delete.csv',index=False)
    else:
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