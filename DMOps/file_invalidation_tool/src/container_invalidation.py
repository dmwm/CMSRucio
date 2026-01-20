import click as click
from rucio.client import Client
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, collect_list, concat_ws
from CMSSpark.spark_utils import get_spark_session
from hadoop_queries import get_df_rse_locks, get_df_rse_replicas, get_df_contents
from pyspark.sql.window import Window


@click.command()
@click.option('--filename', required=True, default=None, type=str,
              help='Name of the text file having the datasets names')
@click.option('--rse', required=False, default=None, type=str,
              help='RSE to look at')
@click.option('--mode', required=False, type=click.Choice(['rucio','spark']), default='rucio', help='List generation mode')
def invalidate_containers(filename,rse, mode):
    #TODO: Check rse option

    if mode=='rucio':
        #Start Rucio Client
        rucio_client = Client()
        
        # Read the containers to delete
        with open('/input/'+filename, 'r') as file:
            containers_to_delete = file.readlines()
        containers_to_delete = list(map(str.strip,containers_to_delete))

        # Get the list of containers
        dict_delete = [{'scope':'cms','name':lfn} for lfn in containers_to_delete]

        #Get the content of the containers to delete (content includes filename, dataset and container)
        container_files = list(rucio_client.bulk_list_files(dict_delete))

        #Files to invalidate on DBS
        df_container_files = pd.DataFrame(columns=['name','parent_name'])
        df_container_files = pd.concat([df_container_files,pd.DataFrame(container_files)])[['name','parent_name']].rename(columns={'parent_name':'CONTAINER','name':'FILENAME'})
        df_container_files['FILENAME'].drop_duplicates().to_csv('/input/dbs_files_inv.txt',index=False, header = False)

        #Replicas to declare as bad
        file_list = [{'scope':'cms','name':name} for name in df_container_files['FILENAME']]
        df_replicas = pd.DataFrame(columns=['name','states'])
        if len(file_list)>0:
            for arr in np.array_split(file_list,indices_or_sections=int(len(file_list)/1000)+1):
                list_replicas_i = list(rucio_client.list_replicas(dids=arr.tolist(),all_states=True))
                df_replicas_i = pd.DataFrame(list_replicas_i)[['name','states']]
                df_replicas = pd.concat([df_replicas,df_replicas_i])
        
        df_replicas['rses']=df_replicas['states'].apply(dict.keys)
        df_replicas['rses']=df_replicas['rses'].apply(lambda s: ';'.join(s))

        if rse is not None:
            df_replicas = df_replicas[df_replicas.rses.str.contains(rse)]
            df_replicas.loc[:,'rses'] = rse

        df_replicas[['name','rses']].rename(columns={'name':'FILENAME','rses':'RSES'}).drop_duplicates().to_csv('/input/rucio_replicas_inv.csv',index=False)

        #Datasets to erase from Rucio
        info_dataset = rucio_client.get_locks_for_dids(dict_delete)
        if len(info_dataset)>0:
            df_datasets = pd.DataFrame(info_dataset)
            if rse is not None:
                df_datasets = df_datasets[df_datasets['rse']==rse]
        else:
            df_datasets = pd.DataFrame(columns=['name'])
        df_datasets[['name']].rename(columns={'name':'DATASET'}).drop_duplicates().to_csv('/input/datasets_inv.txt',index=False,header=False)

        #Rules to erase
        #RSE is exported in case it's tape and require purge_replicas
        dataset_list = [{'scope':'cms','name':n,'type':'dataset'} for n in df_datasets['name'].drop_duplicates().values]
        df_rules = pd.DataFrame(columns=['rse','rule_id'])
        if len(dataset_list)>0:
            for arr in np.array_split(dataset_list,indices_or_sections=int(len(dataset_list)/400)+1):
                info_rules_i = rucio_client.get_locks_for_dids(arr.tolist()) 
                if len(info_rules_i)>0:
                    df_rules_i = pd.DataFrame(info_rules_i)[['rse','rule_id']]
                    df_rules = pd.concat([df_rules,df_rules_i])

        if rse is not None:
            df_rules['includes_rse'] = df_rules['rse_expression'].apply(lambda exp: {'rse':rse} in list(rucio_client.list_rses(rse_expression=exp)))
            df_rules = df_rules.loc[df_rules.includes_rse]

        df_rules.columns = df_rules.columns.str.upper()
        df_rules[['RULE_ID','RSE']].drop_duplicates().to_csv('/input/rucio_rules_delete.csv',index=False)
    else:
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