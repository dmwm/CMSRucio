import os
import django
import sys
import re
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'file_invalidation_server.settings')
django.setup()

from kubernetes import client, config
from fi_manager.models import FileInvalidationRequests  
import logging

logging.basicConfig(level=logging.INFO,format='(%(asctime)s) [%(name)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def fetch_and_process():
    config.load_incluster_config()  
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()

    namespace = 'file-invalidation-tool'
    jobs = batch_v1.list_namespaced_job(namespace=namespace)

    for job in jobs.items:
        job_name = job.metadata.name
        if not job.status.conditions:
            continue

        if job.kind == "CronJob" or ('jobs-log-processor' in job_name) or ('approval-message' in job_name):
            continue

        condition_types = {cond.type: cond.status for cond in job.status.conditions}
        if condition_types.get("Failed") == "True":
            logger.warning(f"Job {job_name} failed")
        elif condition_types.get("Complete") == "True":
            # Get pods created by this job
            pods = core_v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"job-name={job_name}"
            )

            # Use most recent pod
            try:
                latest_pod = sorted(
                    pods.items,
                    key=lambda pod: pod.status.start_time or pod.metadata.creation_timestamp,
                    reverse=True
                )[0]
            except IndexError as e:
                logger.error(f"There are no pods under the {job_name} job name.")
                continue


            pod_name = latest_pod.metadata.name
            logger.info(f"Pod name: {pod_name}")
            logs = core_v1.read_namespaced_pod_log(pod_name, namespace=namespace)
            try:
                rucio_invalidated_dids, dbs_invalidated_dids, dry_run = parse_job_logs(logs)
                logger.info(logs)
                logger.info(f"Job {job_name} has invalidated {len(rucio_invalidated_dids)} dids on Rucio and {len(dbs_invalidated_dids)} dids on DBS.")
                logger.info(f"Job {job_name} has invalidated the following DIDs on Rucio: {rucio_invalidated_dids}")
                logger.info(f"Job {job_name} has invalidated the following DIDs on DBS: {dbs_invalidated_dids}")
                if (len(rucio_invalidated_dids)>0) or (len(dbs_invalidated_dids)>0):
                    update_database(job_name, rucio_invalidated_dids, dbs_invalidated_dids, dry_run)
                    logger.info(f"Job {job_name} has completed and the DIDs have updated.")
                else:
                    raise Exception(f"Job {job_name} did not invalidate any DIDs on Rucio or DBS.")
            except Exception as e:
                logger.error(f"Job {pod_name} has failed with error: {e}")
                update_database_for_failed_job(pod_name,f'Job {pod_name} has failed with error: {str(e)}\n{logs}')



            delete_opts = client.V1DeleteOptions(propagation_policy='Foreground')

            batch_v1.delete_namespaced_job(
                    name=job_name,
                    namespace=namespace,
                       body=delete_opts)

def parse_job_logs(logs: str):
    if "Error running shell script" in logs:
        raise Exception(f"Job has failed with error: Error running shell script")
    dry_run = 'Would declare file' in logs

    if dry_run:
        rucio_invalidated_files = re.findall(pattern='(?:Would declare file) (\/[\w\/\-]+.root) as bad at',
                                                string=logs)

        dbs_invalidated_files = re.findall(pattern='(?:Would invalidate file on DBS:) (\/[\w\/\-]+.root)\s',
                                                string=logs)
    else:
        rucio_invalidated_files = re.findall(pattern='(?:Declared file) (\/[\w\/\-]+.root) as bad at',
                                                string=logs)

        dbs_invalidated_files = re.findall(pattern='(?:Invalidation OK for file:) (\/[\w\/\-]+.root)\s',
                                                string=logs)
        
        dbs_invalidated_dataset = re.findall(pattern='(?:Invalidation OK for dataset:) (\/[\w\/\-]+.root)\s',
                                                string=logs)

    if dbs_invalidated_dataset:
        # Assumes that for datasets, DBS dataset invalidation implies Rucio file invalidation
        dbs_invalidated_files = dbs_invalidated_files.append(dbs_invalidated_dataset)
        rucio_invalidated_files = rucio_invalidated_files.append(dbs_invalidated_dataset)

    return rucio_invalidated_files, dbs_invalidated_files, dry_run

def update_database(job_name, rucio_list, dbs_list, dry_run):
    globally_invalidated_dids = set(rucio_list) & set(dbs_list)
    job_id = re.findall(pattern='file-invalidation-job-(\w{8})',string=job_name)[0]
    
    job_files = FileInvalidationRequests.objects.filter(job_id=job_id)

    only_rucio_invalidated = FileInvalidationRequests.objects.filter(job_id=job_id,file_name__in=rucio_list)
    only_rucio_invalidated.update(status='success',mode='rucio_only',dry_run=dry_run)

    only_dbs_invalidated = FileInvalidationRequests.objects.filter(job_id=job_id,file_name__in=dbs_list)
    only_dbs_invalidated.update(status='success',mode='dbs_only',dry_run=dry_run)

    global_invalidated = FileInvalidationRequests.objects.filter(job_id=job_id,file_name__in=globally_invalidated_dids)
    global_invalidated.update(status='success',mode='global',dry_run=dry_run)

def update_database_for_failed_job(job_name,logs):
    job_id = re.findall(pattern='file-invalidation-job-(\w{8})-\w',string=job_name)[0]
    failed_invalidation = FileInvalidationRequests.objects.filter(job_id=job_id)
    failed_invalidation.update(status='failed',logs=logs)

if __name__ == "__main__":
    fetch_and_process()