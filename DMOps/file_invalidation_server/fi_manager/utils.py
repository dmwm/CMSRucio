from .models import FileInvalidationRequests
import tempfile
import os
import subprocess
import logging
import yaml
import uuid
import re
import time
from kubernetes import client, config
from django.core.mail import send_mail
from django.conf import settings

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
yaml_path = os.path.join(src_dir, 'controllers', 'job.yaml')

def get_cern_username(request):
        return (
            request.META.get("HTTP_X_FORWARDED_PREFERRED_USERNAME")
            or request.META.get("HTTP_X_FORWARDED_USER")
            or request.META.get("REMOTE_USER")
        )

def send_approval_alert(request_id):
    files = FileInvalidationRequests.objects.filter(request_id=request_id)

    request_user = files.first().request_user
    reason = files.first().reason
    reason = re.sub(r'(CMS(?:PROD|TRANSF|DM)\-\d+)',r'<a href="https://its.cern.ch/jira/browse/\1">\1</a>',reason)
    count = files.count()
    dry_run = files.first().dry_run
    mode = files.first().mode
    rse = files.first().rse
    contains_raw = False

    file_names_plain = []
    for f in files:
        if '/RAW/' in f.file_name:
            contains_raw = True
        file_names_plain.append(f"\t\t- {f.file_name}")

    plain_text_body = "\n".join([
        "A new file invalidation request has been submitted.",
        "",
        f"Request ID: {request_id}",
        f"Reason: {reason}",
        f"Mode: {mode}",
        f"RSE: {rse}",
        f"Dry run: {dry_run}",
        f"Requested by: {request_user}",
        f"Number of files: {count}",
        f"Contains /RAW/: {contains_raw}",
        "",
        "File names:",
        *file_names_plain,
    ])

    html_body = f"""
        <html>
        <body>
            <h2>A new file invalidation request has been submitted and awaits approval</h2>

            <p><b>Request ID:</b> {request_id}</p>
            <p><b>Reason:</b> {reason}</p>
            <p><b>Mode:</b> {mode}</p>
            <p><b>RSE:</b> {rse}</p>
            <p><b>Dry run:</b> {dry_run}</p>
            <p><b>Requested by:</b> {request_user}</p>
            <p><b>Number of files:</b> {count}</p>
            <p><b>Contains /RAW/?:</b> {contains_raw}</p>

            <p><b>File names:</b></p>
            <ul>
            {''.join(f"<li>{f.file_name}</li>" for f in files)}
            </ul>
        </body>
        </html>
        """
    

    # Send email
    send_mail(
        subject=f"[File Invalidation] A new file invalidation request has been submitted",
        message=plain_text_body,
        from_email=settings.DEFAULT_FROM_EMAIL,
        recipient_list=settings.ADMIN_EMAILS,
        html_message=html_body,
    )


def process_invalidation(request_id, reason, dry_run=True,mode='global',rse=None,to_process='approved',global_invalidate_last_replicas=False):
    file_records = FileInvalidationRequests.objects.filter(request_id=request_id,status=to_process)
    if file_records.count()==0:
        raise ValueError(f'There are no records in the db for the request_id: {request_id}')
    list_of_files = [record.file_name for record in file_records]
    txt_content = '\n'.join(list_of_files)

    if not txt_content.strip():
        raise ValueError(f'The text file is empty.')

    pvc_mount_path = "/shared-data"
    if len(os.listdir(pvc_mount_path))>0:
        for entry in os.listdir(pvc_mount_path):
            os.remove(os.path.join(pvc_mount_path, entry))
    file_path = os.path.join(pvc_mount_path, f'temp_file_{str(request_id).replace("-","")}.txt')

    with open(file_path, "w") as temp_file:
        temp_file.write(txt_content)

        logging.info(f"File saved to: {file_path}.")
        job_unique_uuid = uuid.uuid4().hex[:8]
        status = create_job_from_yaml(
            filename=f'temp_file_{str(request_id).replace("-","")}.txt',
            reason=reason,
            filepath=file_path,
            job_id=job_unique_uuid,
            dry_run=dry_run,
            mode=mode,
            rse=rse,
            global_invalidate_last_replicas=global_invalidate_last_replicas
        )        
        
        # On job creation, the files are sent to queued status
        sent_requests = FileInvalidationRequests.objects.filter(request_id=request_id)
        sent_requests.update(status=status)
        sent_requests.update(job_id=job_unique_uuid)


    if status=='in_progress':
        message = f'{len(sent_requests)}/{len(file_records)} files are being invalidated corresponding to request id #{request_id} and job id #{job_unique_uuid}.'
    else:
        message= f'File invalidation job has failed for request id #{request_id} and job id #{job_unique_uuid}'

        
    return message


def create_job_from_yaml(filename,reason,filepath,job_id,dry_run,mode,rse,global_invalidate_last_replicas):

    with open(yaml_path) as f:
        job_definition = yaml.safe_load(f)

    
    job_definition['metadata']['name'] = f"file-invalidation-job-{job_id}"

    if mode == 'global':
        arg_list = ["global","--reason", "$(REASON)", "--rucio-mode"]
    elif mode == 'local':
        arg_list = ["site-invalidation","--reason", "$(REASON)","--rse",f"{rse}","--rucio-mode"]

    if dry_run:
        arg_list.append("--dry-run")
        
    if global_invalidate_last_replicas:
        arg_list.append("--global-invalidate-last-replicas")

    job_definition['spec']['template']['spec']['containers'][0]['args'] = arg_list

    for container in job_definition['spec']['template']['spec']['containers']:
        for env in container['env']:
            if env['name'] == 'FILENAME':
                env['value'] = filename
            elif env['name'] == 'REASON':
                env['value'] = reason
            elif env['name'] == 'FILEPATH':
                env['value'] = filepath

    try:
        try:
            config.load_incluster_config()
        except config.ConfigException:
            raise Exception("Could not configure kubernetes client")
        batch_v1 = client.BatchV1Api()
        job = batch_v1.create_namespaced_job(namespace="file-invalidation-tool", body=job_definition)
        logging.info(f"Job {job.metadata.name} created successfully.")
        return 'in_progress'
    except client.exceptions.ApiException as e:
        logging.error(f"Error creating job: {e}")
        return 'failed'

def wait_for_job_completion(job_id, timeout=1000):
    job_name = f"file-invalidation-job-{job_id}"
    batch_v1 = client.BatchV1Api()
    start_time = time.time()
    while time.time() - start_time < timeout:
        job = batch_v1.read_namespaced_job(name=job_name,namespace='file-invalidation-tool')
        conditions = job.status.conditions
        if conditions:
            for condition in conditions:
                if condition.type == "Complete" and condition.status == "True":
                    return True
                if condition.type == "Failed" and condition.status == "True":
                    raise RuntimeError(f"Job {job_name} failed")
        time.sleep(2)
    raise TimeoutError(f"Timeout waiting for job {job_name} to complete")



def get_pod_logs(job_id):
    core_v1 = client.CoreV1Api()
    job_name = f'file-invalidation-job-{job_id}'
    label_selector = f"job-name={job_name}"

    wait_for_job_completion(job_id)
    
    namespace = 'file-invalidation-tool'
    pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)

    for pod in pods.items:
        pod_name = pod.metadata.name
        logging.info(f"Fetching logs for pod: {pod_name}")
        
        try:
            pod_logs = core_v1.read_namespaced_pod_log(name=pod_name, namespace=namespace)
            logging.info(f"Logs from pod {pod_name}:\n{pod_logs}")
        except client.exceptions.ApiException as e:
            logging.error(f"Error fetching logs for pod {pod_name}: {e}")