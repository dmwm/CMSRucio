import json
import os
import re
import shlex
import time
from datetime import datetime
from glob import glob

import pandas as pd
from jira import JIRA
from kubernetes import client, config

POLL_INTERVAL = int(os.environ.get('POLL_INTERVAL', 60))   # seconds between status checks
JOB_TIMEOUT   = int(os.environ.get('JOB_TIMEOUT', 3600)) # seconds before we give up waiting

def spawn_worker_jobs(overview_path: str):
    config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()

    # Namespace and pod name are injected automatically by k8s via downward API
    namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()
    pod_name = os.environ["POD_NAME"]

    # Read own pod spec to reuse image and volumes
    pod = core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)
    current_container = pod.spec.containers[0]
    image = current_container.image
    volumes = pod.spec.volumes
    volume_mounts = current_container.volume_mounts

    with open(overview_path) as f:
        failures = json.load(f)["failures"]

    complete_overview = pd.read_json(overview_path.replace('.json','_complete.json'), orient='records')

    spawned = []  # list of dicts: {job_name, rse, error}

    for failure in failures:
        rse = failure["rse"]
        error = failure["error"]
        raw_name = f"susp-prod-{datetime.today().strftime('%Y-%m-%d')}-{error}-{rse}".lower()
        job_name = ''.join(c if c.isalnum() or c == '-' else '-' for c in raw_name)
        job_name = job_name.strip('-')[:63]
        worker_args = [error, "--input-file", "/shared/locks_suspended_rules.csv", "--rse", rse, "--suspended"]
        handler_cmd = "python3 /src/run_handler.py " + " ".join(shlex.quote(a) for a in worker_args)


        job = client.V1Job(
            metadata=client.V1ObjectMeta(name=job_name, namespace=namespace,labels={"spawned-by":"susp-prod-handler","spawned-on":f"{datetime.today().strftime('%Y-%m-%d')}"}),
            spec=client.V1JobSpec(
                ttl_seconds_after_finished=14*24*60*60,#Deletes jobs after two weeks
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        restart_policy="Never",
                        service_account_name="job-spawner-sa",
                        volumes=volumes,          # ← reused directly from own spec
                        containers=[client.V1Container(
                            name="worker",
                            image=image,          # ← reused directly from own spec
                            command=["/bin/sh", "-c"],
                            args=[f"source /src/setup_rucio.sh && {handler_cmd}"],
                            volume_mounts=volume_mounts,  # ← reused directly from own spec
                            env_from=[
                                client.V1EnvFromSource(
                                    secret_ref=client.V1SecretEnvSource(name="handler-tool-secrets")
                                )
                            ],
                        )]
                    )
                )
            )
        )
        try:
            batch_v1.create_namespaced_job(namespace=namespace, body=job)
            spawned.append({"job_name": job_name, "rse": rse, "error": error})
            print(f"Spawned job: {job_name}")
        except Exception as e:
            print(f"Could not create job {job_name} for rse {rse} with error {error}, {str(e)}")

    results = wait_for_jobs(batch_v1, core_v1, namespace, spawned)
    #create_jira_ticket(complete_overview,results)

def wait_for_jobs(batch_v1, core_v1, namespace: str, spawned: list) -> list:
    """
    Poll until every job in `spawned` reaches a terminal state.
    Returns a list of result dicts with status, duration, etc.
    """
    pending   = {s["job_name"]: s for s in spawned}
    results   = []
    start     = time.time()
    start_ts  = datetime.utcnow()

    print(f"Waiting for {len(pending)} jobs to complete...")

    while pending:
        if time.time() - start > JOB_TIMEOUT:
            for job_name, meta in pending.items():
                results.append({**meta, "status": "timeout", "duration_s": JOB_TIMEOUT})
            print("Timed out waiting for remaining jobs.")
            break

        time.sleep(POLL_INTERVAL)

        for job_name in list(pending.keys()):
            try:
                job = batch_v1.read_namespaced_job(name=job_name, namespace=namespace)
                controller_uid = job.spec.selector.match_labels.get("batch.kubernetes.io/controller-uid") or job.spec.selector.match_labels.get("controller-uid")
                label_selector = f"controller-uid={controller_uid}"
            except client.exceptions.ApiException as e:
                print(f"Could not read job {job_name}: {e}")
                continue

            conds = job.status.conditions or []

            is_complete = any(c.type == "Complete" and c.status == "True" for c in conds)
            is_failed   = any(c.type == "Failed"   and c.status == "True" for c in conds)

            if is_complete or is_failed:
                elapsed = int(time.time() - start)
                status  = "success" if is_complete else "failed"
                description = get_job_last_log_line(core_v1, namespace, label_selector, job_name) if is_complete else ""
                results.append({**pending[job_name], "status": status, "duration_s": elapsed, "description":description})
                print(f"  {job_name}: {status} after {elapsed}s")
                del pending[job_name]

    return results

def create_jira_ticket(complete_overview: pd.DataFrame, results: list):
    succeeded = [r for r in results if r["status"] == "success"]
    failed    = [r for r in results if r["status"] == "failed"]
    timed_out = [r for r in results if r["status"] == "timeout"]

    run_date = datetime.utcnow().strftime("%Y-%m-%d")

    summary = f"Stuck Rule Handler Summary — {run_date}"

    lines = [
        f"*Run date:* {run_date}",
        f"*Total jobs:* {len(results)}  |  *Succeeded:* {len(succeeded)}  |  *Failed:* {len(failed)}  |  *Timed out:* {len(timed_out)}",
        "",
    ]

    if succeeded:
        lines.append("*✅ Succeeded:*")
        lines.append("")
        lines.append("||RSE||Error type||Description||")
        for r in succeeded:
            succeeded_line = f"|{r['rse']}|{r['error']}|{r['description']}|"
            lines.append(succeeded_line)

    if failed:
        lines.append("")
        lines.append("*❌ Failed:*")
        lines.append("")
        lines.append("||RSE||Error type||")
        for r in failed:
            lines.append(f"|{r['rse']}|{r['error']}|")

    if timed_out:
        lines.append("")
        lines.append("*⏱ Timed out:*")
        lines.append("")
        lines.append("||RSE||Error type||")
        for r in timed_out:
            lines.append(f"|{r['rse']}|{r['error']}|")

    description = "\n".join(lines)

    complete_overview['error_desc']=complete_overview['error_desc'].str.replace('\n','')
    complete_overview['file_size']=complete_overview['file_size']*1000
    complete_overview = complete_overview.rename(columns={'file_size':'file_size_tb','rule_size':'rule_size_pb'})
    complete_overview_lines = complete_overview.to_markdown(index=False,floatfmt=".2f").split('\n')
    header = complete_overview_lines[0].replace('|', '||')
    complete_overview = '\n'.join([header] + complete_overview_lines[2:])

    description = "*Overview of currently SUSPENDED rules:*\n"+complete_overview+'\n *Summary of rule handler tool jobs:* '+description

    jira_client = JIRA(server="https://its.cern.ch/jira/",token_auth=os.environ['JIRA_API_TOKEN'])

    new_issue = jira_client.create_issue(project=os.environ['JIRA_PROJECT_KEY'], summary=summary,
                              description=description, issuetype={'name': 'Task'})


def get_job_last_log_line(core_v1, namespace: str, label_selector: str, job_name: str):
    files = glob(f'/shared/*{job_name}*.log')
    log_file = files[0] if len(files)>0 else None

    try:
        with open(log_file,'r') as f:
            logs = f.readlines()
        lines = [l.strip() for l in logs if l.strip()]
        last_line = lines[-1] if lines else None
        pattern = r'INFO \[[^\:]+: (.+)'
        description_search = re.search(pattern, last_line) if last_line else False
        description = description_search.group(1) if description_search else last_line
        return description
    except Exception as e:
        print(f"Could not fetch logs for job {job_name}: {e}")
        return None
    finally:
        if log_file is not None:
            os.remove(log_file)

if __name__ == "__main__":
    spawn_worker_jobs(overview_path='./overview.json')