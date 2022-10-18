'''
This file contains functions for creating Airflow tasks to convert from a time range to a ledger range.
'''
from datetime import timedelta
import logging
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from kubernetes.client import models as k8s

def build_time_task(dag, use_testnet=False, use_next_exec_time=True, resource_cfg="default"):
    '''
    Creates a task to run the get_ledger_range_from_times command from the stellar-etl Docker image. The start time is the previous
    execution time. Since checkpoints are only written to History Archives every 64 ledgers, we have to account for a 5-6 min delay.
    When use_next_exec_time is True, the end time is the current execution time. If it is False, then the end time is the same
    as the start time. The task that is returned allows for retreiving the ledger range for the given execution. The range object
    is sent as a string representation of a JSON object to the xcom, where it can be accessed by subsequent tasks.
    
    Parameters:
        dag - parent dag that the task will be attached to 
        use_next_exec_time - determines whether to use the next execution time or replace it with the current execution time
    Returns:
        the newly created task
    '''
    start_time = '{{ subtract_data_interval(dag, data_interval_start).isoformat() }}'
    end_time = '{{ subtract_data_interval(dag, data_interval_end).isoformat() }}' if use_next_exec_time else '{{ ts }}'
    command = ["stellar-etl"]
    args = [ "get_ledger_range_from_times", "-s", start_time, "-o", "/airflow/xcom/return.json", '-e', end_time]
    logging.info(f'Constructing command with args: {args}')
    if use_testnet:
        args.append("--testnet")
    config_file_location = Variable.get('kube_config_location')
    in_cluster = False if config_file_location else True
    resources_requests = Variable.get('resources', deserialize_json=True).get(resource_cfg).get('requests')
    affinity = Variable.get('affinity', deserialize_json=True).get(resource_cfg)

    return KubernetesPodOperator(
         task_id='get_ledger_range_from_times',
         name='get_ledger_range_from_times',
         execution_timeout=timedelta(seconds=Variable.get('task_timeout', deserialize_json=True)[build_time_task.__name__]),
         namespace=Variable.get('k8s_namespace'),
         service_account_name=Variable.get('k8s_service_account'),
         image=Variable.get('image_name'),
         cmds=command,
         arguments=args,
         dag=dag,
         do_xcom_push=True,
         is_delete_operator_pod=True,
         startup_timeout_seconds=720,
         in_cluster=in_cluster,
         config_file=config_file_location,
         affinity=affinity,
         resources=k8s.V1ResourceRequirements(requests=resources_requests),
         image_pull_policy=Variable.get('image_pull_policy')
     ) 
