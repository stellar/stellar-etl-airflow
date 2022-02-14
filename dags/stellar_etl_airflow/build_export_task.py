'''
This file contains functions for creating Airflow tasks to run stellar-etl export functions.
'''
import datetime
import logging
import os
from airflow import AirflowException
from airflow.models import Variable 


def get_path_variables(use_testnet=False):
    '''
        Returns the image output path, core executable path, and core config path.
    '''
    config = '/etl/docker/stellar-core.cfg'
    if use_testnet:
        config = '/etl/docker/stellar-core_testnet.cfg'
    return Variable.get('image_output_path'), '/usr/bin/stellar-core', config

def select_correct_filename(cmd_type, base_name, batched_name):
    switch = {
        'archive': batched_name,
        'bucket': batched_name,
        'bounded-core': base_name,
        'unbounded-core': base_name,
    }
    filename = switch.get(cmd_type, 'No file')
    if filename == 'No file':
        raise AirflowException("Command type is not supported: ", cmd_type)
    return filename

def generate_etl_cmd(command, base_filename, cmd_type, use_gcs=False, use_testnet=False):
    '''
    Runs the provided stellar-etl command with arguments that are appropriate for the command type.
    The supported command types are: 
        'archive' - indicates that information is being read within a bounded range from the history archives
        'bucket' - indicates that information is being read up to an end time from the history archives' bucket list.
        'bounded-core' - indicates that information is being read within a bounded range from stellar-core
        'unbounded-core' - indicates that information is being read from the start time onwards from stellar-core

    The supported commands are export_accounts, export_ledgers, export_offers, export_operations, export_trades, export_transactions, and export_trustlines.  
    
    Parameters:
        command - stellar-etl command (ex. export_ledgers, export_accounts)
        base_filename - base filename for the output file or folder; the ledger range is pre-pended to this filename
        cmd_type - the type of the command, which is determined by the information source
    Returns:
        the generated etl command; name of the file that contains the exported data
    '''
    # These are JINJA templates, which are filled by airflow at runtime. The json from get_ledger_range_from_times is pulled from XCOM. 
    logging.info("Pulling ledger start and end times.....")
    start_ledger = '{{ ti.xcom_pull(task_ids="get_ledger_range_from_times")["start"] }}'
    end_ledger = '{{ ti.xcom_pull(task_ids="get_ledger_range_from_times")["end"]}}'

    '''
    For history archives, the start time of the next 5 minute interval is the same as the end time of the current interval, leading to a 1 ledger overlap.
    We need to subtract 1 ledger from the end so that there is no overlap. However, sometimes the start ledger equals the end ledger. 
    By setting the end=max(start, end-1), we ensure every range is valid.
    '''
    if cmd_type == 'archive':
        end_ledger = '{{ [ti.xcom_pull(task_ids="get_ledger_range_from_times")["end"]-1, ti.xcom_pull(task_ids="get_ledger_range_from_times")["start"]] | max}}'

    image_output_path, core_exec, core_cfg = get_path_variables(use_testnet)

    batch_filename = '-'.join([start_ledger, end_ledger, base_filename])
    run_id = '{{ run_id }}'
    filepath = ""
    if use_gcs:
        filepath = os.path.join(Variable.get('gcs_exported_object_prefix'), run_id)
    batched_path = os.path.join(filepath, batch_filename)
    base_path = os.path.join(filepath, base_filename)

    correct_filename = select_correct_filename(cmd_type, base_filename, batch_filename)
    switch = {
        'archive': ['stellar-etl', command, '-s', start_ledger, '-e', end_ledger, '-o', batched_path],
        'bucket': ['stellar-etl', command, '-e', end_ledger, '-o', batched_path],
        'bounded-core': ['stellar-etl', command, '-s', start_ledger, '-e', end_ledger, '-x', core_exec, '-c', core_cfg, '-o', base_path],
        'unbounded-core': ['stellar-etl', command, '-s', start_ledger, '-x', core_exec, '-c', core_cfg, '-o', base_path],
    }

    cmd = switch.get(cmd_type, None)
    if cmd is None:
        raise AirflowException("Command type is not supported: ", cmd_type)
    if use_gcs:
        cmd.extend(['--gcs-bucket', Variable.get('gcs_exported_data_bucket_name')])
        batch_date = '{{ prev_execution_date.to_datetime_string() }}'
        batch_insert_ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        metadata = f"'batch_id={run_id},batch_run_date={batch_date},batch_insert_ts={batch_insert_ts}'"
        cmd.extend(['-u', metadata])
    if use_testnet:
        cmd.append('--testnet')
    cmd.append('--strict-export')
    return cmd, os.path.join(filepath, correct_filename)
    

def build_docker_exporter(dag, command, etl_cmd_string, output_file):
    '''
    Creates the export task using a DockerOperator.
    Parameters:
        dag - the parent dag
        command - stellar-etl command type (ex. export_ledgers, export_accounts)
        etl_cmd_string - a string of the fully formed command that includes all flags and arguments to be sent to the etl
        output_file - filename for the output file or folder
    Returns:
        the DockerOperator for the export task
    '''
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    full_cmd = f'bash -c "{etl_cmd_string} >> /dev/null && echo \"{output_file}\""'
    config_file_location = Variable.get('kube_config_location')
    in_cluster = False if config_file_location else True
    force_pull = True if Variable.get('image_pull_policy')=='Always' else False
    return KubernetesPodOperator(
        task_id=command + '_task',
        name=command + '_task',
        namespace=Variable.get('namespace'),
        image=Variable.get('image_name'),
        command=full_cmd, 
        # volumes=[f'{Variable.get("local_output_path")}:{Variable.get("image_output_path")}'],
        dag=dag,
        do_xcom_push=True,
        is_delete_operator_pod=True,
        in_cluster=in_cluster,
        config_file=config_file_location,
        affinity=Variable.get('affinity', deserialize_json=True),
        auto_remove=True,
        tty=True,
        force_pull=force_pull,
    ) 

def build_export_task(dag, cmd_type, command, filename, use_gcs=False, use_testnet=False):
    '''
    Creates a task that calls the provided export function with the correct arguments in the stellar-etl Docker image.
    Can either return a DockerOperator or a KubernetesPodOperator, depending on the value of the 
    use_kubernetes_pod_exporter variable.
    
    Parameters:
        dag - the parent dag
        cmd_type - the type of the command, which is determined by the information source
        command - stellar-etl command (ex. export_ledgers, export_accounts)
        filename - filename for the output file or folder
    Returns:
        the newly created task
    '''
    etl_cmd, output_file = generate_etl_cmd(command, filename, cmd_type, use_gcs, use_testnet)
    etl_cmd_string = ' '.join(etl_cmd)
    return build_docker_exporter(dag, command, etl_cmd_string, output_file)
         